use std::str;
use std::error::Error;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::thread::sleep;
use std::sync::atomic::{AtomicU16, Ordering};
use std::cell::RefCell;
use futures::channel::mpsc::Sender;
use futures::sink::SinkExt;
use futures::stream::{Stream, TryStreamExt};
use hyper::{Request, Body, Chunk, StatusCode};
use hyper::client::{HttpConnector, Client};
use hyper::header::{AUTHORIZATION, CONTENT_TYPE, CONTENT_LENGTH, CONNECTION};
use hyper_tls::HttpsConnector;
use serde_json::{self, Value};
use data_encoding::BASE64_NOPAD;
use indicatif::ProgressBar;
use crate::crypto::{self, AppKeys, encode_meta, decode_meta};
use crate::data::file::{RemoteFile, RemoteFileVersion};
use crate::config::Config;
use crate::progress::ProgressDataReader;
use crate::box_result::BoxResult;


#[derive(Clone, PartialEq)]
pub struct B2Upload {
    pub upload_url: String,
    pub auth_token: String,
}

pub struct B2 {
    pub key: crypto::Key,
    pub bucket_id: String,
    pub acc_id: String,
    pub auth_token: String,
    pub api_url: String,
    pub bucket_download_url: String,
    pub client: Client<HttpsConnector<HttpConnector>>,
    pub tx_progress: Option<ProgressBar>,
}

impl Clone for B2 {
    fn clone(&self) -> Self {
        B2 {
            key: self.key.clone(),
            bucket_id: self.bucket_id.clone(),
            acc_id: self.acc_id.clone(),
            auth_token: self.auth_token.clone(),
            api_url: self.api_url.clone(),
            bucket_download_url: self.bucket_download_url.clone(),
            client: make_client(),
            tx_progress: None,
        }
    }
}

async fn warning(maybe_progress: &Option<ProgressBar>, msg: &str) {
    match maybe_progress {
        Some(progress) => {
            progress.println(format!("Warning: {}", msg));
        },
        None => println!("Warning: {}", msg),
    }
}

fn make_basic_auth(AppKeys{b2_key_id: username, b2_key: password, ..}: &AppKeys) -> String {
    let val = username.to_owned() + ":" + password;
    let encoded = BASE64_NOPAD.encode(val.as_bytes());
    "Basic ".to_owned() + &encoded
}

fn make_client() -> Client<HttpsConnector<HttpConnector>> {
    let https = HttpsConnector::new().unwrap();
    Client::builder()
        .keep_alive(false) // Caused hangs when used with spawn_with_handle...
        .build::<_, hyper::Body>(https)
}

impl B2 {
    async fn request_with_backoff<F>(&self, req_builder: F) -> Result<(StatusCode, Chunk), Box<dyn Error + Send + Sync + 'static>>
        where F: Fn() -> Request<Body>
    {
        let mut attempts = 0;
        loop {
            attempts += 1;
            if attempts > 1 {
                let cooldown = (1 << attempts.min(5)) * 100; // Up to 3.2 seconds
                sleep(Duration::from_millis(cooldown));
            }

            let req = req_builder();
            let res = match self.client.request(req).await {
                Ok(res) => res,
                Err(e) => {
                    let err_str = format!("Unexpected request failure: {}", e);
                    warning(&self.tx_progress, &err_str).await;
                    continue;
                },
            };
            let status = res.status();
            let body = res.into_body().try_concat().await?;

            // Temporary failure is not an error, just asking for an exponential backoff
            if status.as_u16() == 503 || status.as_u16() == 408 {
                warning(&self.tx_progress, status.canonical_reason().unwrap_or("Temporary request failure")).await;
                continue;
            }

            return Ok((status, body))
        }
    }

    pub async fn authenticate(config: &Config, keys: &AppKeys) -> BoxResult<B2> {
        let client = make_client();
        let basic_auth = make_basic_auth(keys);
        let bucket_name = config.bucket_name.to_owned();

        let req = Request::get("https://api.backblazeb2.com/b2api/v2/b2_authorize_account")
            .header(AUTHORIZATION, basic_auth)
            .header(CONNECTION, "keep-alive")
            .body(Body::empty())
            .unwrap();

        let res = client.request(req).await?;
        let status = res.status();
        let body = res.into_body().try_concat().await?;

        let reply_json: Value = match serde_json::from_slice(&body) {
            Err(_) => return Err(From::from(format!("authenticate failed to parse json: {}",
                                                    str::from_utf8(&body).unwrap()))),
            Ok(json) => json,
        };

        if !status.is_success() {
            let mut err_msg = String::from("Backblaze B2 login failure: ")+str::from_utf8(&body).unwrap();
            if let Value::String(ref reply_err_msg) = reply_json["message"] {
                err_msg += &(String::from(": ") + &reply_err_msg);
            }
            return Err(From::from(err_msg));
        }

        let bucket_download_url = reply_json["downloadUrl"].as_str().unwrap().to_string() + "/file/" + &config.bucket_name + "/";

        let mut b2 = B2 {
            key: keys.encryption_key.clone(),
            acc_id: reply_json["accountId"].as_str().unwrap().to_string(),
            auth_token: reply_json["authorizationToken"].as_str().unwrap().to_string(),
            bucket_id: String::new(),
            api_url: reply_json["apiUrl"].as_str().unwrap().to_string(),
            bucket_download_url,
            tx_progress: None,
            client,
        };

        let bucket_id = b2.get_bucket_id(&bucket_name).await?;
        b2.bucket_id = bucket_id;

        Ok(b2)
    }

    async fn get_bucket_id(&self, bucket_name: &str) -> BoxResult<String> {
        let bucket_name = bucket_name.to_owned(); // Can't wait for the Pin API!

        let (status, body) = self.request_with_backoff(||
            Request::builder()
                .uri(self.api_url.clone() + "/b2api/v2/b2_list_buckets")
                .method("POST")
                .header(AUTHORIZATION, self.auth_token.clone())
                .header(CONNECTION, "keep-alive")
                .body(Body::from(format!("{{\
                    \"bucketName\":\"{}\",\
                    \"accountId\":\"{}\"\
                    }}", bucket_name, self.acc_id)))
                .unwrap()
        ).await?;

        let reply_json: Value = serde_json::from_slice(&body)?;

        if !status.is_success() {
            return Err(From::from(format!("get_bucket_id failed with error {}: {}",
                                          status.as_u16(),
                                          reply_json["message"])));
        }

        let buckets = reply_json["buckets"].as_array().unwrap();
        for bucket in buckets {
            if bucket["bucketName"] == bucket_name {
                return Ok(bucket["bucketId"].as_str().unwrap().to_string())
            }
        }
        Err(From::from(format!("Bucket '{}' not found", bucket_name)))
    }

    pub async fn list_remote_files(&self, prefix: &str) -> BoxResult<Vec<RemoteFile>> {
        let url = self.api_url.clone()+"/b2api/v2/b2_list_file_names";

        let body_base = format!("\"bucketId\":\"{}\",\
                                \"maxFileCount\":10000,\
                                \"prefix\":\"{}\"", self.bucket_id, prefix);
        let mut start_filename: Option<String> = None;
        let mut files: Vec<RemoteFile> = Vec::new();

        loop {
            let (status, body) = self.request_with_backoff(|| {
                let body = if start_filename.is_some() {
                    format!("{{\"startFileName\":\"{}\",\
                                {}}}", start_filename.as_ref().unwrap(), body_base)
                } else {
                    format!("{{{}}}", body_base)
                };

                Request::post(&url)
                    .header(AUTHORIZATION, self.auth_token.clone())
                    .header(CONNECTION, "keep-alive")
                    .body(Body::from(body))
                    .unwrap()
            }).await?;

            let reply_json: Value = serde_json::from_slice(&body)?;
            if !status.is_success() {
                return Err(From::from(format!("list_remote_files failed with error {}: {}",
                                              status.as_u16(),
                                              reply_json["message"])));
            }

            for file in reply_json["files"].as_array().unwrap() {
                let full_name = file["fileName"].as_str().unwrap();
                let id = file["fileId"].as_str().unwrap();
                let enc_meta = file["fileInfo"]["enc_meta"].as_str().unwrap();
                let (filename, mtime, mode, is_symlink) = decode_meta(&self.key, enc_meta)?;
                files.push(RemoteFile::new(&filename, full_name, id, mtime, mode, is_symlink)?)
            }

            if let Some(next) = reply_json["nextFileName"].as_str() {
                start_filename = Some(next.to_string());
            } else {
                break;
            }
        }

        Ok(files)
    }

    pub async fn list_remote_file_versions(&self, prefix: &str)
                    -> BoxResult<Vec<RemoteFileVersion>> {
        let url = self.api_url.clone() + "/b2api/v2/b2_list_file_versions";

        let body_base = format!("\"bucketId\":\"{}\",\
                                \"maxFileCount\":10000,\
                                \"prefix\":\"{}\"", self.bucket_id, prefix);
        let mut start_file_version: Option<RemoteFileVersion> = None;
        let mut files: Vec<RemoteFileVersion> = Vec::new();

        loop {
            let (status, body) = self.request_with_backoff(|| {
                let body = if start_file_version.is_some() {
                    let ver = start_file_version.as_ref().unwrap();
                    format!("{{\"startFileName\":\"{}\",\
                                  \"startFileId\":\"{}\",\
                                 {}}}", ver.path, ver.id, body_base)
                } else {
                    format!("{{{}}}", body_base)
                };

                Request::post(&url)
                    .header(AUTHORIZATION, self.auth_token.clone())
                    .header(CONNECTION, "keep-alive")
                    .body(Body::from(body))
                    .unwrap()
            }).await?;

            let reply_json: Value = serde_json::from_slice(&body)?;
            if !status.is_success() {
                return Err(From::from(format!("list_remote_files_versions failed with error {}: {}",
                                              status.as_u16(),
                                              reply_json["message"])));
            }

            for file in reply_json["files"].as_array().unwrap() {
                // Ignore hidden files entirely
                if file["action"] != "upload" {
                    continue;
                }
                let file_id = file["fileId"].as_str().unwrap().to_string();
                let file_name = file["fileName"].as_str().unwrap().to_string();
                files.push(RemoteFileVersion{path: file_name, id: file_id});
            }

            let maybe_next_name = reply_json["nextFileName"].as_str();
            let maybe_next_id = reply_json["nextFileId"].as_str();
            if let (Some(name), Some(id)) = (maybe_next_name, maybe_next_id) {
                start_file_version = Some(RemoteFileVersion{
                    path: name.to_string(),
                    id: id.to_string()
                });
            } else {
                break;
            }
        }

        Ok(files)
    }

    pub async fn get_upload_url(&self) -> BoxResult<B2Upload> {
        let (status, body) = self.request_with_backoff(||
            Request::post(self.api_url.clone() + "/b2api/v2/b2_get_upload_url")
                .header(AUTHORIZATION, self.auth_token.clone())
                .header(CONNECTION, "keep-alive")
                .body(Body::from(format!("{{\"bucketId\":\"{}\"}}", self.bucket_id)))
                .unwrap()
        ).await?;

        let reply_json: Value = serde_json::from_slice(&body)?;
        if !status.is_success() {
            return Err(From::from(format!("get_upload_url failed with error {}: {}",
                                          status.as_u16(),
                                          reply_json["message"])));
        }


        Ok(B2Upload {
            upload_url: reply_json["uploadUrl"].as_str().unwrap().to_string(),
            auth_token: reply_json["authorizationToken"].as_str().unwrap().to_string(),
        })
    }

    pub async fn delete_file_version(&self, file_version: &RemoteFileVersion) -> BoxResult<()> {
        let (status, body) = self.request_with_backoff(||
            Request::post(self.api_url.clone()+"/b2api/v2/b2_delete_file_version")
                .header(AUTHORIZATION, self.auth_token.clone())
                .header(CONNECTION, "keep-alive")
                .body(Body::from(format!("{{\"fileId\": \"{}\", \
                              \"fileName\": \"{}\"}}", file_version.id, file_version.path)))
                .unwrap()
        ).await?;

        if !status.is_success() {
            let reply_json: Value = serde_json::from_slice(&body)?;
            return Err(From::from(format!("Removal of {} failed with error {}: {}",
                                          file_version.path, status.as_u16(),
                                          reply_json["message"])));
        }
        Ok(())
    }

    pub async fn upload_file_simple(&self, filename: &str, data: Vec<u8>) -> BoxResult<RemoteFileVersion> {
        let data_reader = ProgressDataReader::new_silent(data);
        let upload_url = self.get_upload_url().await?;
        self.upload_file(&upload_url, filename, data_reader, None).await
    }

    pub async fn upload_file(&self, B2Upload{upload_url, auth_token}: &B2Upload,
                             filename: &str, data: ProgressDataReader,
                             enc_meta: Option<String>) -> BoxResult<RemoteFileVersion> {
        let enc_meta = if enc_meta.is_some() {
            enc_meta.as_ref().unwrap().to_owned()
        } else {
            let last_modified = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
            let mode = 0o644;
            encode_meta(&self.key, Path::new(filename), last_modified, mode, false)
        };

        let (status, body) = self.request_with_backoff(|| {
            let data_stream = Box::new(data.clone()) as Box<dyn Stream<Item=Result<Chunk, Box<(dyn std::error::Error + Sync + Send + 'static)>>> + Send + Sync + 'static>;
            let sha1 = crypto::sha1_string(data.as_slice());
            Request::post(upload_url)
                .header(AUTHORIZATION, &auth_token as &str)
                .header(CONNECTION, "keep-alive")
                .header(CONTENT_TYPE, "application/octet-stream")
                .header(CONTENT_LENGTH, data.len())
                .header("X-Bz-File-Name", filename.to_string())
                .header("X-Bz-Content-Sha1", sha1)
                .header("X-Bz-Info-enc_meta", enc_meta.to_owned())
                .body(Body::from(data_stream))
                .unwrap()
        }).await?;

        let reply_json: Value = match serde_json::from_slice(&body) {
            Err(_) => return Err(From::from(format!("upload_file failed to parse json: {}",
                                                    str::from_utf8(&body).unwrap()))),
            Ok(json) => json,
        };

        if !status.is_success() {
            return Err(From::from(format!("upload_file failed with error {}: {}, {}",
                                          status.as_u16(),
                                          reply_json["code"],
                                          reply_json["message"])));
        }

        Ok(RemoteFileVersion {
            path: reply_json["fileName"].as_str().unwrap().to_string(),
            id: reply_json["fileId"].as_str().unwrap().to_string(),
        })
    }

    pub async fn download_file(&self, filename: &str) -> BoxResult<Vec<u8>> {
        let filename = filename.to_owned();
        let (status, body) = self.request_with_backoff(||
            Request::get(self.bucket_download_url.clone() + &filename)
                .header(AUTHORIZATION, self.auth_token.clone())
                .header(CONNECTION, "keep-alive")
                .body(Body::empty())
                .unwrap()
        ).await?;

        if !status.is_success() {
            return Err(From::from(format!("Download of {} failed with error {}",
                                          filename, status.as_u16())));
        }

        Ok(body.to_vec())
    }

    pub async fn hide_file(&self, file_path_hash: &str) -> BoxResult<()> {
        let (status, body) = self.request_with_backoff(||
            Request::post(self.api_url.clone()+"/b2api/v2/b2_hide_file")
                .header(AUTHORIZATION, self.auth_token.clone())
                .header(CONNECTION, "keep-alive")
                .body(Body::from(format!("{{\"bucketId\": \"{}\", \
                              \"fileName\": \"{}\"}}", self.bucket_id, file_path_hash)))
                .unwrap()
        ).await?;

        if !status.is_success() {
            let reply_json: Value = serde_json::from_slice(&body)?;
            return Err(From::from(format!("Hiding of {} failed with error {}: {}",
                                          file_path_hash, status.as_u16(),
                                          reply_json["message"])));
        }
        Ok(())
    }
}
