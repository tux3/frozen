use crate::box_result::BoxResult;
use crate::config::Config;
use crate::crypto::{self, decode_meta, encode_meta, sha1_string, AppKeys};
use crate::data::file::{RemoteFile, RemoteFileVersion};
use crate::progress::ProgressHandler;
use crate::stream::{HashedStream, SimpleBytesStream};
use bytes::Bytes;
use data_encoding::BASE64_NOPAD;
use futures::{Stream, StreamExt};
use hyper::client::{Client, HttpConnector};
use hyper::header::{AUTHORIZATION, CONNECTION, CONTENT_LENGTH, CONTENT_TYPE};
use hyper::{Body, Request, StatusCode};
use hyper_tls::HttpsConnector;
use serde_json::{self, Value};
use std::path::Path;
use std::str::from_utf8;
use std::thread::sleep;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Copy, Clone)]
pub enum FileListDepth {
    Shallow, // List only files in the current "folder"
    Deep,    // List every file recursively
}

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
    pub progress: Option<ProgressHandler>,
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
            progress: self.progress.clone(),
        }
    }
}

async fn warning(maybe_progress: &Option<ProgressHandler>, msg: &str) {
    match maybe_progress {
        Some(progress) => {
            progress.println(format!("Warning: {}", msg));
        }
        None => println!("Warning: {}", msg),
    }
}

fn make_basic_auth(
    AppKeys {
        b2_key_id: username,
        b2_key: password,
        ..
    }: &AppKeys,
) -> String {
    let val = username.to_owned() + ":" + password;
    let encoded = BASE64_NOPAD.encode(val.as_bytes());
    "Basic ".to_owned() + &encoded
}

fn make_client() -> Client<HttpsConnector<HttpConnector>> {
    let https = HttpsConnector::new();
    Client::builder()
        .pool_max_idle_per_host(0) // Caused hangs when used with spawn_with_handle...
        .build::<_, hyper::Body>(https)
}

impl B2 {
    async fn request_with_backoff<F>(&self, req_builder: F) -> BoxResult<(StatusCode, Bytes)>
    where
        F: FnMut() -> Request<Body>,
    {
        let (status, body) = self.request_stream_with_backoff(req_builder).await?;
        let body_bytes = hyper::body::to_bytes(body).await?;
        Ok((status, body_bytes))
    }

    async fn request_stream_with_backoff<F>(&self, mut req_builder: F) -> BoxResult<(StatusCode, Body)>
    where
        F: FnMut() -> Request<Body>,
    {
        let mut hard_fails = 0u32;
        let mut attempts = 0u32;
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
                    warning(&self.progress, &err_str).await;
                    continue;
                }
            };
            let status = res.status();

            // Temporary failure is not an error, just asking for an exponential backoff
            if status.as_u16() == 503 || status.as_u16() == 408 {
                warning(
                    &self.progress,
                    status.canonical_reason().unwrap_or("Temporary request failure"),
                )
                .await;
                continue;
            }

            // Treat internal server errors as temporary failures, for a few attempts
            if status.as_u16() == 500 && hard_fails < 5 {
                hard_fails += 1;
                warning(
                    &self.progress,
                    status.canonical_reason().unwrap_or("Internal server error"),
                )
                .await;
                continue;
            }

            return Ok((status, res.into_body()));
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
        let body = hyper::body::to_bytes(res.into_body()).await?;

        let reply_json: Value = match serde_json::from_slice(&body) {
            Err(_) => {
                return Err(From::from(format!(
                    "authenticate failed to parse json: {}",
                    std::str::from_utf8(&body).unwrap()
                )))
            }
            Ok(json) => json,
        };

        if !status.is_success() {
            let mut err_msg = "Backblaze B2 login failure: ".to_string() + from_utf8(&body).unwrap();
            if let Value::String(ref reply_err_msg) = reply_json["message"] {
                err_msg += &(String::from(": ") + &reply_err_msg);
            }
            return Err(From::from(err_msg));
        }

        let bucket_download_url = format!(
            "{}/file/{}/",
            reply_json["downloadUrl"].as_str().unwrap(),
            &config.bucket_name
        );

        let mut b2 = B2 {
            key: keys.encryption_key.clone(),
            acc_id: reply_json["accountId"].as_str().unwrap().to_string(),
            auth_token: reply_json["authorizationToken"].as_str().unwrap().to_string(),
            bucket_id: String::new(),
            api_url: reply_json["apiUrl"].as_str().unwrap().to_string(),
            bucket_download_url,
            progress: None,
            client,
        };

        let bucket_id = b2.get_bucket_id(&bucket_name).await?;
        b2.bucket_id = bucket_id;

        Ok(b2)
    }

    async fn get_bucket_id(&self, bucket_name: &str) -> BoxResult<String> {
        let bucket_name = bucket_name.to_owned(); // Can't wait for the Pin API!

        let (status, body) = self
            .request_with_backoff(|| {
                Request::builder()
                    .uri(self.api_url.clone() + "/b2api/v2/b2_list_buckets")
                    .method("POST")
                    .header(AUTHORIZATION, self.auth_token.clone())
                    .header(CONNECTION, "keep-alive")
                    .body(Body::from(format!(
                        r#"{{
                         "bucketName":"{}",
                         "accountId":"{}"
                         }}"#,
                        bucket_name, self.acc_id
                    )))
                    .unwrap()
            })
            .await?;

        let reply_json = Self::get_json_reply("get_bucket_id", status, body).await?;

        let buckets = reply_json["buckets"].as_array().unwrap();
        for bucket in buckets {
            if bucket["bucketName"] == bucket_name {
                return Ok(bucket["bucketId"].as_str().unwrap().to_string());
            }
        }
        Err(From::from(format!("Bucket '{}' not found", bucket_name)))
    }

    pub async fn list_remote_files(&self, prefix: &str, depth: FileListDepth) -> BoxResult<Vec<RemoteFile>> {
        let url = self.api_url.clone() + "/b2api/v2/b2_list_file_names";

        let delimiter = match depth {
            FileListDepth::Shallow => r#""/""#,
            FileListDepth::Deep => "null",
        };
        let body_base = format!(
            "\"bucketId\":\"{}\",\
             \"maxFileCount\":10000,\
             \"delimiter\":{},\
             \"prefix\":\"{}\"",
            self.bucket_id, delimiter, prefix
        );
        let mut start_filename: Option<String> = None;
        let mut files: Vec<RemoteFile> = Vec::new();

        loop {
            let (status, body) = self
                .request_with_backoff(|| {
                    let body = if start_filename.is_some() {
                        format!(
                            "{{\"startFileName\":\"{}\",\
                             {}}}",
                            start_filename.as_ref().unwrap(),
                            body_base
                        )
                    } else {
                        format!("{{{}}}", body_base)
                    };

                    Request::post(&url)
                        .header(AUTHORIZATION, self.auth_token.clone())
                        .header(CONNECTION, "keep-alive")
                        .body(Body::from(body))
                        .unwrap()
                })
                .await?;

            let reply_json = Self::get_json_reply("list_remote_files", status, body).await?;

            for file in reply_json["files"].as_array().unwrap() {
                // Ignore non-files (folders, large file starts) entirely
                if file["action"] != "upload" {
                    continue;
                }
                let full_name = file["fileName"].as_str().unwrap();
                let id = file["fileId"].as_str().unwrap();
                let enc_meta = file["fileInfo"]["enc_meta"].as_str().unwrap();
                let (filename, mtime, mode, is_symlink) = decode_meta(&self.key, enc_meta)?;
                files.push(RemoteFile::new(&filename, full_name, id, mtime, mode, is_symlink))
            }

            if let Some(next) = reply_json["nextFileName"].as_str() {
                start_filename = Some(next.to_string());
            } else {
                break;
            }
        }

        Ok(files)
    }

    pub async fn list_remote_file_versions(&self, prefix: &str) -> BoxResult<Vec<RemoteFileVersion>> {
        let url = self.api_url.clone() + "/b2api/v2/b2_list_file_versions";

        let body_base = format!(
            "\"bucketId\":\"{}\",\
             \"maxFileCount\":10000,\
             \"prefix\":\"{}\"",
            self.bucket_id, prefix
        );
        let mut start_file_version: Option<RemoteFileVersion> = None;
        let mut files: Vec<RemoteFileVersion> = Vec::new();

        loop {
            let (status, body) = self
                .request_with_backoff(|| {
                    let body = if start_file_version.is_some() {
                        let ver = start_file_version.as_ref().unwrap();
                        format!(
                            "{{\"startFileName\":\"{}\",\
                             \"startFileId\":\"{}\",\
                             {}}}",
                            ver.path, ver.id, body_base
                        )
                    } else {
                        format!("{{{}}}", body_base)
                    };

                    Request::post(&url)
                        .header(AUTHORIZATION, self.auth_token.clone())
                        .header(CONNECTION, "keep-alive")
                        .body(Body::from(body))
                        .unwrap()
                })
                .await?;

            let reply_json = Self::get_json_reply("list_remote_files_versions", status, body).await?;

            for file in reply_json["files"].as_array().unwrap() {
                // Ignore non-files (folders, hidden files, large file starts) entirely
                if file["action"] != "upload" {
                    continue;
                }
                let file_id = file["fileId"].as_str().unwrap().to_string();
                let file_name = file["fileName"].as_str().unwrap().to_string();
                files.push(RemoteFileVersion {
                    path: file_name,
                    id: file_id,
                });
            }

            let maybe_next_name = reply_json["nextFileName"].as_str();
            let maybe_next_id = reply_json["nextFileId"].as_str();
            if let (Some(name), Some(id)) = (maybe_next_name, maybe_next_id) {
                start_file_version = Some(RemoteFileVersion {
                    path: name.to_string(),
                    id: id.to_string(),
                });
            } else {
                break;
            }
        }

        Ok(files)
    }

    pub async fn list_unfinished_large_files(&self, prefix: &str) -> BoxResult<Vec<RemoteFile>> {
        let url = self.api_url.clone() + "/b2api/v2/b2_list_unfinished_large_files";

        let body_base = format!(
            r#""bucketId":"{}",
             "namePrefix":"{}""#,
            self.bucket_id, prefix
        );
        let mut start_file_version: Option<String> = None;
        let mut unfinished_files: Vec<RemoteFile> = Vec::new();

        loop {
            let (status, body) = self
                .request_with_backoff(|| {
                    let body = if let Some(ref ver) = start_file_version {
                        format!(
                            r#"{{
                                "startFileId":"{}",
                                {}
                            }}"#,
                            ver, body_base
                        )
                    } else {
                        format!("{{{}}}", body_base)
                    };

                    Request::post(&url)
                        .header(AUTHORIZATION, self.auth_token.clone())
                        .header(CONNECTION, "keep-alive")
                        .body(Body::from(body))
                        .unwrap()
                })
                .await?;

            let reply_json = Self::get_json_reply("list_unfinished_large_files", status, body).await?;

            for file in reply_json["files"].as_array().unwrap() {
                // Ignore non-large files (regular uploads, folders, hidden files) entirely
                if file["action"] != "start" {
                    continue;
                }
                let full_name = file["fileName"].as_str().unwrap();
                let id = file["fileId"].as_str().unwrap();
                let enc_meta = file["fileInfo"]["enc_meta"].as_str().unwrap();
                let (filename, mtime, mode, is_symlink) = decode_meta(&self.key, enc_meta)?;
                unfinished_files.push(RemoteFile::new(&filename, full_name, id, mtime, mode, is_symlink))
            }

            let maybe_next_id = reply_json["nextFileId"].as_str();
            if let Some(id) = maybe_next_id {
                start_file_version = Some(id.to_string());
            } else {
                break;
            }
        }

        Ok(unfinished_files)
    }

    pub async fn get_upload_url(&self) -> BoxResult<B2Upload> {
        let (status, body) = self
            .request_with_backoff(|| {
                Request::post(self.api_url.clone() + "/b2api/v2/b2_get_upload_url")
                    .header(AUTHORIZATION, self.auth_token.clone())
                    .header(CONNECTION, "keep-alive")
                    .body(Body::from(format!("{{\"bucketId\":\"{}\"}}", self.bucket_id)))
                    .unwrap()
            })
            .await?;

        let reply_json = Self::get_json_reply("get_upload_url", status, body).await?;
        Ok(B2Upload {
            upload_url: reply_json["uploadUrl"].as_str().unwrap().to_string(),
            auth_token: reply_json["authorizationToken"].as_str().unwrap().to_string(),
        })
    }

    /// The returned B2Upload struct is only valid for the one large file being uploaded
    pub async fn get_upload_part_url(&self, file_id: &str) -> BoxResult<B2Upload> {
        let (status, body) = self
            .request_with_backoff(|| {
                Request::post(self.api_url.clone() + "/b2api/v2/b2_get_upload_part_url")
                    .header(AUTHORIZATION, self.auth_token.clone())
                    .header(CONNECTION, "keep-alive")
                    .body(Body::from(format!(r#"{{"fileId":"{}"}}"#, file_id.to_owned())))
                    .unwrap()
            })
            .await?;

        let reply_json: Value = serde_json::from_slice(&body)?;
        if !status.is_success() {
            return Err(From::from(format!(
                "get_upload_part_url failed with error {}: {}",
                status.as_u16(),
                reply_json["message"]
            )));
        }

        Ok(B2Upload {
            upload_url: reply_json["uploadUrl"].as_str().unwrap().to_string(),
            auth_token: reply_json["authorizationToken"].as_str().unwrap().to_string(),
        })
    }

    pub async fn delete_file_version(&self, file_version: &RemoteFileVersion) -> BoxResult<()> {
        let (status, body) = self
            .request_with_backoff(|| {
                Request::post(self.api_url.clone() + "/b2api/v2/b2_delete_file_version")
                    .header(AUTHORIZATION, self.auth_token.clone())
                    .header(CONNECTION, "keep-alive")
                    .body(Body::from(format!(
                        "{{\"fileId\": \"{}\", \
                         \"fileName\": \"{}\"}}",
                        file_version.id, file_version.path
                    )))
                    .unwrap()
            })
            .await?;

        if !status.is_success() {
            let reply_json: Value = serde_json::from_slice(&body)?;
            return Err(From::from(format!(
                "Removal of {} failed with error {}: {}",
                file_version.path,
                status.as_u16(),
                reply_json["message"]
            )));
        }
        Ok(())
    }

    pub async fn upload_file_simple(&self, filename: &str, data: Vec<u8>) -> BoxResult<RemoteFileVersion> {
        let upload_url = self.get_upload_url().await?;
        self.upload_file(&upload_url, filename, data, None).await
    }

    pub async fn upload_file(
        &self,
        b2upload: &B2Upload,
        filename: &str,
        data: Vec<u8>,
        enc_meta: Option<String>,
    ) -> BoxResult<RemoteFileVersion> {
        let data_stream = Box::new(SimpleBytesStream::new(data.into()));
        self.upload_file_stream(b2upload, filename, data_stream, enc_meta).await
    }

    // NOTE: The 'a lifetimes and the manual async/impl Future work around rust-lang/rust#63033
    pub fn upload_file_stream<'a>(
        &'a self,
        b2upload: &'a B2Upload,
        filename: &'a str,
        data_stream: impl Stream<Item = BoxResult<Bytes>> + Unpin + Send + Sync + 'static,
        enc_meta: Option<String>,
    ) -> impl std::future::Future<Output = BoxResult<RemoteFileVersion>> + 'a {
        async move {
            let enc_meta = if enc_meta.is_some() {
                enc_meta.as_ref().unwrap().to_owned()
            } else {
                let last_modified = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                let mode = 0o644;
                encode_meta(&self.key, Path::new(filename), last_modified, mode, false)
            };

            let lower_bound_size = data_stream.size_hint().0;
            if lower_bound_size >= 2 {
                self.upload_large_file_stream(filename, data_stream, &enc_meta).await
            } else {
                self.upload_small_file_stream(b2upload, filename, data_stream, &enc_meta)
                    .await
            }
        }
    }

    /// Uploads a stream in one shot using b2_upload_file
    async fn upload_small_file_stream(
        &self,
        b2upload: &B2Upload,
        filename: &str,
        mut data_stream: impl Stream<Item = BoxResult<Bytes>> + Unpin + Send + Sync + 'static,
        enc_meta: &str,
    ) -> BoxResult<RemoteFileVersion> {
        let data = data_stream.next().await;
        let data = data.expect("Data stream to upload must not be empty")?;
        // Small files here means files that have only one chunk
        assert!(data_stream.next().await.is_none());

        let sha1 = sha1_string(&data);

        let (status, body) = self
            .request_with_backoff(|| {
                Request::post(&b2upload.upload_url)
                    .header(AUTHORIZATION, &b2upload.auth_token as &str)
                    .header(CONNECTION, "keep-alive")
                    .header(CONTENT_TYPE, "application/octet-stream")
                    .header(CONTENT_LENGTH, data.len())
                    .header("X-Bz-File-Name", filename.to_string())
                    .header("X-Bz-Content-Sha1", sha1.clone())
                    .header("X-Bz-Info-enc_meta", enc_meta.to_owned())
                    .body(Body::from(data.clone()))
                    .unwrap()
            })
            .await?;

        let reply_json = Self::get_json_reply("upload_file", status, body).await?;
        Ok(RemoteFileVersion {
            path: reply_json["fileName"].as_str().unwrap().to_string(),
            id: reply_json["fileId"].as_str().unwrap().to_string(),
        })
    }

    /// Uploads a stream as a large file
    async fn upload_large_file_stream(
        &self,
        filename: &str,
        data_stream: impl Stream<Item = BoxResult<Bytes>> + Unpin + Send + Sync + 'static,
        enc_meta: &str,
    ) -> BoxResult<RemoteFileVersion> {
        let file_id = self.start_large_file(filename, enc_meta).await?;
        let result = self.upload_large_file_stream_parts(&file_id, data_stream).await;

        if result.is_err() {
            let _ = self.cancel_large_file(&file_id).await;
        }
        result
    }

    async fn upload_large_file_stream_parts(
        &self,
        file_id: &str,
        data_stream: impl Stream<Item = BoxResult<Bytes>> + Unpin + Send + Sync + 'static,
    ) -> BoxResult<RemoteFileVersion> {
        let b2upload = self.get_upload_part_url(&file_id).await?;
        let mut part_hashes = Vec::<String>::new();

        let hashed_stream = HashedStream::new(Box::new(data_stream));
        let mut hashed_stream = hashed_stream.enumerate();

        while let Some((idx, result)) = hashed_stream.next().await {
            let (part_data, part_hash) = result?;

            let part_num = idx + 1; // Parts are indexed from 1
            self.upload_part(&b2upload, part_num, &part_hash, part_data).await?;
            part_hashes.push(part_hash);
        }

        self.finish_large_file(&file_id, &part_hashes).await
    }

    async fn upload_part(
        &self,
        B2Upload {
            ref upload_url,
            ref auth_token,
        }: &B2Upload,
        part_index: usize,
        sha1: &str,
        data: Bytes,
    ) -> BoxResult<()> {
        let (status, body) = self
            .request_with_backoff(|| {
                Request::post(upload_url)
                    .header(AUTHORIZATION, auth_token)
                    .header(CONNECTION, "keep-alive")
                    .header(CONTENT_TYPE, "application/octet-stream")
                    .header(CONTENT_LENGTH, data.len())
                    .header("X-Bz-Part-Number", part_index.to_string())
                    .header("X-Bz-Content-Sha1", sha1)
                    .body(Body::from(data.clone()))
                    .unwrap()
            })
            .await?;

        Self::get_json_reply("upload_file", status, body).await?;
        Ok(())
    }

    async fn finish_large_file(&self, file_id: &str, part_hashes: &[String]) -> BoxResult<RemoteFileVersion> {
        let part_hashes_json = part_hashes
            .iter()
            .map(|hash| '"'.to_string() + hash + "\"")
            .collect::<Vec<_>>()
            .join(",");
        let body_json = format!(
            r#"{{
                "fileId": "{}",
                "partSha1Array": [{}]
            }}"#,
            file_id, part_hashes_json
        );

        let (status, body) = self
            .request_with_backoff(|| {
                Request::post(self.api_url.clone() + "/b2api/v2/b2_finish_large_file")
                    .header(AUTHORIZATION, self.auth_token.clone())
                    .header(CONNECTION, "keep-alive")
                    .body(Body::from(body_json.clone()))
                    .unwrap()
            })
            .await?;

        let reply_json = Self::get_json_reply("finish_large_file", status, body).await?;
        Ok(RemoteFileVersion {
            path: reply_json["fileName"].as_str().unwrap().to_string(),
            id: reply_json["fileId"].as_str().unwrap().to_string(),
        })
    }

    async fn cancel_large_file(&self, file_id: &str) -> BoxResult<()> {
        let (status, body) = self
            .request_with_backoff(|| {
                Request::post(self.api_url.clone() + "/b2api/v2/b2_cancel_large_file")
                    .header(AUTHORIZATION, self.auth_token.clone())
                    .header(CONNECTION, "keep-alive")
                    .body(Body::from(format!(
                        r#"{{
                            "fileId": "{}"
                        }}"#,
                        file_id
                    )))
                    .unwrap()
            })
            .await?;

        Self::get_json_reply("finish_large_file", status, body).await?;
        Ok(())
    }

    async fn start_large_file(&self, filename: &str, enc_meta: &str) -> BoxResult<String> {
        let (status, body) = self
            .request_with_backoff(|| {
                Request::post(self.api_url.clone() + "/b2api/v2/b2_start_large_file")
                    .header(AUTHORIZATION, self.auth_token.clone())
                    .header(CONNECTION, "keep-alive")
                    .body(Body::from(format!(
                        r#"{{
                            "bucketId": "{}",
                            "fileName": "{}",
                            "contentType": "application/octet-stream",
                            "fileInfo":{{
                                "enc_meta": "{}"
                            }}
                        }}"#,
                        self.bucket_id,
                        filename,
                        enc_meta.to_owned()
                    )))
                    .unwrap()
            })
            .await?;

        let reply_json = Self::get_json_reply("start_large_file", status, body).await?;
        Ok(reply_json["fileId"].as_str().unwrap().to_string())
    }

    async fn get_json_reply(api_name: &str, status: StatusCode, body: Bytes) -> BoxResult<Value> {
        let reply_json: Value = match serde_json::from_slice(&body) {
            Err(_) => {
                return Err(From::from(format!(
                    "{} failed to parse json: {}",
                    api_name,
                    std::str::from_utf8(&body).unwrap()
                )))
            }
            Ok(json) => json,
        };

        if !status.is_success() {
            return Err(From::from(format!(
                "{} failed with error {}: {}, {}",
                api_name,
                status.as_u16(),
                reply_json["code"],
                reply_json["message"]
            )));
        }
        Ok(reply_json)
    }

    pub async fn download_file(&self, filename: &str) -> BoxResult<Bytes> {
        let body = self.download_file_stream(filename).await?;
        Ok(hyper::body::to_bytes(body).await?)
    }

    pub async fn download_file_stream(&self, filename: &str) -> BoxResult<Body> {
        let filename = filename.to_owned();
        let (status, body) = self
            .request_stream_with_backoff(|| {
                Request::get(self.bucket_download_url.clone() + &filename)
                    .header(AUTHORIZATION, self.auth_token.clone())
                    .header(CONNECTION, "keep-alive")
                    .body(Body::empty())
                    .unwrap()
            })
            .await?;

        if !status.is_success() {
            return Err(From::from(format!(
                "Download of {} failed with error {}",
                filename,
                status.as_u16()
            )));
        }

        Ok(body)
    }

    pub async fn hide_file(&self, file_path_hash: &str) -> BoxResult<()> {
        let (status, body) = self
            .request_with_backoff(|| {
                Request::post(self.api_url.clone() + "/b2api/v2/b2_hide_file")
                    .header(AUTHORIZATION, self.auth_token.clone())
                    .header(CONNECTION, "keep-alive")
                    .body(Body::from(format!(
                        "{{\"bucketId\": \"{}\", \
                         \"fileName\": \"{}\"}}",
                        self.bucket_id, file_path_hash
                    )))
                    .unwrap()
            })
            .await?;

        if !status.is_success() {
            let reply_json: Value = serde_json::from_slice(&body)?;
            return Err(From::from(format!(
                "Hiding of {} failed with error {}: {}",
                file_path_hash,
                status.as_u16(),
                reply_json["message"]
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
pub mod test_helpers {
    use super::{make_client, B2};
    use crate::crypto::Key;

    pub fn test_b2(key: Key) -> B2 {
        B2 {
            key,
            bucket_id: "bucket_id".to_string(),
            acc_id: "acc_id".to_string(),
            auth_token: "auth_token".to_string(),
            api_url: "https://example.org/api/".to_string(),
            bucket_download_url: "https://example.org/download_url/".to_string(),
            client: make_client(),
            progress: None,
        }
    }
}
