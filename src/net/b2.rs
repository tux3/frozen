use crate::config::Config;
use crate::crypto::{self, decode_meta, encode_meta, sha1_string, AppKeys};
use crate::data::file::{RemoteFile, RemoteFileVersion};
use crate::progress::ProgressHandler;
use crate::stream::{HashedStream, SimpleBytesStream};
use bytes::Bytes;
use data_encoding::BASE64_NOPAD;
use eyre::{bail, ensure, eyre, Result};
use futures::stream::BoxStream;
use futures::{Stream, StreamExt};
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE};
use reqwest::{tls, Body, Client, ClientBuilder, Response, StatusCode, Url};
use serde_json::{self, json, Value};
use std::future::Future;
use std::iter::FromIterator;
use std::path::Path;
use std::str::{from_utf8, FromStr};
use std::thread::sleep;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Copy, Clone)]
pub enum FileListDepth {
    Shallow,
    // List only files in the current "folder"
    Deep, // List every file recursively
}

#[derive(Clone, PartialEq, Debug)]
pub struct B2Upload {
    pub upload_url: String,
    pub auth_token: String,
}

#[derive(Clone)]
pub struct B2 {
    pub key: crypto::Key,
    pub bucket_id: String,
    pub acc_id: String,
    pub auth_token: String,
    pub api_url: Url,
    pub bucket_download_url: Url,
    pub client: Client,
    pub progress: Option<ProgressHandler>,
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

fn base_client() -> ClientBuilder {
    Client::builder()
        .https_only(true)
        .min_tls_version(tls::Version::TLS_1_2)
}

impl B2 {
    async fn request_with_backoff<Fn, Fut>(&self, req_fn: Fn) -> Result<(StatusCode, Bytes)>
    where
        Fn: FnMut() -> Fut,
        Fut: Future<Output = Result<Response, reqwest::Error>>,
    {
        let (status, response) = self.request_response_with_backoff(req_fn).await?;
        Ok((status, response.bytes().await?))
    }

    async fn request_response_with_backoff<Fn, Fut>(&self, mut req_fn: Fn) -> Result<(StatusCode, Response)>
    where
        Fn: FnMut() -> Fut,
        Fut: Future<Output = Result<Response, reqwest::Error>>,
    {
        let mut hard_fails = 0u32;
        let mut attempts = 0u32;
        loop {
            attempts += 1;
            if attempts > 1 {
                let cooldown = (1 << attempts.min(5)) * 100; // Up to 3.2 seconds
                sleep(Duration::from_millis(cooldown));
            }

            let res = match req_fn().await {
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

            return Ok((status, res));
        }
    }

    pub async fn authenticate(config: &Config, keys: &AppKeys) -> Result<B2> {
        let client = base_client().build().expect("Failed to build HTTP client");
        let basic_auth = make_basic_auth(keys);
        let bucket_name = config.bucket_name.to_owned();

        let res = client
            .get("https://api.backblazeb2.com/b2api/v2/b2_authorize_account")
            .header(AUTHORIZATION, basic_auth)
            .send()
            .await?;
        let status = res.status();
        let body = res.bytes().await?;

        let reply_json: Value = match serde_json::from_slice(&body) {
            Err(_) => bail!(
                "authenticate failed to parse json: {}",
                std::str::from_utf8(&body).unwrap()
            ),
            Ok(json) => json,
        };

        if !status.is_success() {
            let mut err_msg = "Backblaze B2 login failure: ".to_string() + from_utf8(&body).unwrap();
            if let Value::String(ref reply_err_msg) = reply_json["message"] {
                err_msg += &(String::from(": ") + reply_err_msg);
            }
            bail!(err_msg);
        }

        let auth_token = reply_json["authorizationToken"].as_str().unwrap().to_string();
        let bucket_download_url = Url::from_str(&format!(
            "{}/file/{}/",
            reply_json["downloadUrl"].as_str().unwrap(),
            &config.bucket_name
        ))?;

        let headers = HeaderMap::from_iter([(AUTHORIZATION, HeaderValue::from_str(&auth_token)?)]);
        let client = base_client()
            .default_headers(headers)
            .build()
            .expect("Failed to build HTTP client");
        let api_url = Url::from_str(reply_json["apiUrl"].as_str().unwrap())?.join("b2api/v2/")?;

        let mut b2 = B2 {
            key: keys.encryption_key.clone(),
            acc_id: reply_json["accountId"].as_str().unwrap().to_string(),
            auth_token,
            bucket_id: String::new(),
            api_url,
            bucket_download_url,
            progress: None,
            client,
        };

        let bucket_id = b2.get_bucket_id(&bucket_name).await?;
        b2.bucket_id = bucket_id;

        Ok(b2)
    }

    async fn get_bucket_id(&self, bucket_name: &str) -> Result<String> {
        let bucket_name = bucket_name.to_owned(); // Can't wait for the Pin API!

        let (status, body) = self
            .request_with_backoff(|| async {
                self.client
                    .post(self.api_url.join("b2_list_buckets").unwrap())
                    .json(&json!({
                         "bucketName": bucket_name,
                         "accountId": self.acc_id
                    }))
                    .send()
                    .await
            })
            .await?;

        let reply_json = Self::get_json_reply("get_bucket_id", status, body).await?;

        let buckets = reply_json["buckets"].as_array().unwrap();
        for bucket in buckets {
            if bucket["bucketName"] == bucket_name {
                return Ok(bucket["bucketId"].as_str().unwrap().to_string());
            }
        }
        Err(eyre!("Bucket '{}' not found", bucket_name))
    }

    pub async fn list_remote_files(&self, prefix: &str, depth: FileListDepth) -> Result<Vec<RemoteFile>> {
        let delimiter = match depth {
            FileListDepth::Shallow => Some("/"),
            FileListDepth::Deep => None,
        };
        let body = json!({
            "bucketId": self.bucket_id,
            "maxFileCount": 10000,
            "delimiter": delimiter,
            "prefix": prefix,
        });
        let mut start_filename: Option<String> = None;
        let mut files: Vec<RemoteFile> = Vec::new();

        loop {
            let (status, body) = self
                .request_with_backoff(|| async {
                    let mut body = body.clone();
                    if start_filename.is_some() {
                        body.as_object_mut()
                            .unwrap()
                            .insert("startFileName".into(), start_filename.clone().unwrap().into());
                    }

                    self.client
                        .post(self.api_url.join("b2_list_file_names").unwrap())
                        .json(&body)
                        .send()
                        .await
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

    pub async fn list_remote_file_versions(&self, prefix: &str) -> Result<Vec<RemoteFileVersion>> {
        let body = json!({
            "bucketId": self.bucket_id,
            "maxFileCount": 10000,
            "prefix": prefix,
        });
        let mut start_file_version: Option<RemoteFileVersion> = None;
        let mut files: Vec<RemoteFileVersion> = Vec::new();

        loop {
            let (status, body) = self
                .request_with_backoff(|| async {
                    let mut body = body.clone();
                    if start_file_version.is_some() {
                        let ver = start_file_version.as_ref().unwrap();
                        let body_mut = body.as_object_mut().unwrap();
                        body_mut.insert("startFileName".into(), ver.path.clone().into());
                        body_mut.insert("startFileId".into(), ver.id.clone().into());
                    }

                    self.client
                        .post(self.api_url.join("b2_list_file_versions").unwrap())
                        .json(&body)
                        .send()
                        .await
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

    pub async fn list_unfinished_large_files(&self, prefix: &str) -> Result<Vec<RemoteFile>> {
        let body = json!({
            "bucketId": self.bucket_id,
            "namePrefix": prefix,
        });
        let mut start_file_version: Option<String> = None;
        let mut unfinished_files: Vec<RemoteFile> = Vec::new();

        loop {
            let (status, body) = self
                .request_with_backoff(|| async {
                    let mut body = body.clone();
                    if let Some(ver) = start_file_version.as_deref() {
                        let body_mut = body.as_object_mut().unwrap();
                        body_mut.insert("startFileId".into(), ver.into());
                    };

                    self.client
                        .post(self.api_url.join("b2_list_unfinished_large_files").unwrap())
                        .json(&body)
                        .send()
                        .await
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

    pub async fn get_upload_url(&self) -> Result<B2Upload> {
        let (status, body) = self
            .request_with_backoff(|| async {
                self.client
                    .post(self.api_url.join("b2_get_upload_url").unwrap())
                    .json(&json!({"bucketId": self.bucket_id}))
                    .send()
                    .await
            })
            .await?;

        let reply_json = Self::get_json_reply("get_upload_url", status, body).await?;
        Ok(B2Upload {
            upload_url: reply_json["uploadUrl"].as_str().unwrap().to_string(),
            auth_token: reply_json["authorizationToken"].as_str().unwrap().to_string(),
        })
    }

    /// The returned B2Upload struct is only valid for the one large file being uploaded
    pub async fn get_upload_part_url(&self, file_id: &str) -> Result<B2Upload> {
        let (status, body) = self
            .request_with_backoff(|| async {
                self.client
                    .post(self.api_url.join("b2_get_upload_part_url").unwrap())
                    .json(&json!({ "fileId": file_id }))
                    .send()
                    .await
            })
            .await?;

        let reply_json: Value = serde_json::from_slice(&body)?;
        ensure!(
            status.is_success(),
            "get_upload_part_url failed with error {}: {}",
            status.as_u16(),
            reply_json["message"]
        );

        Ok(B2Upload {
            upload_url: reply_json["uploadUrl"].as_str().unwrap().to_string(),
            auth_token: reply_json["authorizationToken"].as_str().unwrap().to_string(),
        })
    }

    pub async fn delete_file_version(&self, file_version: &RemoteFileVersion) -> Result<()> {
        let (status, body) = self
            .request_with_backoff(|| async {
                self.client
                    .post(self.api_url.join("b2_delete_file_version").unwrap())
                    .json(&json!({
                        "fileId": file_version.id,
                         "fileName": file_version.path,
                    }))
                    .send()
                    .await
            })
            .await?;

        if !status.is_success() {
            let reply_json: Value = serde_json::from_slice(&body)?;
            bail!(
                "Removal of {} failed with error {}: {}",
                file_version.path,
                status.as_u16(),
                reply_json["message"]
            );
        }
        Ok(())
    }

    pub async fn upload_file_simple(&self, filename: &str, data: Vec<u8>) -> Result<RemoteFileVersion> {
        let upload_url = self.get_upload_url().await?;
        self.upload_file(&upload_url, filename, data, None).await
    }

    pub async fn upload_file(
        &self,
        b2upload: &B2Upload,
        filename: &str,
        data: Vec<u8>,
        enc_meta: Option<String>,
    ) -> Result<RemoteFileVersion> {
        let data_stream = Box::new(SimpleBytesStream::new(data.into()));
        self.upload_file_stream(b2upload, filename, data_stream, enc_meta).await
    }

    pub async fn upload_file_stream(
        &self,
        b2upload: &B2Upload,
        filename: &str,
        data_stream: impl Stream<Item = Result<Bytes>> + Unpin + Send + Sync + 'static,
        enc_meta: Option<String>,
    ) -> Result<RemoteFileVersion> {
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

    /// Uploads a stream in one shot using b2_upload_file
    async fn upload_small_file_stream(
        &self,
        b2upload: &B2Upload,
        filename: &str,
        mut data_stream: impl Stream<Item = Result<Bytes>> + Unpin + Send + Sync + 'static,
        enc_meta: &str,
    ) -> Result<RemoteFileVersion> {
        let data = data_stream.next().await;
        let data = data.expect("Data stream to upload must not be empty")?;
        // Small files here means files that have only one chunk
        assert!(data_stream.next().await.is_none());

        let sha1 = sha1_string(&data);

        let (status, body) = self
            .request_with_backoff(|| async {
                self.client
                    .post(&b2upload.upload_url)
                    .header(AUTHORIZATION, &b2upload.auth_token as &str)
                    .header(CONTENT_TYPE, "application/octet-stream")
                    .header(CONTENT_LENGTH, data.len())
                    .header("X-Bz-File-Name", filename.to_string())
                    .header("X-Bz-Content-Sha1", sha1.clone())
                    .header("X-Bz-Info-enc_meta", enc_meta.to_owned())
                    .body(Body::from(data.clone()))
                    .send()
                    .await
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
        data_stream: impl Stream<Item = Result<Bytes>> + Unpin + Send + Sync + 'static,
        enc_meta: &str,
    ) -> Result<RemoteFileVersion> {
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
        data_stream: impl Stream<Item = Result<Bytes>> + Unpin + Send + Sync + 'static,
    ) -> Result<RemoteFileVersion> {
        let b2upload = self.get_upload_part_url(file_id).await?;
        let mut part_hashes = Vec::<String>::new();

        let hashed_stream = HashedStream::new(Box::new(data_stream));
        let mut hashed_stream = hashed_stream.enumerate();

        while let Some((idx, result)) = hashed_stream.next().await {
            let (part_data, part_hash) = result?;

            let part_num = idx + 1; // Parts are indexed from 1
            self.upload_part(&b2upload, part_num, &part_hash, part_data).await?;
            part_hashes.push(part_hash);
        }

        self.finish_large_file(file_id, &part_hashes).await
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
    ) -> Result<()> {
        let (status, body) = self
            .request_with_backoff(|| async {
                self.client
                    .post(upload_url)
                    .header(AUTHORIZATION, auth_token)
                    .header(CONTENT_TYPE, "application/octet-stream")
                    .header(CONTENT_LENGTH, data.len())
                    .header("X-Bz-Part-Number", part_index.to_string())
                    .header("X-Bz-Content-Sha1", sha1)
                    .body(Body::from(data.clone()))
                    .send()
                    .await
            })
            .await?;

        Self::get_json_reply("upload_file", status, body).await?;
        Ok(())
    }

    async fn finish_large_file(&self, file_id: &str, part_hashes: &[String]) -> Result<RemoteFileVersion> {
        let (status, body) = self
            .request_with_backoff(|| async {
                self.client
                    .post(self.api_url.join("b2_finish_large_file").unwrap())
                    .json(&json!({
                        "fileId": file_id,
                        "partSha1Array": part_hashes,
                    }))
                    .send()
                    .await
            })
            .await?;

        let reply_json = Self::get_json_reply("finish_large_file", status, body).await?;
        Ok(RemoteFileVersion {
            path: reply_json["fileName"].as_str().unwrap().to_string(),
            id: reply_json["fileId"].as_str().unwrap().to_string(),
        })
    }

    async fn cancel_large_file(&self, file_id: &str) -> Result<()> {
        let (status, body) = self
            .request_with_backoff(|| async {
                self.client
                    .post(self.api_url.join("b2_cancel_large_file").unwrap())
                    .json(&json!({ "fileId": file_id }))
                    .send()
                    .await
            })
            .await?;

        Self::get_json_reply("finish_large_file", status, body).await?;
        Ok(())
    }

    async fn start_large_file(&self, filename: &str, enc_meta: &str) -> Result<String> {
        let (status, body) = self
            .request_with_backoff(|| async {
                self.client
                    .post(self.api_url.join("b2_start_large_file").unwrap())
                    .json(&json!({
                        "bucketId": self.bucket_id,
                        "fileName": filename,
                        "contentType": "application/octet-stream",
                        "fileInfo": {
                            "enc_meta": enc_meta
                        }
                    }))
                    .send()
                    .await
            })
            .await?;

        let reply_json = Self::get_json_reply("start_large_file", status, body).await?;
        Ok(reply_json["fileId"].as_str().unwrap().to_string())
    }

    async fn get_json_reply(api_name: &str, status: StatusCode, body: Bytes) -> Result<Value> {
        let reply_json: Value = match serde_json::from_slice(&body) {
            Err(_) => {
                bail!(
                    "{} failed to parse json: {}",
                    api_name,
                    std::str::from_utf8(&body).unwrap()
                );
            }
            Ok(json) => json,
        };

        ensure!(
            status.is_success(),
            "{} failed with error {}: {}, {}",
            api_name,
            status.as_u16(),
            reply_json["code"],
            reply_json["message"]
        );
        Ok(reply_json)
    }

    pub async fn download_file(&self, filename: &str) -> Result<Bytes> {
        let res = self.download_file_response(filename).await?;
        Ok(res.bytes().await?)
    }

    pub async fn download_file_stream(
        &self,
        filename: &str,
    ) -> Result<BoxStream<'static, Result<Bytes, reqwest::Error>>> {
        let res = self.download_file_response(filename).await?;
        Ok(res.bytes_stream().boxed())
    }

    async fn download_file_response(&self, filename: &str) -> Result<Response> {
        let (status, body) = self
            .request_response_with_backoff(|| async {
                self.client
                    .get(self.bucket_download_url.join(filename).unwrap())
                    .send()
                    .await
            })
            .await?;

        ensure!(
            status.is_success(),
            "Download of {} failed with error {}",
            filename,
            status.as_u16()
        );
        Ok(body)
    }

    pub async fn hide_file(&self, file_path_hash: &str) -> Result<()> {
        let (status, body) = self
            .request_with_backoff(|| async {
                self.client
                    .post(self.api_url.join("b2_hide_file").unwrap())
                    .json(&json!({
                        "bucketId": self.bucket_id,
                        "fileName": file_path_hash
                    }))
                    .send()
                    .await
            })
            .await?;

        if !status.is_success() {
            let reply_json: Value = serde_json::from_slice(&body)?;
            bail!(
                "Hiding of {} failed with error {}: {}",
                file_path_hash,
                status.as_u16(),
                reply_json["message"]
            );
        }
        Ok(())
    }
}

#[cfg(test)]
pub mod test_helpers {
    use super::{base_client, B2};
    use crate::crypto::Key;
    use reqwest::Url;
    use std::str::FromStr;

    pub fn test_b2(key: Key) -> B2 {
        B2 {
            key,
            bucket_id: "bucket_id".to_string(),
            acc_id: "acc_id".to_string(),
            auth_token: "auth_token".to_string(),
            api_url: Url::from_str("https://example.org/api/").unwrap(),
            bucket_download_url: Url::from_str("https://example.org/download_url/").unwrap(),
            client: base_client().build().unwrap(),
            progress: None,
        }
    }
}
