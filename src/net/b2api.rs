use std::error::Error;
use std::io::Read;
use std::vec::Vec;
use std::thread::sleep;
use std::time::{SystemTime, Duration, UNIX_EPOCH};
use crypto::{self, encode_meta, decode_meta};
use data::file::{RemoteFile, RemoteFileVersion};
use config::Config;
use progress::ProgressDataReader;
use hyper::client::{Client, Body};
use hyper::client::response::Response;
use hyper::header::{Authorization, Basic, ContentType, ContentLength};
use hyper::net::HttpsConnector;
use hyper_openssl::OpensslClient;
use serde_json::{self, Value};

header!{(XBzFileName, "X-Bz-File-Name") => [String]}
header!{(XBzContentSha1, "X-Bz-Content-Sha1") => [String]}
header!{(XBzEncMeta, "X-Bz-Info-enc_meta") => [String]}

#[derive(Clone, PartialEq)]
pub struct B2Upload {
    pub url: String,
    pub auth_token: String,
}

#[derive(Clone, PartialEq)]
pub struct B2 {
    pub key: crypto::Key,
    pub bucket_id: String,
    pub acc_id: String,
    pub auth_token: String,
    pub api_url: String,
    pub download_url: String,
    pub upload: Option<B2Upload>,
}

fn get_frozen_bucket_id(b2: &B2) -> Result<String, Box<Error>> {
    let client = make_client();
    let basic_auth = Authorization(b2.auth_token.clone());
    let url = b2.api_url.clone()+"/b2api/v1/b2_list_buckets";
    let mut reply: Response = client.post(&url)
        .header(basic_auth)
        .body(&format!("{{\"accountId\":\"{}\"}}", b2.acc_id))
        .send()?;

    let reply_data = &mut String::new();
    reply.read_to_string(reply_data)?;
    let reply_json: Value = serde_json::from_str(reply_data)?;

    if !reply.status.is_success() {
        return Err(From::from(format!("get_frozen_bucket_id failed with error {}: {}",
                                      reply.status.to_u16(),
                                      reply_json["message"])));
    }

    let buckets = reply_json["buckets"].as_array().unwrap();
    for bucket in buckets {
        if bucket["bucketName"] == "frozen" {
            return Ok(bucket["bucketId"].as_str().unwrap().to_string())
        }
    }
    Err(From::from("Bucket 'frozen' not found"))
}

pub fn list_remote_files(b2: &B2, prefix: &str) -> Result<Vec<RemoteFile>, Box<Error>> {
    let client = make_client();
    let url = b2.api_url.clone()+"/b2api/v1/b2_list_file_names";

    let body_base = format!("\"bucketId\":\"{}\",\
                            \"maxFileCount\":10000,\
                            \"prefix\":\"{}\"", b2.bucket_id, prefix);
    let mut body: String;
    let mut start_filename: Option<String> = None;
    let mut files: Vec<RemoteFile> = Vec::new();

    loop {
        if start_filename.is_some() {
            body = format!("{{\"startFileName\":\"{}\",\
                            {}}}", start_filename.as_ref().unwrap(), body_base)
        } else {
            body = format!("{{{}}}", body_base)
        }
        let mut reply: Response = client.post(&url)
            .header(Authorization(b2.auth_token.clone()))
            .body(&body)
            .send()?;

        let reply_data = &mut String::new();
        reply.read_to_string(reply_data)?;
        let reply_json: Value = serde_json::from_str(reply_data)?;

        if !reply.status.is_success() {
            return Err(From::from(format!("list_remote_files failed with error {}: {}",
                                          reply.status.to_u16(),
                                          reply_json["message"])));
        }

        for file in reply_json["files"].as_array().unwrap() {
            let full_name = file["fileName"].as_str().unwrap();
            let id = file["fileId"].as_str().unwrap();
            let enc_meta = file["fileInfo"]["enc_meta"].as_str().unwrap();
            let (filename, last_modified, is_symlink) = decode_meta(&b2.key, enc_meta)?;
            files.push(RemoteFile::new(&filename, full_name, id, last_modified, is_symlink)?)
        }

        let maybe_next = reply_json["nextFileName"].as_str();
        if let Some(next) = maybe_next {
            start_filename = Some(next.to_string());
        } else {
            break;
        }
    }

    Ok(files)
}

pub fn list_remote_file_versions(b2: &B2, prefix: &str)
                                 -> Result<Vec<RemoteFileVersion>, Box<Error>> {
    let client = make_client();
    let url = b2.api_url.clone()+"/b2api/v1/b2_list_file_versions";

    let body_base = format!("\"bucketId\":\"{}\",\
                            \"maxFileCount\":10000,\
                            \"prefix\":\"{}\"", b2.bucket_id, prefix);
    let mut body: String;
    let mut start_file_version: Option<RemoteFileVersion> = None;
    let mut files: Vec<RemoteFileVersion> = Vec::new();

    loop {
        if start_file_version.is_some() {
            let ver = start_file_version.as_ref().unwrap();
            body = format!("{{\"startFileName\":\"{}\",\
                              \"startFileId\":\"{}\",\
                             {}}}", ver.path, ver.id, body_base)
        } else {
            body = format!("{{{}}}", body_base)
        }
        let mut reply: Response = client.post(&url)
            .header(Authorization(b2.auth_token.clone()))
            .body(&body)
            .send()?;

        let reply_data = &mut String::new();
        reply.read_to_string(reply_data)?;
        let reply_json: Value = serde_json::from_str(reply_data)?;

        if !reply.status.is_success() {
            return Err(From::from(format!("list_remote_files_versions failed with error {}: {}",
                                          reply.status.to_u16(),
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
        if maybe_next_name.is_some() && maybe_next_id.is_some() {
            start_file_version = Some(RemoteFileVersion{
                path: maybe_next_name.unwrap().to_string(),
                id: maybe_next_id.unwrap().to_string()
            });
        } else {
            break;
        }
    }

    Ok(files)
}

fn get_upload_url(b2: &mut B2) -> Result<B2Upload, Box<Error>> {
    let client = make_client();
    let basic_auth = Authorization(b2.auth_token.clone());
    let url = b2.api_url.clone()+"/b2api/v1/b2_get_upload_url";
    let mut reply: Response = client.post(&url)
        .header(basic_auth)
        .body(&format!("{{\"bucketId\":\"{}\"}}", b2.bucket_id))
        .send()?;

    let reply_data = &mut String::new();
    reply.read_to_string(reply_data)?;
    let reply_json: Value = serde_json::from_str(reply_data)?;

    if !reply.status.is_success() {
        return Err(From::from(format!("get_upload_url failed with error {}: {}",
                                      reply.status.to_u16(),
                                      reply_json["message"])));
    }

    Ok(B2Upload {
        url: reply_json["uploadUrl"].as_str().unwrap().to_string(),
        auth_token: reply_json["authorizationToken"].as_str().unwrap().to_string(),
    })
}

pub fn upload_file(b2: &mut B2, filename: &str,
                        data: &mut ProgressDataReader,
                        enc_meta: Option<String>) -> Result<RemoteFileVersion, Box<Error>> {
    let enc_meta = if enc_meta.is_some() {
        enc_meta.unwrap()
    } else {
        let last_modified = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        encode_meta(&b2.key, &filename, last_modified, false)
    };

    let mut backoff = Duration::from_millis(500);
    for _ in 0..5 {
        if let Some(upload_version) = upload_file_once(b2, filename, data, &enc_meta)? {
            return Ok(upload_version);
        } else {
            sleep(backoff);
            backoff *= 2;
        }
    }
    return Err(From::from("Too many failed attempts"));
}

fn upload_file_once(b2: &mut B2, filename: &str,
                   data: &mut ProgressDataReader,
                   enc_meta: &str) -> Result<Option<RemoteFileVersion>, Box<Error>> {
    if b2.upload.is_none() {
        b2.upload = Some(get_upload_url(b2)?);
    }

    let reply = {
        let client = make_client();
        let sha1 = crypto::sha1_string(data.as_slice());
        let data_size = data.len() as u64;
        let body = Body::SizedBody(data, data_size);
        let b2upload = &b2.upload.as_mut().unwrap();
        let basic_auth = Authorization(b2upload.auth_token.clone());
        client.post(&b2upload.url)
            .header(basic_auth)
            .header(XBzFileName(filename.to_string()))
            .header(ContentType("application/octet-stream".parse().unwrap()))
            .header(ContentLength(data_size))
            .header(XBzContentSha1(sha1))
            .header(XBzEncMeta(enc_meta.to_owned()))
            .body(body)
            .send()
    };

    if reply.is_err() {
        return Ok(None)
    }
    let mut reply = reply.unwrap();

    let reply_data = &mut String::new();
    reply.read_to_string(reply_data)?;
    let reply_json: Value = serde_json::from_str(reply_data)?;

    // Temporary failure is not an error, just asking for an exponential backoff
    if reply.status.to_u16() == 503 || reply.status.to_u16() == 408 {
        return Ok(None);
    }

    if !reply.status.is_success() {
        b2.upload = None;
        return Err(From::from(format!("upload_file failed with error {}: {}",
                                      reply.status.to_u16(),
                                      reply_json["message"])));
    }

    Ok(Some(RemoteFileVersion {
        path: reply_json["fileName"].as_str().unwrap().to_string(),
        id: reply_json["fileId"].as_str().unwrap().to_string(),
    }))
}

pub fn download_file(b2: &B2, filename: &str) -> Result<Vec<u8>, Box<Error>> {
    let client = make_client();
    let basic_auth = Authorization(b2.auth_token.clone());
    let url = b2.download_url.clone()+"/file/frozen/"+filename;
    let mut reply: Response = client.get(&url)
        .header(basic_auth)
        .send()?;
    if !reply.status.is_success() {
        return Err(From::from(format!("Download of {} failed with error {}",
                                      filename, reply.status.to_u16())));
    }
    let mut reply_data = Vec::new();
    reply.read_to_end(&mut reply_data)?;
    Ok(reply_data)
}

pub fn delete_file_version(b2: &B2, file_version: &RemoteFileVersion) -> Result<(), Box<Error>> {
    let client = make_client();
    let basic_auth = Authorization(b2.auth_token.clone());
    let url = b2.api_url.clone()+"/b2api/v1/b2_delete_file_version";
    let mut reply: Response = client.post(&url)
        .header(basic_auth)
        .body(&format!("{{\"fileId\": \"{}\", \
                          \"fileName\": \"{}\"}}", file_version.id, file_version.path))
        .send()?;
    if !reply.status.is_success() {
        let reply_data = &mut String::new();
        reply.read_to_string(reply_data)?;
        let reply_json: Value = serde_json::from_str(reply_data)?;

        return Err(From::from(format!("Removal of {} failed with error {}: {}",
                                      file_version.path, reply.status.to_u16(),
                                      reply_json["message"])));
    }
    Ok(())
}

pub fn hide_file(b2: &B2, file_path_hash: &str) -> Result<(), Box<Error>> {
    let client = make_client();
    let basic_auth = Authorization(b2.auth_token.clone());
    let url = b2.api_url.clone()+"/b2api/v1/b2_hide_file";
    let mut reply: Response = client.post(&url)
        .header(basic_auth)
        .body(&format!("{{\"bucketId\": \"{}\", \
                          \"fileName\": \"{}\"}}", b2.bucket_id, file_path_hash))
        .send()?;
    if !reply.status.is_success() {
        let reply_data = &mut String::new();
        reply.read_to_string(reply_data)?;
        let reply_json: Value = serde_json::from_str(reply_data)?;

        return Err(From::from(format!("Hiding of {} failed with error {}: {}",
                                      file_path_hash, reply.status.to_u16(),
                                      reply_json["message"])));
    }
    Ok(())
}

pub fn authenticate(config: &Config) -> Result<B2, Box<Error>> {
    let client = make_client();
    let basic_auth = Authorization( Basic{
        username: config.acc_id.clone(),
        password: Some(config.app_key.clone()),
    });

    let mut reply: Response = client.get("https://api.backblazeb2.com/b2api/v1/b2_authorize_account")
            .header(basic_auth)
            .send()?;
    let reply_data = &mut String::new();
    reply.read_to_string(reply_data)?;
    let reply_json: Value = serde_json::from_str(reply_data)?;

    if !reply.status.is_success() {
        let mut err_msg = String::from("Backblaze B2 login failure");
        if let Value::String(ref reply_err_msg) = reply_json["message"] {
            err_msg += &(String::from(": ")+&reply_err_msg);
        }
        return Err(From::from(err_msg));
    }

    let mut b2 = B2{
        key: config.key.clone(),
        acc_id: reply_json["accountId"].as_str().unwrap().to_string(),
        auth_token: reply_json["authorizationToken"].as_str().unwrap().to_string(),
        bucket_id: String::new(),
        api_url: reply_json["apiUrl"].as_str().unwrap().to_string(),
        download_url: reply_json["downloadUrl"].as_str().unwrap().to_string(),
        upload: None,
    };
    b2.bucket_id = get_frozen_bucket_id(&b2)?;
    Ok(b2)
}

fn make_client() -> Client {
    let ssl = OpensslClient::new().unwrap();
    let connector = HttpsConnector::new(ssl);
    Client::with_connector(connector)
}