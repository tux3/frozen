use std::error::Error;
use std::io::Read;
use std::vec::Vec;
use std::time::{SystemTime, UNIX_EPOCH};
use crypto::{self, encode_meta, decode_meta};
use data::file::RemoteFile;
use config::Config;
use progress::ProgressDataReader;
use hyper::client::{Client, Body};
use hyper::client::response::Response;
use hyper::header::{Authorization, Basic, ContentType, ContentLength};
use hyper::net::HttpsConnector;
use hyper_openssl::OpensslClient;
use rustc_serialize::json::Json;

header!{(XBzFileName, "X-Bz-File-Name") => [String]}
header!{(XBzContentSha1, "X-Bz-Content-Sha1") => [String]}
header!{(XBzEncMeta, "X-Bz-Info-enc_meta") => [String]}

#[derive(Clone)]
pub struct B2Upload {
    pub url: String,
    pub auth_token: String,
}

#[derive(Clone)]
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
    let reply_json: Json = Json::from_str(reply_data)?;

    if !reply.status.is_success() {
        return Err(From::from(format!("get_frozen_bucket_id failed with error {}: {}",
                                      reply.status.to_u16(),
                                      reply_json.find("message").unwrap())));
    }

    let buckets = reply_json.find("buckets").unwrap().as_array().unwrap();
    for bucket in buckets {
        if bucket.find("bucketName").unwrap().as_string().unwrap() == "frozen" {
            return Ok(bucket.find("bucketId").unwrap().as_string().unwrap().to_string())
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
        let reply_json: Json = Json::from_str(reply_data)?;

        if !reply.status.is_success() {
            return Err(From::from(format!("list_remote_files failed with error {}: {}",
                                          reply.status.to_u16(),
                                          reply_json.find("message").unwrap())));
        }

        for file in reply_json.find("files").unwrap().as_array().unwrap() {
            let fullname = file.find("fileName").unwrap().as_string().unwrap();
            let enc_meta = file.find("fileInfo").unwrap()
                                    .find("enc_meta").unwrap().as_string().unwrap();
            let (filename, last_modified) = decode_meta(&b2.key, enc_meta)?;
            files.push(RemoteFile::new(&filename, &fullname, last_modified)?)
        }

        let maybe_next = reply_json.find("nextFileName").unwrap().as_string();
        if maybe_next.is_some() {
            start_filename = Some(maybe_next.unwrap().to_string());
        } else {
            break;
        }
    }

    Ok(files)
}

pub fn list_remote_file_versions(b2: &B2, prefix: &str) -> Result<Vec<String>, Box<Error>> {
    let client = make_client();
    let url = b2.api_url.clone()+"/b2api/v1/b2_list_file_versions";

    let body_base = format!("\"bucketId\":\"{}\",\
                            \"maxFileCount\":10000,\
                            \"prefix\":\"{}\"", b2.bucket_id, prefix);
    let mut body: String;
    let mut start_file_id: Option<String> = None;
    let mut files: Vec<String> = Vec::new();

    loop {
        if start_file_id.is_some() {
            body = format!("{{\"startFileId\":\"{}\",\
                            {}}}", start_file_id.as_ref().unwrap(), body_base)
        } else {
            body = format!("{{{}}}", body_base)
        }
        let mut reply: Response = client.post(&url)
            .header(Authorization(b2.auth_token.clone()))
            .body(&body)
            .send()?;

        let reply_data = &mut String::new();
        reply.read_to_string(reply_data)?;
        let reply_json: Json = Json::from_str(reply_data)?;

        if !reply.status.is_success() {
            return Err(From::from(format!("list_remote_files_versions failed with error {}: {}",
                                          reply.status.to_u16(),
                                          reply_json.find("message").unwrap())));
        }

        for file in reply_json.find("files").unwrap().as_array().unwrap() {
            files.push(file.find("fileId").unwrap().as_string().unwrap().to_string());
        }

        let maybe_next = reply_json.find("nextFileId").unwrap().as_string();
        if maybe_next.is_some() {
            start_file_id = Some(maybe_next.unwrap().to_string());
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
    let reply_json: Json = Json::from_str(reply_data)?;

    if !reply.status.is_success() {
        return Err(From::from(format!("get_upload_url failed with error {}: {}",
                                      reply.status.to_u16(),
                                      reply_json.find("message").unwrap())));
    }

    Ok(B2Upload {
        url: reply_json.find("uploadUrl").unwrap().as_string().unwrap().to_string(),
        auth_token: reply_json.find("authorizationToken").unwrap().as_string().unwrap().to_string(),
    })
}

pub fn upload_file(b2: &mut B2, filename: &str,
                   data: &mut ProgressDataReader,
                   last_modified: Option<u64>,
                   plain_filename: Option<&str>) -> Result<(), Box<Error>> {
    if b2.upload.is_none() {
        b2.upload = Some(get_upload_url(b2)?);
    }

    let mut reply: Response;

    {
        let last_modified = last_modified.unwrap_or_else(||
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs());
        let plain_filename = plain_filename.unwrap_or_else(|| filename);
        let meta_enc = encode_meta(&b2.key, &plain_filename, last_modified);
        let client = make_client();
        let sha1 = crypto::sha1_string(data.as_slice());
        let data_size = data.len() as u64;
        let body = Body::SizedBody(data, data_size);
        let b2upload = &b2.upload.as_mut().unwrap();
        let basic_auth = Authorization(b2upload.auth_token.clone());
        reply = client.post(&b2upload.url)
            .header(basic_auth)
            .header(XBzFileName(filename.to_string()))
            .header(ContentType("application/octet-stream".parse().unwrap()))
            .header(ContentLength(data_size))
            .header(XBzContentSha1(sha1))
            .header(XBzEncMeta(meta_enc))
            .body(body)
            .send()?;
    }

    let reply_data = &mut String::new();
    reply.read_to_string(reply_data)?;
    let reply_json: Json = Json::from_str(reply_data)?;

    if !reply.status.is_success() {
        b2.upload = None;
        return Err(From::from(format!("upload_file failed with error {}: {}",
                                      reply.status.to_u16(),
                                      reply_json.find("message").unwrap())));
    }
    Ok(())
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

pub fn delete_file(b2: &B2, file_name: &str) -> Result<(), Box<Error>> {
    let files = list_remote_file_versions(b2, file_name)?;
    for file_id in files {
        let client = make_client();
        let basic_auth = Authorization(b2.auth_token.clone());
        let url = b2.api_url.clone()+"/b2api/v1/b2_delete_file_version";
        let mut reply: Response = client.post(&url)
            .header(basic_auth)
            .body(&format!("{{\"fileId\": \"{}\", \
                              \"fileName\": \"{}\"}}", file_id, file_name))
            .send()?;
        if !reply.status.is_success() {
            let reply_data = &mut String::new();
            reply.read_to_string(reply_data)?;
            let reply_json: Json = Json::from_str(reply_data)?;

            return Err(From::from(format!("Removal of {} failed with error {}: {}",
                                          file_name, reply.status.to_u16(),
                                          reply_json.find("message").unwrap())));
        }
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
    let reply_json: Json = Json::from_str(reply_data)?;

    if !reply.status.is_success() {
        let mut err_msg = String::from("Backblaze B2 login failure");
        if let Some(reply_err_msg) = reply_json.find("message") {
            err_msg += &(String::from(": ")+reply_err_msg.as_string().unwrap());
        }
        return Err(From::from(err_msg));
    }

    let mut b2 = B2{
        key: config.key.clone(),
        acc_id: reply_json.find("accountId").unwrap().as_string().unwrap().to_string(),
        auth_token: reply_json.find("authorizationToken").unwrap().as_string().unwrap().to_string(),
        bucket_id: String::new(),
        api_url: reply_json.find("apiUrl").unwrap().as_string().unwrap().to_string(),
        download_url: reply_json.find("downloadUrl").unwrap().as_string().unwrap().to_string(),
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