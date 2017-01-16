use std::error::Error;
use std::io::Read;
use std::vec::Vec;
use std::fmt;
use crypto;
use config::Config;
use hyper::client::Client;
use hyper::client::response::Response;
use hyper::header::{Authorization, Basic, Headers, ContentType, ContentLength};
use hyper::mime::{Mime, TopLevel, SubLevel};
use rustc_serialize::json::Json;

header!{(XBzFileName, "X-Bz-File-Name") => [String]}
header!{(XBzContentSha1, "X-Bz-Content-Sha1") => [String]}

struct B2Upload {
    pub url: String,
    pub auth_token: String,
}

pub struct B2 {
    pub key: crypto::Key,
    pub bucket_id: String,
    pub acc_id: String,
    pub auth_token: String,
    pub api_url: String,
    pub download_url: String,
    upload: Option<B2Upload>,
}

fn get_frozen_bucket_id(b2: &B2) -> Result<String, Box<Error>> {
    let client = Client::new();
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

fn get_upload_url(b2: &mut B2) -> Result<B2Upload, Box<Error>> {
    let client = Client::new();
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

pub fn upload_file(b2: &mut B2, filename: &str, data: &[u8]) -> Result<(), Box<Error>> {
    if b2.upload.is_none() {
        b2.upload = Some(get_upload_url(b2)?);
    }

    println!("About to upload {}, {} bytes", filename, data.len());

    let b2upload = &b2.upload.as_mut().unwrap();
    let client = Client::new();
    let basic_auth = Authorization(b2upload.auth_token.clone());
    let mut reply: Response = client.post(&b2upload.url)
        .header(basic_auth)
        .header(XBzFileName(filename.to_string()))
        .header(ContentType("application/octet-stream".parse().unwrap()))
        .header(ContentLength(data.len() as u64))
        .header(XBzContentSha1(crypto::sha1_string(data)))
        .body(data)
        .send()?;

    let reply_data = &mut String::new();
    reply.read_to_string(reply_data)?;
    let reply_json: Json = Json::from_str(reply_data)?;

    if !reply.status.is_success() {
        return Err(From::from(format!("upload_file failed with error {}: {}",
                                      reply.status.to_u16(),
                                      reply_json.find("message").unwrap())));
    }
    Ok(())
}

pub fn download_file(b2: &B2, filename: &str) -> Result<Vec<u8>, Box<Error>> {
    let client = Client::new();
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
    return Ok(reply_data);
}

pub fn authenticate(config: &Config) -> Result<B2, Box<Error>> {
    let client = Client::new();
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