use std::error::Error;
use std::io::Read;
use std::vec::Vec;
use config::Config;
use hyper::client::Client;
use hyper::client::response::Response;
use hyper::header::{Authorization, Basic};
use rustc_serialize::json::Json;

pub struct B2 {
    acc_id: String,
    auth_token: String,
    api_url: String,
    download_url: String,
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

    Ok(B2{
        acc_id: reply_json.find("accountId").unwrap().as_string().unwrap().to_string(),
        auth_token: reply_json.find("authorizationToken").unwrap().as_string().unwrap().to_string(),
        api_url: reply_json.find("apiUrl").unwrap().as_string().unwrap().to_string(),
        download_url: reply_json.find("downloadUrl").unwrap().as_string().unwrap().to_string(),
    })
}