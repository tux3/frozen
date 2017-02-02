use std::io::prelude::*;
use std::fs::File;
use std::env;
use std::error::Error;
use rustc_serialize::json;
use util::*;
use crypto;

static CONFIG_FILE_RELPATH: &'static str = ".config/frozen.json";
pub static UPLOAD_THREADS: u8 = 8;
pub static COMPRESSION_LEVEL: i32 = 18;

pub struct Config {
    pub acc_id: String,
    pub app_key: String,
    pub key: crypto::Key,
}

#[derive(RustcDecodable, RustcEncodable)]
struct ConfigFile {
    pub acc_id: String,
    pub encrypted_app_key: Vec<u8>,
}

fn get_config_file_path() -> String {
    let home = env::var("HOME").unwrap();
    return home+"/"+CONFIG_FILE_RELPATH;
}

pub fn read_config() -> Result<Config, Box<Error>> {
    let mut file : File = File::open(get_config_file_path())?;
    let contents = &mut String::new();
    file.read_to_string(contents)?;
    let config_file: ConfigFile = json::decode(contents)?;

    let mut key: crypto::Key;
    let app_key: String;
    loop {
        let pwd = prompt_password("Enter your backup password");
        key = crypto::derive_key(&pwd, &config_file.acc_id);
        let app_key_maybe = crypto::decrypt(&config_file.encrypted_app_key, &key);
        if app_key_maybe.is_ok() {
            app_key = String::from_utf8(app_key_maybe.unwrap())?;
            break;
        }
        if !prompt_yes_no("Invalid password, try again?") {
            return Err(From::from("Couldn't decrypt config file"));
        }
    }

    Ok(Config{
        acc_id: config_file.acc_id,
        app_key: app_key,
        key: key,
    })
}

pub fn create_config_interactive() -> Config {
    let acc_id = prompt("Enter your account ID");
    let app_key = prompt("Enter you application key");
    let passwd = prompt_password("Choose a backup password");
    Config {
        key: crypto::derive_key(&passwd, &acc_id),
        acc_id: acc_id,
        app_key: app_key,
    }
}

pub fn save_config(config : &Config) -> Result<(), Box<Error>> {
    let mut file = File::create(get_config_file_path())?;
    let config_file = ConfigFile{
        acc_id: config.acc_id.clone(),
        encrypted_app_key: crypto::encrypt(&Vec::from(config.app_key.as_str()), &config.key),
    };
    let encoded = json::encode(&config_file)?;
    file.set_len(0)?;
    file.write(encoded.as_bytes())?;
    file.flush()?;
    Ok(())
}