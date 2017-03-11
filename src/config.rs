use std::io::prelude::*;
use std::fs::File;
use std::env;
use std::error::Error;
use rustc_serialize::json;
use util::*;
use crypto;

static CONFIG_FILE_RELPATH: &'static str = ".config/frozen.json";
pub static UPLOAD_THREADS_DEFAULT: u16 = 6;
pub static DOWNLOAD_THREADS_DEFAULT: u16 = 8;
pub static COMPRESSION_LEVEL_DEFAULT: i32 = 18;

#[derive(Clone)]
pub struct Config {
    pub acc_id: String,
    pub app_key: String,
    pub key: crypto::Key,
    pub upload_threads: u16,
    pub download_threads: u16,
    pub compression_level: i32,
}

#[derive(RustcDecodable, RustcEncodable)]
struct ConfigFile {
    pub acc_id: String,
    pub encrypted_app_key: Vec<u8>,
    pub upload_threads: u16,
    pub download_threads: u16,
    pub compression_level: i32,
}

fn get_config_file_path() -> String {
    let home = env::var("HOME").unwrap();
    home+"/"+CONFIG_FILE_RELPATH
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
        upload_threads: config_file.upload_threads,
        download_threads: config_file.download_threads,
        compression_level: config_file.compression_level,
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
        upload_threads: UPLOAD_THREADS_DEFAULT,
        download_threads: DOWNLOAD_THREADS_DEFAULT,
        compression_level: COMPRESSION_LEVEL_DEFAULT,
    }
}

pub fn save_config(config : &Config) -> Result<(), Box<Error>> {
    let mut file = File::create(get_config_file_path())?;
    let config_file = ConfigFile{
        acc_id: config.acc_id.clone(),
        encrypted_app_key: crypto::encrypt(&Vec::from(config.app_key.as_str()), &config.key),
        upload_threads: config.upload_threads,
        download_threads: config.download_threads,
        compression_level: config.compression_level,
    };
    let encoded = json::encode(&config_file)?;
    file.set_len(0)?;
    file.write_all(encoded.as_bytes())?;
    file.flush()?;
    Ok(())
}