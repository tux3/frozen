use std::io::prelude::*;
use std::fs::File;
use std::env;
use std::error::Error;
use serde::{Serialize, Deserialize};
use serde_json;
use crate::util::*;
use crate::crypto;

static CONFIG_FILE_RELPATH: &'static str = ".config/frozen.json";
pub static UPLOAD_THREADS_DEFAULT: u16 = 6;
pub static DOWNLOAD_THREADS_DEFAULT: u16 = 8;
pub static DELETE_THREADS_DEFAULT: u16 = 16;
pub static COMPRESSION_LEVEL_DEFAULT: i32 = 18;

#[derive(Clone)]
pub struct Config {
    pub acc_id: String,
    pub app_key_id: String,
    pub app_key: String,
    pub key: crypto::Key,
    pub bucket_name: String,
    pub upload_threads: u16,
    pub download_threads: u16,
    pub delete_threads: u16,
    pub compression_level: i32,
    pub verbose: bool,
}

#[derive(Serialize, Deserialize)]
struct ConfigFile {
    pub acc_id: String,
    pub encrypted_app_key_id: Vec<u8>,
    pub encrypted_app_key: Vec<u8>,
    pub bucket_name: String,
    pub upload_threads: u16,
    pub download_threads: u16,
    pub delete_threads: u16,
    pub compression_level: i32,
}

pub fn get_or_create_config(verbose: bool) -> Config {
    let mut config = read_config().unwrap_or_else(|_| {
        println!("No configuration found, creating it.");
        let config = create_config_interactive();
        save_config(&config).expect("Failed to save configuration!");
        config
    });
    config.verbose = verbose;
    config
}

fn get_config_file_path() -> String {
    let home = env::var("HOME").unwrap();
    home+"/"+CONFIG_FILE_RELPATH
}

fn read_config() -> Result<Config, Box<dyn Error>> {
    let mut file : File = File::open(get_config_file_path())?;
    let contents = &mut String::new();
    file.read_to_string(contents)?;
    let config_file: ConfigFile = serde_json::from_str(&contents)?;

    let mut key: crypto::Key;
    let app_key: String;
    let app_key_id: String;
    loop {
        let pwd = prompt_password("Enter your backup password");
        key = crypto::derive_key(&pwd, &config_file.acc_id);
        if let (Ok(ok_app_key), Ok(ok_app_key_id)) = (crypto::decrypt(&config_file.encrypted_app_key, &key),
                                                      crypto::decrypt(&config_file.encrypted_app_key_id, &key)) {
            app_key = String::from_utf8(ok_app_key)?;
            app_key_id = String::from_utf8(ok_app_key_id)?;
            break;
        }
        if !prompt_yes_no("Invalid password, try again?") {
            return Err(From::from("Couldn't decrypt config file"));
        }
    }

    Ok(Config{
        acc_id: config_file.acc_id,
        app_key_id,
        app_key,
        key,
        bucket_name: config_file.bucket_name,
        upload_threads: config_file.upload_threads,
        download_threads: config_file.download_threads,
        delete_threads: config_file.delete_threads,
        compression_level: config_file.compression_level,
        verbose: false,
    })
}

fn create_config_interactive() -> Config {
    let acc_id = prompt("Enter your account ID");
    let app_key_id = prompt("Enter you application key ID");
    let app_key = prompt("Enter you application key");
    let bucket_name = prompt("Enter your backup bucket name");
    let passwd = prompt_password("Choose a backup password");
    Config {
        key: crypto::derive_key(&passwd, &acc_id),
        acc_id,
        app_key_id,
        app_key,
        bucket_name,
        upload_threads: UPLOAD_THREADS_DEFAULT,
        download_threads: DOWNLOAD_THREADS_DEFAULT,
        delete_threads: DELETE_THREADS_DEFAULT,
        compression_level: COMPRESSION_LEVEL_DEFAULT,
        verbose: false,
    }
}

fn save_config(config : &Config) -> Result<(), Box<Error>> {
    let mut file = File::create(get_config_file_path())?;
    let config_file = ConfigFile{
        acc_id: config.acc_id.clone(),
        encrypted_app_key_id: crypto::encrypt(&Vec::from(config.app_key_id.as_str()), &config.key),
        encrypted_app_key: crypto::encrypt(&Vec::from(config.app_key.as_str()), &config.key),
        bucket_name: config.bucket_name.clone(),
        upload_threads: config.upload_threads,
        download_threads: config.download_threads,
        delete_threads: config.delete_threads,
        compression_level: config.compression_level,
    };
    let encoded = serde_json::to_string(&config_file)?;
    file.set_len(0)?;
    file.write_all(encoded.as_bytes())?;
    file.flush()?;
    Ok(())
}