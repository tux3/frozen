use std::io::prelude::*;
use std::fs::File;
use std::env;
use std::error::Error;
use serde::{Serialize, Deserialize};
use serde_json;
use crate::termio::{prompt, prompt_password, prompt_yes_no};
use crate::crypto::{AppKeys, derive_key, decrypt, encrypt};

static CONFIG_FILE_RELPATH: &'static str = ".config/frozen.json";
pub static UPLOAD_THREADS_DEFAULT: u16 = 6;
pub static DOWNLOAD_THREADS_DEFAULT: u16 = 8;
pub static DELETE_THREADS_DEFAULT: u16 = 16;
pub static COMPRESSION_LEVEL_DEFAULT: i32 = 18;

#[derive(Clone)]
pub struct Config {
    encrypted_app_key_id: Vec<u8>,
    encrypted_app_key: Vec<u8>,
    pub acc_id: String,
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

impl Config {
    pub fn get_or_create(verbose: bool) -> Self {
        let mut config = Self::new_from_file().unwrap_or_else(|_| {
            println!("No configuration found, creating it.");
            let config = Self::new_interactive();
            config.save().expect("Failed to save configuration!");
            config
        });
        config.verbose = verbose;
        config
    }

    pub fn get_app_keys(&self) -> Result<AppKeys, Box<dyn Error>> {
        let keys = loop {
            let pwd = prompt_password("Enter your backup password");
            let key = derive_key(&pwd, &self.acc_id);
            if let (Ok(app_key), Ok(app_key_id)) = (decrypt(&self.encrypted_app_key, &key),
                                                    decrypt(&self.encrypted_app_key_id, &key)) {
                break AppKeys {
                    b2_key_id: String::from_utf8(app_key_id)?,
                    b2_key: String::from_utf8(app_key)?,
                    encryption_key: key,
                };
            }
            if !prompt_yes_no("Invalid password, try again?") {
                return Err(From::from("Couldn't decrypt config file"));
            }
        };

        Ok(keys)
    }

    fn new_interactive() -> Config {
        let acc_id = prompt("Enter your account ID");
        let b2_key_id = prompt("Enter you application key ID");
        let b2_key = prompt("Enter you application key");
        let bucket_name = prompt("Enter your backup bucket name");
        let passwd = prompt_password("Choose a backup password");

        let encryption_key = derive_key(&passwd, &acc_id);
        Config {
            encrypted_app_key_id: encrypt(&Vec::from(b2_key_id.as_str()), &encryption_key),
            encrypted_app_key: encrypt(&Vec::from(b2_key.as_str()), &encryption_key),
            acc_id,
            bucket_name,
            upload_threads: UPLOAD_THREADS_DEFAULT,
            download_threads: DOWNLOAD_THREADS_DEFAULT,
            delete_threads: DELETE_THREADS_DEFAULT,
            compression_level: COMPRESSION_LEVEL_DEFAULT,
            verbose: false,
        }
    }

    fn new_from_file() -> Result<Self, Box<dyn Error>> {
        let mut file : File = File::open(Self::get_file_path())?;
        let contents = &mut String::new();
        file.read_to_string(contents)?;
        let config_file: ConfigFile = serde_json::from_str(&contents)?;

        Ok(Config{
            encrypted_app_key_id: config_file.encrypted_app_key_id,
            encrypted_app_key: config_file.encrypted_app_key,
            acc_id: config_file.acc_id,
            bucket_name: config_file.bucket_name,
            upload_threads: config_file.upload_threads,
            download_threads: config_file.download_threads,
            delete_threads: config_file.delete_threads,
            compression_level: config_file.compression_level,
            verbose: false,
        })
    }

    fn save(&self) -> Result<(), Box<dyn Error>> {
        let mut file = File::create(Self::get_file_path())?;
        let config_file = ConfigFile{
            acc_id: self.acc_id.clone(),
            encrypted_app_key_id: self.encrypted_app_key_id.clone(),
            encrypted_app_key: self.encrypted_app_key.clone(),
            bucket_name: self.bucket_name.clone(),
            upload_threads: self.upload_threads,
            download_threads: self.download_threads,
            delete_threads: self.delete_threads,
            compression_level: self.compression_level,
        };
        let encoded = serde_json::to_string(&config_file)?;
        file.set_len(0)?;
        file.write_all(encoded.as_bytes())?;
        file.flush()?;
        Ok(())
    }

    fn get_file_path() -> String {
        let home = env::var("HOME").unwrap();
        home+"/"+CONFIG_FILE_RELPATH
    }
}