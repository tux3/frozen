use crate::crypto::{decrypt, derive_key, encrypt, AppKeys, Key};
use crate::prompt::{prompt, prompt_password, prompt_yes_no};
use eyre::{bail, Result};
use serde::{Deserialize, Serialize};
use std::env;
use std::error::Error;
use std::ffi::OsString;
use std::fs::File;
use std::io::prelude::*;
use std::path::PathBuf;

static CONFIG_FILE_RELPATH: &str = ".config/frozen.json";
static KEY_FILE_RELPATH: &str = ".config/frozen.key";
pub static UPLOAD_THREADS_DEFAULT: u16 = 16;
pub static DOWNLOAD_THREADS_DEFAULT: u16 = 8;
pub static DELETE_THREADS_DEFAULT: u16 = 32;
pub static COMPRESSION_LEVEL_DEFAULT: i32 = 18;

#[derive(Clone)]
pub struct Config {
    encrypted_app_key: Vec<u8>,
    app_key_id: String,
    pub bucket_name: String,
    pub upload_threads: u16,
    pub download_threads: u16,
    pub delete_threads: u16,
    pub compression_level: i32,
    pub verbose: bool,
}

#[derive(Serialize, Deserialize)]
struct ConfigFile {
    pub encrypted_app_key: Vec<u8>,
    pub app_key_id: String,
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

    fn try_derive_app_keys(&self, key: &Key) -> Option<AppKeys> {
        if let Ok(app_key) = decrypt(&self.encrypted_app_key, key) {
            Some(AppKeys {
                b2_key_id: self.app_key_id.clone(),
                b2_key: String::from_utf8(app_key).unwrap(),
                encryption_key: key.to_owned(),
            })
        } else {
            None
        }
    }

    pub fn get_app_keys(&self) -> Result<AppKeys> {
        if let Ok(key) = std::fs::read(Self::get_keyfile_path()) {
            let key = Key::from_slice(key.as_slice()).expect("Invalid keyfile");
            if let Some(app_key) = self.try_derive_app_keys(&key) {
                return Ok(app_key);
            } else {
                eprintln!("Found a keyfile, but failed to decrypt app keys. You may be using the wrong keyfile.");
            }
        }

        loop {
            let pwd = prompt_password("Enter your backup password");
            let key = derive_key(&pwd, &self.bucket_name);
            if let Some(app_key) = self.try_derive_app_keys(&key) {
                return Ok(app_key);
            }
            if !prompt_yes_no("Invalid password, try again?") {
                bail!("Couldn't decrypt config file");
            }
        }
    }

    pub fn has_keyfile() -> bool {
        Self::get_keyfile_path().exists()
    }

    pub fn save_encryption_key(app_keys: &AppKeys) -> Result<()> {
        let key = app_keys.encryption_key.as_ref();
        let mut file = File::create(Self::get_keyfile_path())?;
        file.write_all(key)?;
        Ok(())
    }

    fn new_interactive() -> Config {
        let b2_key_id = prompt("Enter you app key ID (or account ID)");
        let b2_key = prompt("Enter you app key");
        let bucket_name = prompt("Enter your backup bucket name");
        let passwd = prompt_password("Choose a backup password");

        let encryption_key = derive_key(&passwd, &bucket_name);
        Config {
            encrypted_app_key: encrypt(&Vec::from(b2_key.as_str()), &encryption_key),
            app_key_id: b2_key_id,
            bucket_name,
            upload_threads: UPLOAD_THREADS_DEFAULT,
            download_threads: DOWNLOAD_THREADS_DEFAULT,
            delete_threads: DELETE_THREADS_DEFAULT,
            compression_level: COMPRESSION_LEVEL_DEFAULT,
            verbose: false,
        }
    }

    fn new_from_file() -> Result<Self, Box<dyn Error>> {
        let contents = std::fs::read_to_string(Self::get_file_path())?;
        let config_file: ConfigFile = serde_json::from_str(&contents)?;

        Ok(Config {
            encrypted_app_key: config_file.encrypted_app_key,
            app_key_id: config_file.app_key_id,
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
        let config_file = ConfigFile {
            encrypted_app_key: self.encrypted_app_key.clone(),
            app_key_id: self.app_key_id.clone(),
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

    fn get_file_path() -> PathBuf {
        let home = env::var_os("HOME").unwrap();
        [home, OsString::from(CONFIG_FILE_RELPATH)].iter().collect()
    }

    fn get_keyfile_path() -> PathBuf {
        let home = env::var_os("HOME").unwrap();
        [home, OsString::from(KEY_FILE_RELPATH)].iter().collect()
    }
}
