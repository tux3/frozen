use std::error::Error;
use std::path::Path;
use std::fs;
use config::Config;
use data::root;
use net::b2api;
use util::prompt_yes_no;

pub fn unlock(config: &Config, path: &str) -> Result<(), Box<Error>> {
    let path = fs::canonicalize(path)?.to_string_lossy().into_owned();
    if !Path::new(&path).is_dir() {
        return Err(From::from(format!("{} is not a folder!", &path)))
    }

    println!("Connecting to Backblaze B2");
    let b2 = &mut b2api::authenticate(config)?;

    println!("Downloading backup metadata");
    let roots = root::fetch_roots(b2);

    if !prompt_yes_no("Are you sure you want to wipe the lock files for this folder?\n\
                        Do not do this unless you know the lock is expired!") {
        println!("Nothing was done.");
        return Ok(());
    }

    println!("Unlocking backup folder {}", path);
    root::wipe_locks(b2, &roots, &path)?;

    Ok(())
}
