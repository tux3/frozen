use std::error::Error;
use std::path::Path;
use std::fs;
use clap::ArgMatches;
use crate::config::Config;
use crate::data::root;
use crate::net::b2::B2;
use crate::termio::prompt_yes_no;

pub async fn unlock<'a>(config: &'a Config, args: &'a ArgMatches<'a>) -> Result<(), Box<dyn Error + 'static>> {
    let path = fs::canonicalize(args.value_of("target").unwrap())?.to_string_lossy().into_owned();
    if !Path::new(&path).is_dir() {
        return Err(From::from(format!("{} is not a folder!", &path)))
    }

    let keys = config.get_app_keys()?;

    println!("Connecting to Backblaze B2");
    let mut b2 = await!(B2::authenticate(config, &keys))?;

    println!("Downloading backup metadata");
    let roots = await!(root::fetch_roots(&b2))?;

    if !prompt_yes_no("Are you sure you want to wipe the lock files for this folder?\n\
                        Do not do this unless you know the lock is expired!") {
        println!("Nothing was done.");
        return Ok(());
    }

    println!("Unlocking backup folder {}", path);
    await!(root::wipe_locks(&mut b2, &roots, &path))?;

    Ok(())
}
