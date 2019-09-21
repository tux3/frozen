use std::error::Error;
use clap::ArgMatches;
use crate::config::Config;
use crate::data::{root, paths::path_from_arg};
use crate::net::b2::B2;
use crate::termio::prompt_yes_no;

pub async fn unlock<'a>(config: &'a Config, args: &'a ArgMatches<'a>) -> Result<(), Box<dyn Error + 'static>> {
    let path = path_from_arg(args, "target")?;
    let keys = config.get_app_keys()?;

    println!("Connecting to Backblaze B2");
    let mut b2 = B2::authenticate(config, &keys).await?;

    println!("Downloading backup metadata");
    let roots = root::fetch_roots(&b2).await?;

    println!("Unlocking backup folder {}", path.display());
    root::wipe_locks(&mut b2, &roots, &path).await?;

    Ok(())
}
