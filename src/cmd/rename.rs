use std::error::Error;
use clap::ArgMatches;
use crate::config::Config;
use crate::data::root;
use crate::net::b2::B2;

pub async fn rename<'a>(config: &'a Config, args: &'a ArgMatches<'a>) -> Result<(), Box<dyn Error + 'static>> {
    let src_path = args.value_of("source").unwrap().to_owned();
    let target_path = args.value_of("target").unwrap();

    let keys = config.get_app_keys()?;

    println!("Connecting to Backblaze B2");
    let mut b2 = await!(B2::authenticate(config, &keys))?;

    println!("Downloading backup metadata");
    let mut roots = await!(root::fetch_roots(&b2))?;

    let root = match roots.iter_mut().find(|r| r.path == *src_path) {
        Some(root) => root,
        None => return Err(From::from(format!("Backup folder {} does not exist", src_path))),
    };

    println!("Renaming folder {} to {}", src_path, target_path);
    root.rename(target_path);
    await!(root::save_roots(&mut b2, &roots))
}
