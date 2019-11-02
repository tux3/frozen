use clap::ArgMatches;
use crate::config::Config;
use crate::data::{root, paths::path_from_arg};
use crate::net::b2::B2;
use crate::box_result::BoxResult;

pub async fn rename<'a>(config: &'a Config, args: &'a ArgMatches<'a>) -> BoxResult<()> {
    let src_path = path_from_arg(args, "source")?;
    let target_path = path_from_arg(args, "target")?;

    let keys = config.get_app_keys()?;

    println!("Connecting to Backblaze B2");
    let b2 = B2::authenticate(config, &keys).await?;

    println!("Downloading backup metadata");
    let mut roots = root::fetch_roots(&b2).await?;

    let root = match roots.iter_mut().find(|r| r.path == *src_path) {
        Some(root) => root,
        None => return Err(From::from(format!("Backup folder {} does not exist", src_path.display()))),
    };

    println!("Renaming folder {} to {}", src_path.display(), target_path.display());
    root.rename(target_path);
    root::save_roots(&b2, &roots).await
}
