use crate::config::Config;
use crate::data::{paths::path_from_arg, root};
use crate::net::b2::B2;
use clap::ArgMatches;
use eyre::{bail, Result};

pub async fn rename(config: &Config, args: &ArgMatches) -> Result<()> {
    let src_path = path_from_arg(args, "source")?;
    let target_path = path_from_arg(args, "target")?;

    let keys = config.get_app_keys()?;

    println!("Connecting to Backblaze B2");
    let b2 = B2::authenticate(config, &keys).await?;

    println!("Downloading backup metadata");
    let mut roots = root::fetch_roots(&b2).await?;

    let root = match roots.iter_mut().find(|r| r.path == *src_path) {
        Some(root) => root,
        None => {
            bail!("Backup folder {} does not exist", src_path.display());
        }
    };

    println!("Renaming folder {} to {}", src_path.display(), target_path.display());
    root.rename(target_path);
    root::save_roots(&b2, &roots).await
}
