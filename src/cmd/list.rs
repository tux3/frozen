use crate::config::Config;
use crate::data::root;
use crate::net::b2::B2;
use clap::ArgMatches;
use eyre::Result;

pub async fn list(config: &Config, _args: &ArgMatches) -> Result<()> {
    let keys = config.get_app_keys()?;

    println!("Connecting to Backblaze B2");
    let b2 = B2::authenticate(config, &keys).await?;

    println!("Downloading backup metadata");
    let mut roots = root::fetch_roots(&b2).await?;
    roots.sort_by(|a, b| a.path.cmp(&b.path));

    println!("Backed-up folders:");
    for root in roots {
        println!("{}\t{}", root.path_hash, root.path.display());
    }

    Ok(())
}
