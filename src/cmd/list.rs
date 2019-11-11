use crate::box_result::BoxResult;
use crate::config::Config;
use crate::data::root;
use crate::net::b2::B2;
use clap::ArgMatches;

pub async fn list<'a>(config: &'a Config, _args: &'a ArgMatches<'a>) -> BoxResult<()> {
    let keys = config.get_app_keys()?;

    println!("Connecting to Backblaze B2");
    let b2 = B2::authenticate(config, &keys).await?;

    println!("Downloading backup metadata");
    let roots = root::fetch_roots(&b2).await?;

    println!("Backed-up folders:");
    for root in roots {
        println!("{}\t{}", root.path_hash, root.path.display());
    }

    Ok(())
}
