use std::error::Error;
use clap::ArgMatches;
use crate::config::Config;
use crate::data::root;
use crate::net::b2::B2;

pub async fn list<'a>(config: &'a Config, _args: &'a ArgMatches<'a>) -> Result<(), Box<dyn Error + 'static>> {
    let keys = config.get_app_keys()?;

    println!("Connecting to Backblaze B2");
    let b2 = await!(B2::authenticate(config, &keys))?;

    println!("Downloading backup metadata");
    let roots = await!(root::fetch_roots(&b2))?;

    println!("Backed-up folders:");
    for root in roots {
        println!("{}\t{}", root.path_hash, root.path);
    }

    Ok(())
}
