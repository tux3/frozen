use std::error::Error;
use crate::config::Config;
use crate::data::root;
use crate::net::b2::B2;

pub async fn list(config: &Config) -> Result<(), Box<dyn Error + 'static>> {
    println!("Connecting to Backblaze B2");
    let b2 = await!(B2::authenticate(config))?;

    println!("Downloading backup metadata");
    let roots = await!(root::fetch_roots(&b2))?;

    println!("Backed-up folders:");
    for root in roots {
        println!("{}\t{}", root.path_hash, root.path);
    }

    Ok(())
}
