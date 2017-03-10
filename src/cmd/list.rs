use std::error::Error;
use config::Config;
use data::root;
use net::{b2api};

pub fn list(config: &Config) -> Result<(), Box<Error>> {
    println!("Connecting to Backblaze B2");
    let b2 = &b2api::authenticate(config)?;

    println!("Downloading backup metadata");
    let roots = root::fetch_roots(b2);

    println!("Backed-up folders:");
    for root in roots {
        println!("{}", root.path);
    }

    Ok(())
}
