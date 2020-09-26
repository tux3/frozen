use crate::config::Config;
use clap::ArgMatches;
use eyre::{ensure, Result};

pub async fn save_key<'a>(config: &'a Config, _args: &'a ArgMatches<'a>) -> Result<()> {
    ensure!(
        !Config::has_keyfile(),
        "A keyfile already exists! If you want to regenerate the keyfile, please delete it first.",
    );

    let keys = config.get_app_keys()?;

    println!("Saving keyfile");
    Config::save_encryption_key(&keys)?;

    Ok(())
}
