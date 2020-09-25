use crate::box_result::BoxResult;
use crate::config::Config;
use clap::ArgMatches;

pub async fn save_key<'a>(config: &'a Config, _args: &'a ArgMatches<'a>) -> BoxResult<()> {
    if Config::has_keyfile() {
        return Err(From::from(
            "A keyfile already exists! If you want to regenerate the keyfile, please delete it first.",
        ));
    }

    let keys = config.get_app_keys()?;

    println!("Saving keyfile");
    Config::save_encryption_key(&keys)?;

    Ok(())
}
