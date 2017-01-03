use std::error::Error;
use std::process::exit;
use config::Config;

#[allow(unused_variables)]
pub fn restore(config: &Config, path: &String) -> Result<(), Box<Error>> {
    println!("Restore not implemented yet!");
    exit(1);
}
