#[macro_use]
extern crate hyper;
extern crate hyper_openssl;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate serde;
extern crate data_encoding;
extern crate bincode;
extern crate rpassword;
extern crate libsodium_sys;
extern crate sodiumoxide;
extern crate blake2;
extern crate sha_1;
extern crate digest;
extern crate zstd;
extern crate pretty_bytes;
extern crate ctrlc;

use std::env;
use std::process::exit;

mod cmd;
mod config;
mod net;
mod util;
mod crypto;
mod data;
mod progress;
mod vt100;

fn help_and_die(selfname: &str) -> ! {
    println!("Usage: {} command [arguments]", selfname);
    exit(1);
}

fn main() {
    let config = config::read_config().unwrap_or_else(|_| {
        println!("No configuration found, creating it.");
        let config = config::create_config_interactive();
        config::save_config(&config).expect("Failed to save configuration!");
        config
    });

    let args: Vec<_> = env::args().collect();
    if args.len() <= 1 {
        help_and_die(&args[0]);
    }

    let target_path = if args.len() >= 4 {
        Some(args[3].as_str())
    } else {
        None
    };

    match args[1].as_ref() {
        "backup" => cmd::backup(&config, &args[2]),
        "restore" => cmd::restore(&config, &args[2], target_path),
        "delete" => cmd::delete(&config, &args[2]),
        "unlock" => cmd::unlock(&config, &args[2]),
        "list" => cmd::list(&config),
        _ => help_and_die(&args[0]),
    }.unwrap_or_else(|err| {
        println!("{} failed: {}", args[1], err);
        exit(1);
    })
}
