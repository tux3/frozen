extern crate rustc_serialize;
extern crate bincode;
extern crate rpassword;
extern crate sodiumoxide;
extern crate sha1;
#[macro_use]
extern crate hyper;

use std::env;
use std::process::exit;

mod cmd;
mod config;
mod b2api;
mod util;
mod crypto;
mod data;

fn help_and_die(selfname: &String) -> ! {
    println!("Usage: {} command path", selfname);
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
        println!("Nothing do do.");
        return;
    }
    else if args.len() < 3 {
        help_and_die(&args[0]);
    }

    match args[1].as_ref() {
        "backup" => cmd::backup(&config, &args[2]),
        "restore" => cmd::restore(&config, &args[2]),
        _ => help_and_die(&args[0]),
    }.unwrap_or_else(|err| {
        println!("{} failed: {}", args[1], err);
        exit(1);
    })
}
