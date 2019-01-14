#![feature(await_macro, async_await, futures_api)]

use std::env;
use std::process::exit;
use tokio::await;

mod cmd;
mod config;
mod net;
mod util;
mod crypto;
mod data;
mod progress;
mod vt100;
mod futures_compat;

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

    let mut return_code = 0;
    crate::futures_compat::tokio_run(async move {
        let target_path = if args.len() > 3 {
            Some(args[3].as_str())
        } else {
            None
        };

        match args[1].as_ref() {
        "backup" => await!(cmd::backup(&config, &args[2])),
        "restore" => await!(cmd::restore(&config, &args[2], target_path)),
        "delete" => await!(cmd::delete(&config, &args[2])),
        "unlock" => await!(cmd::unlock(&config, &args[2])),
        "list" => await!(cmd::list(&config)),
        _ => help_and_die(&args[0]),
        }.unwrap_or_else(|err| {
            println!("\r{} failed: {}", args[1], err);
            return_code = 1;
        });
    });

    exit(return_code);
}
