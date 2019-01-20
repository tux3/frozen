#![feature(await_macro, async_await, futures_api)]

use clap::{Arg, App, SubCommand, ArgMatches};
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

fn help_and_die(args: &ArgMatches) -> ! {
    println!("{}", args.usage());
    exit(1);
}

fn main() {
    let args = App::new("Frozen Backup")
        .about("Encrypted and compressed backups to Backblaze B2")
        .subcommand(SubCommand::with_name("list")
            .about("List the currently backup up folders")
        )
        .subcommand(SubCommand::with_name("backup")
            .about("Backup a folder, encrypted and compressed, to the cloud")
            .arg(Arg::with_name("keep-existing")
                .short("k")
                .help("Keep remote files that have been deleted locally"))
            .arg(Arg::with_name("source")
                .help("The source folder to backup")
                .required(true)
                .index(1))
            .arg(Arg::with_name("destination")
                .help("Save the back up under a different path")
                .index(2))
        )
        .subcommand(SubCommand::with_name("restore")
            .about("Restore a backed up folder")
            .arg(Arg::with_name("source")
                .help("The backed up folder to restore")
                .required(true)
                .index(1))
            .arg(Arg::with_name("destination")
                .help("Path to save the downloaded folder")
                .index(2))
        )
        .subcommand(SubCommand::with_name("delete")
            .about("Delete a backed up folder")
            .arg(Arg::with_name("target")
                .help("The backed up folder to delete")
                .required(true)
                .index(1))
        )
        .subcommand(SubCommand::with_name("unlock")
            .about("Force unlocking a folder after an interrupted backup. Dangerous.")
            .arg(Arg::with_name("target")
                .help("The backed up folder to forcibly unlock")
                .required(true)
                .index(1))
        )
        .get_matches();

    let config = config::get_or_create_config();

    let mut return_code = 0;
    crate::futures_compat::tokio_run(async move {
        match args.subcommand() {
            ("backup", Some(sub_args)) => await!(cmd::backup(&config, sub_args)),
            ("restore", Some(sub_args)) => await!(cmd::restore(&config, sub_args)),
            ("delete", Some(sub_args)) => await!(cmd::delete(&config, sub_args)),
            ("unlock", Some(sub_args)) => await!(cmd::unlock(&config, sub_args)),
            ("list", Some(sub_args)) => await!(cmd::list(&config, sub_args)),
            _ => help_and_die(&args),
        }.unwrap_or_else(|err| {
            println!("\r{} failed: {}", args.subcommand_name().unwrap(), err);
            // Note that we can't exit here, we must let any pending spawned futures finish first.
            return_code = 1;
        });
    });

    exit(return_code);
}
