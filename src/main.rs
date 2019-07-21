#![feature(await_macro, async_await)]

use crate::config::Config;
use clap::{Arg, App, SubCommand, ArgMatches};
use std::process::exit;

mod cmd;
mod config;
mod net;
mod termio;
mod signal;
mod crypto;
mod data;
mod dirdb;

fn help_and_die(args: &ArgMatches) -> ! {
    println!("{}", args.usage());
    exit(1);
}

#[tokio::main]
async fn main() {
    signal::setup_signal_handler();

    let args = App::new("Frozen Backup")
        .about("Encrypted and compressed backups to Backblaze B2")
        .arg(Arg::with_name("verbose")
            .short("v")
            .long("verbose")
            .help("Log every file transferred"))
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
        .subcommand(SubCommand::with_name("rename")
            .about("Rename a backed-up folder on the server.")
            .arg(Arg::with_name("source")
                .help("Source path of the folder to rename")
                .required(true)
                .index(1))
            .arg(Arg::with_name("target")
                .help("New path of the backup")
                .required(true)
                .index(2))
        )
        .get_matches();

    let config = Config::get_or_create(args.is_present("verbose"));

    let mut return_code = 0;
    match args.subcommand() {
        ("backup", Some(sub_args)) => cmd::backup(&config, sub_args).await,
        ("restore", Some(sub_args)) => cmd::restore(&config, sub_args).await,
        ("delete", Some(sub_args)) => cmd::delete(&config, sub_args).await,
        ("unlock", Some(sub_args)) => cmd::unlock(&config, sub_args).await,
        ("list", Some(sub_args)) => cmd::list(&config, sub_args).await,
        ("rename", Some(sub_args)) => cmd::rename(&config, sub_args).await,
        _ => help_and_die(&args),
    }.unwrap_or_else(|err| {
        println!("\r{} failed: {}", args.subcommand_name().unwrap(), err);
        // Note that we can't exit here, we must let any pending spawned futures finish first.
        return_code = 1;
    });

    // TODO: Find a way to wait for any spawned futures before we exit. Push them in a global Q? Give them to a thread we can join? Use a tokio wait function?

    exit(return_code);
}
