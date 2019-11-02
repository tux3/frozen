use crate::config::Config;
use crate::box_result::BoxResult;
use clap::{Arg, App, SubCommand};
use std::process::exit;

mod box_result;
mod cmd;
mod config;
mod net;
mod progress;
mod crypto;
mod data;
mod dirdb;
mod action;
mod prompt;
mod signal;

#[tokio::main]
async fn async_main() -> BoxResult<()> {
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
    match args.subcommand() {
        ("backup", Some(sub_args)) => cmd::backup(&config, sub_args).await,
        ("restore", Some(sub_args)) => cmd::restore(&config, sub_args).await,
        ("delete", Some(sub_args)) => cmd::delete(&config, sub_args).await,
        ("unlock", Some(sub_args)) => cmd::unlock(&config, sub_args).await,
        ("list", Some(sub_args)) => cmd::list(&config, sub_args).await,
        ("rename", Some(sub_args)) => cmd::rename(&config, sub_args).await,
        _ => unreachable!(),
    }.map_err(|err| {
        From::from(format!("\r{} failed: {}", args.subcommand_name().unwrap(), err))
    })
}

fn main() {
    let return_code = match async_main() {
        Ok(()) => 0,
        Err(err) => {
            eprintln!("{}", err);
            1
        }
    };
    exit(return_code);
}