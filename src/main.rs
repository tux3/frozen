use crate::config::Config;
use clap::{arg, Command};
use eyre::{Result, WrapErr};
use std::ffi::OsString;
use std::process::exit;

mod action;
mod cmd;
mod config;
mod crypto;
mod data;
mod dirdb;
mod net;
mod progress;
mod prompt;
mod signal;
mod stream;

#[cfg(test)]
mod test_helpers;

#[tokio::main]
async fn async_main() -> Result<()> {
    let args = Command::new("Frozen Backup")
        .about("Encrypted and compressed backups to Backblaze B2")
        .arg(arg!(-v --verbose "Log every file transferred"))
        .subcommand_required(true)
        .subcommand(Command::new("list").about("List the currently backup up folders"))
        .subcommand(
            Command::new("backup")
                .about("Backup a folder, encrypted and compressed, to the cloud")
                .arg(arg!(-k --"keep-existing" "Keep remote files that have been deleted locally"))
                .arg(arg!(<source> "The source folder to backup").value_parser(clap::value_parser!(OsString)))
                .arg(
                    arg!([destination] "Save the back up under a different path")
                        .value_parser(clap::value_parser!(OsString)),
                ),
        )
        .subcommand(
            Command::new("restore")
                .about("Restore a backed up folder")
                .arg(arg!(<source> "The backed up folder to restore").value_parser(clap::value_parser!(OsString)))
                .arg(
                    arg!([destination] "Path to save the downloaded folder")
                        .value_parser(clap::value_parser!(OsString)),
                ),
        )
        .subcommand(
            Command::new("delete")
                .about("Delete a backed up folder")
                .arg(arg!(<target> "The backed up folder to delete").value_parser(clap::value_parser!(OsString))),
        )
        .subcommand(
            Command::new("unlock")
                .about("Force unlocking a folder after an interrupted backup. Dangerous.")
                .arg(
                    arg!(<target> "The backed up folder to forcibly unlock")
                        .value_parser(clap::value_parser!(OsString)),
                ),
        )
        .subcommand(
            Command::new("save-key")
                .about("Saves a keyfile on this computer that will be used instead of your backup password."),
        )
        .subcommand(
            Command::new("rename")
                .about("Rename a backed-up folder on the server.")
                .arg(arg!(<source> "Source path of the folder to rename").value_parser(clap::value_parser!(OsString)))
                .arg(arg!(<target> "New path of the backup").value_parser(clap::value_parser!(OsString))),
        )
        .get_matches();

    let config = Config::get_or_create(args.get_flag("verbose"));
    match args.subcommand().unwrap() {
        ("backup", sub_args) => cmd::backup(&config, sub_args).await,
        ("restore", sub_args) => cmd::restore(&config, sub_args).await,
        ("delete", sub_args) => cmd::delete(&config, sub_args).await,
        ("unlock", sub_args) => cmd::unlock(&config, sub_args).await,
        ("list", sub_args) => cmd::list(&config, sub_args).await,
        ("rename", sub_args) => cmd::rename(&config, sub_args).await,
        ("save-key", sub_args) => cmd::save_key(&config, sub_args).await,
        _ => unreachable!(),
    }
    .wrap_err_with(|| format!("\r{} failed", args.subcommand_name().unwrap()))
}

fn main() {
    sodiumoxide::init().expect("Failed to initialize the crypto library");
    let return_code = match async_main() {
        Ok(()) => 0,
        Err(err) => {
            eprintln!("{:#}", err);
            1
        }
    };
    exit(return_code);
}
