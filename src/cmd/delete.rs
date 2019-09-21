use std::error::Error;
use std::time::Duration;
use clap::ArgMatches;
use futures_timer::Delay;
use ignore_result::Ignore;
use crate::config::Config;
use crate::data::{root, paths::path_from_arg};
use crate::net::b2::B2;
use crate::termio::progress;
use crate::signal::*;

pub async fn delete<'a>(config: &'a Config, args: &'a ArgMatches<'a>) -> Result<(), Box<dyn Error + 'static>> {
    let path = path_from_arg(args, "target")?;
    let keys = config.get_app_keys()?;

    println!("Connecting to Backblaze B2");
    let mut b2 = B2::authenticate(config, &keys).await?;

    println!("Downloading backup metadata");
    let mut roots = root::fetch_roots(&b2).await?;

    println!("Deleting backup folder {}", path.display());

    let root = root::open_root(&b2, &mut roots, &path).await?;
    delete_files(config, &mut b2, &root).await?;

    root::delete_root(&mut b2, &mut roots, &path).await
}

async fn delete_files<'a>(config: &'a Config, b2: &'a mut B2,
                root: &'a root::BackupRoot)
        -> Result<(), Box<dyn Error + 'static>> {
    err_on_signal()?;

    println!("Listing remote files");
    let rfiles = root.list_remote_files(b2).await?;

    // Delete all remote files
    let mut delete_threads = root.start_delete_threads(b2, config);
    progress::start_output(config.verbose, delete_threads.len());

    for rfile in rfiles {
        'delete_send: loop {
            for thread in &mut delete_threads {
                if thread.tx.try_send(Some(rfile.clone())).is_ok() {
                    break 'delete_send;
                }
            }
            err_on_signal()?;
            progress::handle_progress(config.verbose, &mut delete_threads).await;
            Delay::new(Duration::from_millis(20)).await.ignore();
        }
        err_on_signal()?;
        progress::handle_progress(config.verbose, &mut delete_threads).await;
    }

    // Tell our delete threads to stop as they become idle
    let mut thread_id = delete_threads.len() - 1;
    loop {
        err_on_signal()?;
        if thread_id < delete_threads.len() {
            let result = &delete_threads[thread_id].tx.try_send(None);
            if result.is_err() {
                progress::handle_progress(config.verbose, &mut delete_threads).await;
                Delay::new(Duration::from_millis(20)).await.ignore();
                continue;
            }
        }

        if thread_id == 0 {
            break;
        } else {
            thread_id -= 1;
        }
    }

    while !delete_threads.is_empty() {
        err_on_signal()?;
        progress::handle_progress(config.verbose, &mut delete_threads).await;
        Delay::new(Duration::from_millis(20)).await.ignore();
    }

    Ok(())
}