use std::error::Error;
use std::fs;
use std::time::Duration;
use clap::ArgMatches;
use futures_timer::Delay;
use ignore_result::Ignore;
use crate::config::Config;
use crate::data::{root, paths::path_from_arg};
use crate::net::b2::B2;
use crate::termio::progress;
use crate::signal::*;

pub async fn restore<'a>(config: &'a Config, args: &'a ArgMatches<'a>) -> Result<(), Box<dyn Error + 'static>> {
    let path = path_from_arg(args, "source")?;
    let target = path_from_arg(args, "destination").unwrap_or_else(|_| path.clone());
    fs::create_dir_all(&target)?;

    let keys = config.get_app_keys()?;

    println!("Connecting to Backblaze B2");
    let b2 = await!(B2::authenticate(config, &keys))?;

    println!("Downloading backup metadata");
    let mut roots = await!(root::fetch_roots(&b2))?;

    println!("Opening backup folder {}", path.display());
    let root = await!(root::open_root(&b2, &mut roots, &path))?;

    println!("Starting to list local files");
    let (lfiles_rx, list_thread) = root.list_local_files_async(&b2, &target)?;
    err_on_signal()?;

    println!("Listing remote files");
    let mut rfiles = await!(root.list_remote_files(&b2))?;
    err_on_signal()?;

    println!("Starting download");
    let mut download_threads = root.start_download_threads(&b2, config, &target);

    progress::start_output(download_threads.len());

    for file in lfiles_rx {
        let rfile = rfiles.binary_search_by(|v| v.cmp_local(&file));
        if rfile.is_ok() && rfiles[rfile.unwrap()].last_modified <= file.last_modified {
            rfiles.remove(rfile.unwrap());
        }
        err_on_signal()?;
    }

    for rfile in rfiles {
        'send: loop {
            for thread in &mut download_threads {
                if thread.tx.try_send(Some(rfile.clone())).is_ok() {
                    break 'send;
                }
            }
            err_on_signal()?;
            await!(progress::handle_progress(config.verbose, &mut download_threads));
            await!(Delay::new(Duration::from_millis(20))).ignore();
        }
        err_on_signal()?;
        await!(progress::handle_progress(config.verbose, &mut download_threads));
    }

    // Tell our threads to stop as they become idle
    let mut thread_id = download_threads.len() - 1;
    loop {
        err_on_signal()?;
        if thread_id < download_threads.len() {
            let result = &download_threads[thread_id].tx.try_send(None);
            if result.is_err() {
                await!(progress::handle_progress(config.verbose, &mut download_threads));
                await!(Delay::new(Duration::from_millis(20))).ignore();
                continue;
            }
        }

        if thread_id == 0 {
            break;
        } else {
            thread_id -= 1;
        }
    }

    while !download_threads.is_empty() {
        err_on_signal()?;
        await!(progress::handle_progress(config.verbose, &mut download_threads));
        await!(Delay::new(Duration::from_millis(20))).ignore();
    }
    list_thread.join().unwrap();

    Ok(())
}
