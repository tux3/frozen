use std::error::Error;
use std::time::Duration;
use std::sync::{Arc, atomic::AtomicBool};
use clap::ArgMatches;
use futures_timer::Delay;
use tokio::await;
use crate::config::Config;
use crate::data::root::{self, BackupRoot};
use crate::data::file::RemoteFile;
use crate::data::paths::path_from_arg;
use crate::net::b2;
use crate::termio::progress;
use crate::signal::*;

pub async fn backup<'a>(config: &'a Config, args: &'a ArgMatches<'a>) -> Result<(), Box<dyn Error + 'static>> {
    let path = path_from_arg(args, "source")?;
    if !path.is_dir() {
        return Err(From::from(format!("{} is not a folder!", &path.display())))
    }
    let target = path_from_arg(args, "destination").unwrap_or_else(|_| path.clone());

    let keys = config.get_app_keys()?;

    println!("Connecting to Backblaze B2");
    let b2 = &mut await!(b2::B2::authenticate(config, &keys))?;

    println!("Downloading backup metadata");
    let mut roots = await!(root::fetch_roots(b2))?;

    let signal_flag = setup_signal_flag();

    println!("Opening backup folder {}", target.display());
    let root = await!(root::open_create_root(b2, &mut roots, &target))?;

    println!("Starting to list local files");
    let (lfiles_rx, list_thread) = root.list_local_files_async(b2, &path)?;
    err_on_signal(&signal_flag)?;

    println!("Listing remote files");
    let mut rfiles = await!(root.list_remote_files(b2))?;
    err_on_signal(&signal_flag)?;

    println!("Starting upload");
    let mut upload_threads = root.start_upload_threads(b2, config, &path);

    progress::start_output(upload_threads.len());

    for file in lfiles_rx {
        let rfile = rfiles.binary_search_by(|v| v.cmp_local(&file));
        if rfile.is_err() || rfiles[rfile.unwrap()].last_modified < file.last_modified {
            'upload_send: loop {
                for thread in &mut upload_threads {
                    if thread.tx.try_send(Some(file.clone())).is_ok() {
                        break 'upload_send;
                    }
                }
                await!(progress::handle_progress(config.verbose, &mut upload_threads));
                err_on_signal(&signal_flag)?;
                await!(Delay::new(Duration::from_millis(20))).is_ok();
            }
            err_on_signal(&signal_flag)?;
            await!(progress::handle_progress(config.verbose, &mut upload_threads));
        }
        if let Ok(rfile) = rfile {
            rfiles.remove(rfile);
        }
    }

    // Tell our threads to stop as they become idle
    let mut thread_id = upload_threads.len() - 1;
    loop {
        err_on_signal(&signal_flag)?;
        if thread_id < upload_threads.len() {
            let result = &upload_threads[thread_id].tx.try_send(None);
            if result.is_err() {
                await!(progress::handle_progress(config.verbose, &mut upload_threads));
                await!(Delay::new(Duration::from_millis(20))).is_ok();
                continue;
            }
        }

        if thread_id == 0 {
            break;
        } else {
            thread_id -= 1;
        }
    }

    while !upload_threads.is_empty() {
        err_on_signal(&signal_flag)?;
        await!(progress::handle_progress(config.verbose, &mut upload_threads));
        await!(Delay::new(Duration::from_millis(20))).is_ok();
    }
    list_thread.join().unwrap();

    if !args.is_present("keep-existing") {
        await!(delete_dead_remote_files(config, b2, root, rfiles, signal_flag))?;
    }

    Ok(())
}

/// Delete remote files that were removed locally
async fn delete_dead_remote_files<'a>(config: &'a Config, b2: &'a mut b2::B2,
                                      root: BackupRoot, rfiles: Vec<RemoteFile>,
                                      signal_flag: Arc<AtomicBool>) -> Result<(), Box<dyn Error + 'static>> {
    let mut delete_threads = root.start_delete_threads(b2, config);
    progress::start_output(delete_threads.len());

    for rfile in rfiles {
        'delete_send: loop {
            for thread in &mut delete_threads {
                if thread.tx.try_send(Some(rfile.clone())).is_ok() {
                    break 'delete_send;
                }
            }
            err_on_signal(&signal_flag)?;
            await!(progress::handle_progress(config.verbose, &mut delete_threads));
            await!(Delay::new(Duration::from_millis(20))).is_ok();
        }
        err_on_signal(&signal_flag)?;
        await!(progress::handle_progress(config.verbose, &mut delete_threads));
    }

    // Tell our delete threads to stop as they become idle
    let mut thread_id = delete_threads.len() - 1;
    loop {
        err_on_signal(&signal_flag)?;
        if thread_id < delete_threads.len() {
            let result = &delete_threads[thread_id].tx.try_send(None);
            if result.is_err() {
                await!(progress::handle_progress(config.verbose, &mut delete_threads));
                await!(Delay::new(Duration::from_millis(20))).is_ok();
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
        err_on_signal(&signal_flag)?;
        await!(progress::handle_progress(config.verbose, &mut delete_threads));
        await!(Delay::new(Duration::from_millis(20))).is_ok();
    }

    Ok(())
}