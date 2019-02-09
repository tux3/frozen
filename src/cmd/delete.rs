use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;
use clap::ArgMatches;
use futures_timer::Delay;
use tokio::await;
use crate::config::Config;
use crate::data::root;
use crate::net::b2::B2;
use crate::termio::progress;
use crate::signal::*;

pub async fn delete<'a>(config: &'a Config, args: &'a ArgMatches<'a>) -> Result<(), Box<dyn Error + 'static>> {
    let path = args.value_of("target").unwrap().to_owned();
    let keys = config.get_app_keys()?;

    println!("Connecting to Backblaze B2");
    let mut b2 = await!(B2::authenticate(config, &keys))?;

    println!("Downloading backup metadata");
    let mut roots = await!(root::fetch_roots(&b2))?;

    println!("Deleting backup folder {}", path);

    let signal_flag = setup_signal_flag();

    let root = await!(root::open_root(&b2, &mut roots, &path))?;
    await!(delete_files(config, &mut b2, &root, signal_flag))?;

    await!(root::delete_root(&mut b2, &mut roots, &path))
}

async fn delete_files<'a>(config: &'a Config, b2: &'a mut B2,
                root: &'a root::BackupRoot, signal_flag: Arc<AtomicBool>)
        -> Result<(), Box<dyn Error + 'static>> {
    err_on_signal(&signal_flag)?;

    println!("Listing remote files");
    let rfiles = await!(root.list_remote_files(b2))?;

    // Delete all remote files
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