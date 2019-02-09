use std::error::Error;
use std::fs;
use std::time::Duration;
use clap::ArgMatches;
use futures_timer::Delay;
use tokio::await;
use crate::config::Config;
use crate::data::root;
use crate::net::b2::B2;
use crate::progress;
use crate::util;

pub async fn restore<'a>(config: &'a Config, args: &'a ArgMatches<'a>) -> Result<(), Box<dyn Error + 'static>> {
    let mut path = args.value_of("source").unwrap().to_string();
    if let Ok(canon_path) = fs::canonicalize(&path) {
        path = canon_path.to_string_lossy().into_owned();
    }
    let target = args.value_of("destination").unwrap_or(&path);
    fs::create_dir_all(target)?;

    let keys = config.get_app_keys()?;

    println!("Connecting to Backblaze B2");
    let b2 = await!(B2::authenticate(config, &keys))?;

    println!("Downloading backup metadata");
    let mut roots = await!(root::fetch_roots(&b2))?;

    let signal_flag = util::setup_signal_flag();

    println!("Opening backup folder {}", path);
    let root = await!(root::open_root(&b2, &mut roots, &path))?;

    println!("Starting to list local files");
    let (lfiles_rx, list_thread) = root.list_local_files_async(&b2, target)?;
    util::err_on_signal(&signal_flag)?;

    println!("Listing remote files");
    let mut rfiles = await!(root.list_remote_files(&b2))?;
    util::err_on_signal(&signal_flag)?;

    println!("Starting download");
    let mut download_threads = root.start_download_threads(&b2, config, target);

    progress::start_output(download_threads.len());

    for file in lfiles_rx {
        let rfile = rfiles.binary_search_by(|v| v.cmp_local(&file));
        if rfile.is_ok() && rfiles[rfile.unwrap()].last_modified <= file.last_modified {
            rfiles.remove(rfile.unwrap());
        }
        util::err_on_signal(&signal_flag)?;
    }

    for rfile in rfiles {
        'send: loop {
            for thread in &mut download_threads {
                if thread.tx.try_send(Some(rfile.clone())).is_ok() {
                    break 'send;
                }
            }
            util::err_on_signal(&signal_flag)?;
            await!(progress::handle_progress(config.verbose, &mut download_threads));
            await!(Delay::new(Duration::from_millis(20))).is_ok();
        }
        util::err_on_signal(&signal_flag)?;
        await!(progress::handle_progress(config.verbose, &mut download_threads));
    }

    // Tell our threads to stop as they become idle
    let mut thread_id = download_threads.len() - 1;
    loop {
        util::err_on_signal(&signal_flag)?;
        if thread_id < download_threads.len() {
            let result = &download_threads[thread_id].tx.try_send(None);
            if result.is_err() {
                await!(progress::handle_progress(config.verbose, &mut download_threads));
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

    while !download_threads.is_empty() {
        util::err_on_signal(&signal_flag)?;
        await!(progress::handle_progress(config.verbose, &mut download_threads));
        await!(Delay::new(Duration::from_millis(20))).is_ok();
    }
    list_thread.join().unwrap();

    Ok(())
}
