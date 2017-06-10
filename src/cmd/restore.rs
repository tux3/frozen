use std::error::Error;
use std::thread;
use std::time::Duration;
use std::fs;
use config::Config;
use data::root;
use net::b2api;
use progress;
use util;

pub fn restore(config: &Config, path: &str, target: Option<&str>) -> Result<(), Box<Error>> {
    let mut path = path.to_string();
    let target = if target.is_none() {
        fs::create_dir_all(&path)?;
        path = fs::canonicalize(&path)?.to_string_lossy().into_owned();
        &path
    } else {
        fs::create_dir_all(target.unwrap())?;
        if let Ok(canon_path) = fs::canonicalize(&path) {
            path = canon_path.to_string_lossy().into_owned();
        }
        target.unwrap()
    };

    println!("Connecting to Backblaze B2");
    let b2 = &b2api::authenticate(config)?;

    println!("Downloading backup metadata");
    let mut roots = root::fetch_roots(b2);

    let signal_flag = util::setup_signal_flag();

    println!("Opening backup folder {}", path);
    let root = root::open_root(b2, &mut roots, &path)?;

    println!("Starting to list local files");
    let (lfiles_rx, list_thread) = root.list_local_files_async_at(b2, target)?;
    util::err_on_signal(&signal_flag)?;

    println!("Listing remote files");
    let mut rfiles = root.list_remote_files(b2)?;
    util::err_on_signal(&signal_flag)?;

    println!("Starting download");
    let mut download_threads = root.start_download_threads(b2, config, target);

    progress::start_output(download_threads.len());

    for file in lfiles_rx {
        let rfile = rfiles.binary_search_by(|v| v.cmp_local(&file));
        if rfile.is_ok() && rfiles[rfile.unwrap()].last_modified <= file.last_modified {
            println!("File up to date: {}", file.path_str());
            rfiles.remove(rfile.unwrap());
        }
        util::err_on_signal(&signal_flag)?;
    }

    for rfile in rfiles {
        'send: loop {
            for thread in &download_threads {
                if thread.tx.try_send(Some(rfile.clone())).is_ok() {
                    break 'send;
                }
            }
            util::err_on_signal(&signal_flag)?;
            progress::handle_progress(&mut download_threads);
            thread::sleep(Duration::from_millis(20));
        }
        util::err_on_signal(&signal_flag)?;
        progress::handle_progress(&mut download_threads);
    }

    // Tell our threads to stop as they become idle
    let mut thread_id = download_threads.len() - 1;
    loop {
        util::err_on_signal(&signal_flag)?;
        if thread_id < download_threads.len() {
            let result = &download_threads[thread_id].tx.try_send(None);
            if result.is_err() {
                progress::handle_progress(&mut download_threads);
                thread::sleep(Duration::from_millis(20));
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
        progress::handle_progress(&mut download_threads);
        thread::sleep(Duration::from_millis(20));
    }
    list_thread.join().unwrap();

    Ok(())
}
