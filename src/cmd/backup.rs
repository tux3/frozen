use std::error::Error;
use std::fs;
use std::path::Path;
use std::thread;
use std::time::Duration;
use config::Config;
use data::root;
use net::b2api;
use progress;
use util;

pub fn backup(config: &Config, path: &str) -> Result<(), Box<Error>> {
    let path = fs::canonicalize(path)?.to_string_lossy().into_owned();
    if !Path::new(&path).is_dir() {
        return Err(From::from(format!("{} is not a folder!", &path)))
    }

    println!("Connecting to Backblaze B2");
    let mut b2 = &mut b2api::authenticate(config)?;

    println!("Downloading backup metadata");
    let mut roots = root::fetch_roots(b2);

    let signal_flag = util::setup_signal_flag();

    println!("Opening backup folder {}", path);
    let root = root::open_create_root(b2, &mut roots, &path)?;

    println!("Starting to list local files");
    let (lfiles_rx, list_thread) = root.list_local_files_async(b2)?;
    util::err_on_signal(&signal_flag)?;

    println!("Listing remote files");
    let mut rfiles = root.list_remote_files(b2)?;
    util::err_on_signal(&signal_flag)?;

    println!("Starting upload");
    let mut upload_threads = root.start_upload_threads(b2, config);

    progress::start_output(upload_threads.len());

    for file in lfiles_rx {
        let rfile = rfiles.binary_search_by(|v| v.cmp_local(&file));
        if rfile.is_err() || rfiles[rfile.unwrap()].last_modified < file.last_modified {
            'upload_send: loop {
                for thread in &upload_threads {
                    if thread.tx.try_send(Some(file.clone())).is_ok() {
                        break 'upload_send;
                    }
                }
                progress::handle_progress(&mut upload_threads);
                util::err_on_signal(&signal_flag)?;
                thread::sleep(Duration::from_millis(20));
            }
            util::err_on_signal(&signal_flag)?;
            progress::handle_progress(&mut upload_threads);
        }
        if let Ok(rfile) = rfile {
            rfiles.remove(rfile);
        }
    }

    // Tell our threads to stop as they become idle
    let mut thread_id = upload_threads.len() - 1;
    loop {
        util::err_on_signal(&signal_flag)?;
        if thread_id < upload_threads.len() {
            let result = &upload_threads[thread_id].tx.try_send(None);
            if result.is_err() {
                progress::handle_progress(&mut upload_threads);
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

    while !upload_threads.is_empty() {
        util::err_on_signal(&signal_flag)?;
        progress::handle_progress(&mut upload_threads);
        thread::sleep(Duration::from_millis(20));
    }
    list_thread.join().unwrap();

    // Delete remote files that were removed locally
    let mut delete_threads = root.start_delete_threads(b2, config);
    progress::start_output(delete_threads.len());

    for rfile in rfiles {
        'delete_send: loop {
            for thread in &delete_threads {
                if thread.tx.try_send(Some(rfile.clone())).is_ok() {
                    break 'delete_send;
                }
            }
            util::err_on_signal(&signal_flag)?;
            progress::handle_progress(&mut delete_threads);
            thread::sleep(Duration::from_millis(20));
        }
        util::err_on_signal(&signal_flag)?;
        progress::handle_progress(&mut delete_threads);
    }

    // Tell our delete threads to stop as they become idle
    let mut thread_id = delete_threads.len() - 1;
    loop {
        util::err_on_signal(&signal_flag)?;
        if thread_id < delete_threads.len() {
            let result = &delete_threads[thread_id].tx.try_send(None);
            if result.is_err() {
                progress::handle_progress(&mut delete_threads);
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

    while !delete_threads.is_empty() {
        util::err_on_signal(&signal_flag)?;
        progress::handle_progress(&mut delete_threads);
        thread::sleep(Duration::from_millis(20));
    }

    Ok(())
}