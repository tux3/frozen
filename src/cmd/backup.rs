use std::error::Error;
use std::fs;
use std::path::Path;
use std::thread;
use std::time::Duration;
use config::Config;
use data::root;
use net::{b2api, upload};
use progress;

pub fn backup(config: &Config, path: &str) -> Result<(), Box<Error>> {
    let path = fs::canonicalize(path)?.to_string_lossy().into_owned();
    if !Path::new(&path).is_dir() {
        return Err(From::from(format!("{} is not a folder!", &path)))
    }

    println!("Connecting to Backblaze B2");
    let mut b2 = &mut b2api::authenticate(config)?;

    println!("Downloading backup metadata");
    let mut roots = root::fetch_roots(b2);

    println!("Opening backup folder {}", path);
    let root = root::open_create_root(b2, &mut roots, &path)?;

    println!("Starting to list local files");
    let (lfiles_rx, list_thread) = root.list_local_files_async(b2)?;

    println!("Listing remote files");
    let mut rfiles = root.list_remote_files(b2)?;

    println!("Starting upload");
    let mut upload_threads = root.start_upload_threads(b2, config);

    progress::start_output(upload_threads.len());

    for file in lfiles_rx {
        let rfile = rfiles.binary_search_by(|v| v.cmp_local(&file));
        if rfile.is_err() || rfiles[rfile.unwrap()].last_modified != file.last_modified {
            'send: loop {
                for thread in &upload_threads {
                    if thread.tx.try_send(Some(file.clone())).is_ok() {
                        break 'send;
                    }
                }
                handle_progress(&mut upload_threads);
                thread::sleep(Duration::from_millis(20));
            }
            handle_progress(&mut upload_threads);
        }
        if let Ok(rfile) = rfile {
            rfiles.remove(rfile);
        }
    }

    // Tell our threads to stop as they become idle
    let mut thread_id = upload_threads.len() - 1;
    loop {
        if thread_id < upload_threads.len() {
            let result = &upload_threads[thread_id].tx.try_send(None);
            if result.is_err() {
                handle_progress(&mut upload_threads);
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
        handle_progress(&mut upload_threads);
        thread::sleep(Duration::from_millis(20));
    }
    list_thread.join().unwrap();

    // TODO: Remove remote files that don't exist locally
    for rfile in rfiles {
        println!("Deleting removed file {}", rfile.rel_path);
        b2api::delete_file(&b2, &(root.path_hash.clone()+"/"+&rfile.rel_path_hash))?;
    }

    Ok(())
}

/// Receives and displays progress information. Removes dead threads from the list.
fn handle_progress(threads: &mut Vec<upload::UploadThread>) {
    let mut num_threads = threads.len();
    let mut thread_id = 0;
    while thread_id < num_threads {
        let mut delete_later = false;
        {
            let thread = &threads[thread_id];
            loop {
                let progress = thread.rx.try_recv();
                if progress.is_err() {
                    break;
                }
                let progress = progress.unwrap();
                if let progress::Progress::Terminated = progress {
                    delete_later = true;
                }
                progress::progress_output(&progress, thread_id, num_threads);
            }
        }
        if delete_later {
            threads.remove(thread_id);
            num_threads -= 1;
        }

        thread_id += 1;
    }
    progress::flush();
}