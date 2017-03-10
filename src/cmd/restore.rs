use std::error::Error;
use std::thread;
use std::time::Duration;
use std::fs;
use config::Config;
use data::root;
use net::{b2api, download};
use progress;

pub fn restore(config: &Config, path: &str, target: Option<&str>) -> Result<(), Box<Error>> {
    let target = target.unwrap_or(path);
    fs::create_dir_all(target)?;
    let mut path = path.to_string();
    if path.ends_with("/") {
        path.pop();
    }

    println!("Connecting to Backblaze B2");
    let b2 = &b2api::authenticate(config)?;

    println!("Downloading backup metadata");
    let mut roots = root::fetch_roots(b2);

    println!("Opening backup folder {}", path);
    let root = root::open_root(&mut roots, &path)?;

    println!("Starting to list local files");
    let (lfiles_rx, list_thread) = root.list_local_files_async_at(b2, target)?;

    println!("Listing remote files");
    let mut rfiles = root.list_remote_files(b2)?;

    println!("Starting download");
    let mut download_threads = root.start_download_threads(b2, target);

    progress::start_output(download_threads.len());

    for file in lfiles_rx {
        let rfile = rfiles.binary_search_by(|v| v.cmp_local(&file));
        if rfile.is_ok() && rfiles[rfile.unwrap()].last_modified <= file.last_modified {
            println!("File up to date: {}", file.path_str());
            rfiles.remove(rfile.unwrap());
        }
    }

    for rfile in rfiles {
        'send: loop {
            for thread in &download_threads {
                if thread.tx.try_send(Some(rfile.clone())).is_ok() {
                    break 'send;
                }
            }
            handle_progress(&mut download_threads);
            thread::sleep(Duration::from_millis(50));
        }
        handle_progress(&mut download_threads);
    }

    for thread in &download_threads {
        thread.tx.send(None)?;
    }

    while !download_threads.is_empty() {
        handle_progress(&mut download_threads);
    }
    list_thread.join().unwrap();

    Ok(())
}

/// Receives and displays progress information. Removes dead threads from the list.
fn handle_progress(threads: &mut Vec<download::DownloadThread>) {
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
}