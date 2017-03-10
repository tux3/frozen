use std::error::Error;
use std::thread;
use std::time::Duration;
use config::Config;
use data::root;
use net::{b2api, upload};
use progress;

pub fn backup(config: &Config, path: &str) -> Result<(), Box<Error>> {
    println!("Connecting to Backblaze B2");
    let mut b2 = &mut b2api::authenticate(config)?;

    println!("Downloading backup metadata");
    let mut roots = root::fetch_roots(b2);

    println!("Opening backup folder {}", path);
    let root = root::open_create_root(b2, &mut roots, path)?;

    println!("Starting to list local files");
    let (lfiles_rx, list_thread) = root.list_local_files_async(b2)?;

    println!("Listing remote files");
    let rfiles = root.list_remote_files(b2)?;

    println!("Starting upload");
    let mut upload_threads = root.start_upload_threads(b2);

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
                thread::sleep(Duration::from_millis(50));
            }
            handle_progress(&mut upload_threads);
        }
    }

    for thread in &upload_threads {
        thread.tx.send(None)?;
    }

    while !upload_threads.is_empty() {
        handle_progress(&mut upload_threads);
    }
    list_thread.join().unwrap();

    // TODO: Remove remote files that don't exist locally

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
}