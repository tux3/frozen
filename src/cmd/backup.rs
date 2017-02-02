use std::error::Error;
use std::thread;
use std::time::Duration;
use config::Config;
use data::root;
use b2api;

pub fn backup(config: &Config, path: &String) -> Result<(), Box<Error>> {
    println!("Connecting to Backblaze B2");
    let mut b2 = &mut b2api::authenticate(config)?;

    println!("Downloading backup metadata");
    let mut roots = root::fetch_roots(b2);

    println!("Opening backup folder {}", path);
    let root = root::open_root(b2, &mut roots, path)?;
    println!("Found {} roots", roots.len());
    println!("Opened root {} hash {}", root.path, root.path_hash);

    println!("Starting to list local files");
    let (lfiles_rx, list_thread) = root.list_local_files_async(&b2)?;

    println!("Listing remote files");
    let rfiles = root.list_remote_files(&b2)?;

    println!("Starting upload");
    let upload_threads = root.start_upload_threads(&b2);

    for file in lfiles_rx {
        let rfile = rfiles.binary_search_by(|v| v.cmp(&file));
        if rfile.is_ok() && rfiles[rfile.unwrap()].last_modified == file.last_modified {
            println!("File up to date: {}", file.rel_path_hash);
        } else {
            'send: loop {
                for thread in upload_threads.iter() {
                    if thread.tx.try_send(file.clone()).is_ok() {
                        break 'send;
                    }
                }
                show_upload_status(&upload_threads);
                thread::sleep(Duration::from_millis(50));
            }
            show_upload_status(&upload_threads);
        }
    }

    // TODO: Remove remote files that don't exist locally

    for thread in upload_threads {
        drop(thread.tx);
        thread.handle.join().unwrap();
    }
    list_thread.join().unwrap();
    Ok(())
}

fn show_upload_status(threads: &Vec<root::UploadThread>) {
    /* TODO: Have a line per thread to show what each thread is doing, using VT100
             When a thread is done uploading a file (or on error), print the line above */
}