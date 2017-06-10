use std::error::Error;
use std::fs;
use std::path::Path;
use std::thread;
use std::time::Duration;
use config::Config;
use data::root;
use net::b2api;
use progress;

pub fn delete(config: &Config, path: &str) -> Result<(), Box<Error>> {
    let path = fs::canonicalize(path)?.to_string_lossy().into_owned();
    if !Path::new(&path).is_dir() {
        return Err(From::from(format!("{} is not a folder!", &path)))
    }

    println!("Connecting to Backblaze B2");
    let mut b2 = &mut b2api::authenticate(config)?;

    println!("Downloading backup metadata");
    let mut roots = root::fetch_roots(b2);

    println!("Deleting backup folder {}", path);
    {
        let root = root::open_create_root(&mut b2, &mut roots, &path)?;
        delete_files(config, &mut b2, &root)?;
    }

    root::delete_root(&mut b2, &mut roots, &path)?;

    Ok(())
}

fn delete_files(config: &Config, b2: &mut b2api::B2, root: &root::BackupRoot)
    -> Result<(), Box<Error>> {
    println!("Listing remote files");
    let rfiles = root.list_remote_files(b2)?;

    // Delete all remote files
    let mut delete_threads = root.start_delete_threads(b2, config);
    progress::start_output(delete_threads.len());

    for rfile in rfiles {
        'delete_send: loop {
            for thread in &delete_threads {
                if thread.tx.try_send(Some(rfile.clone())).is_ok() {
                    break 'delete_send;
                }
            }
            progress::handle_progress(&mut delete_threads);
            thread::sleep(Duration::from_millis(20));
        }
        progress::handle_progress(&mut delete_threads);
    }

    // Tell our delete threads to stop as they become idle
    let mut thread_id = delete_threads.len() - 1;
    loop {
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
        progress::handle_progress(&mut delete_threads);
        thread::sleep(Duration::from_millis(20));
    }

    Ok(())
}