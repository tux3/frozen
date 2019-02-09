use std::error::Error;
use std::path::Path;
use std::time::Duration;
use std::sync::{Arc, mpsc::Receiver};
use std::cmp::max;
use clap::ArgMatches;
use futures::stream::StreamExt;
use tokio_threadpool;
use crate::config::Config;
use crate::data::root::{self, BackupRoot};
use crate::data::file::{LocalFile, RemoteFile};
use crate::data::paths::path_from_arg;
use crate::dirdb::{DirDB, diff::DirDiff, diff::FileDiff};
use crate::net::{b2, delete::delete, upload::upload};
use crate::termio::progress::{self, ProgressDataReader};
use crate::signal::*;

pub async fn backup<'a>(config: &'a Config, args: &'a ArgMatches<'a>) -> Result<(), Box<dyn Error + 'static>> {
    let path = path_from_arg(args, "source")?;
    if !path.is_dir() {
        return Err(From::from(format!("{} is not a folder!", &path.display())))
    }
    let target = path_from_arg(args, "destination").unwrap_or_else(|_| path.clone());

    let keep_existing = args.is_present("keep-existing");
    let keys = config.get_app_keys()?;
    let net_executor = tokio_threadpool::Builder::new()
        .pool_size(max(1, num_cpus::get()))
        .build();

    println!("Connecting to Backblaze B2");
    let b2 = b2::B2::authenticate(config, &keys).await?;

    println!("Downloading backup metadata");
    let mut roots = root::fetch_roots(&b2).await?;

    println!("Opening backup folder {}", target.display());
    let root = root::open_create_root(&b2, &mut roots, &target).await?;

    println!("Starting to list local files");
    let (lfiles_rx, list_thread) = root.list_local_files_async(&b2, &path)?;
    err_on_signal()?;

    let local_dirdb = DirDB::new_from_local(&path)?;

    println!("Listing remote files");
    let rfiles = root.list_remote_files(&b2).await?;
    err_on_signal()?;


    let dirdb_path = "dirdb/".to_string()+&root.path_hash;
    let remote_dirdb = b2.download_file(&dirdb_path).await.and_then(|data| {
        DirDB::new_from_packed(&data, &keys.encryption_key)
    }).ok();
    let mut dir_diff = DirDiff::new(&local_dirdb, remote_dirdb)?;
    dir_diff.start_diff_remote_files(&root, &b2).await?;

    if let Some(dirdb_data) = dir_diff.get_pessimistic_dirdb_data(&b2.key)? {
        b2.upload_file(&dirdb_path, ProgressDataReader::new_silent(dirdb_data), None).await?;
    }

    let arc_b2 = Arc::new(b2.clone());
    let arc_root = Arc::new(root);
    let arc_path = Arc::new(path);

    println!("Starting backup");
    progress::start_output(config.verbose, num_cpus::get());
    while let Some(item) = dir_diff.next().await {
        match item {
            FileDiff{local: None, remote: Some(rfile)} => {
                if keep_existing {
                    continue
                }
                net_executor.spawn(delete(arc_root.clone(), arc_b2.clone(), rfile))
            },
            FileDiff{local: Some(lfile), remote} => {
                if let Some(rfile) = remote {
                    if rfile.last_modified >= lfile.last_modified {
                        continue
                    }
                }
                net_executor.spawn(upload(arc_root.clone(), arc_b2.clone(), config.compression_level, arc_path.clone(), lfile))
            },
            FileDiff{local: None, remote: None} => unreachable!()
        }
    };
    net_executor.shutdown_on_idle().await;

//    upload_updated_files(config, &mut b2, &root, &path, lfiles_rx, &mut rfiles).await?;
    list_thread.join().unwrap();
//
//    if !args.is_present("keep-existing") {
//        delete_dead_remote_files(config, &mut b2, &root, &rfiles).await?;
//    }

    let new_dirdb_stream = ProgressDataReader::new_silent(local_dirdb.to_packed(&b2.key)?);
    b2.upload_file(&dirdb_path, new_dirdb_stream, None).await?;

    Ok(())
}

/// Delete remote files that were removed locally
async fn delete_dead_remote_files<'a>(config: &'a Config,
                                      b2: &'a mut b2::B2,
                                      root: &'a BackupRoot,
                                      rfiles: &'a [RemoteFile]) -> Result<(), Box<dyn Error + 'static>> {
    let mut delete_threads = root.start_delete_threads(b2, config);
    progress::start_output(config.verbose, delete_threads.len());

    for rfile in rfiles {
        'delete_send: loop {
            for thread in &mut delete_threads {
                if thread.tx.try_send(Some(rfile.clone())).is_ok() {
                    break 'delete_send;
                }
            }
            err_on_signal()?;
            await!(progress::handle_progress(config.verbose, &mut delete_threads));
            //await!(Delay::new(Duration::from_millis(20))).ignore();
        }
        err_on_signal()?;
        await!(progress::handle_progress(config.verbose, &mut delete_threads));
    }

    // Tell our delete threads to stop as they become idle
    let mut thread_id = delete_threads.len() - 1;
    loop {
        err_on_signal()?;
        if thread_id < delete_threads.len() {
            let result = &delete_threads[thread_id].tx.try_send(None);
            if result.is_err() {
                await!(progress::handle_progress(config.verbose, &mut delete_threads));
                //await!(Delay::new(Duration::from_millis(20))).ignore();
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
        err_on_signal()?;
        await!(progress::handle_progress(config.verbose, &mut delete_threads));
        //await!(Delay::new(Duration::from_millis(20))).ignore();
    }

    Ok(())
}

/// Upload files that were modified locally
async fn upload_updated_files<'a>(config: &'a Config, b2: &'a mut b2::B2,
                                  root: &'a BackupRoot, path: &'a Path,
                                  lfiles_rx: Receiver<LocalFile>,
                                  rfiles: &'a mut Vec<RemoteFile>) -> Result<(), Box<dyn Error + 'static>> {
    let mut upload_threads = root.start_upload_threads(b2, config, path);

    progress::start_output(config.verbose, upload_threads.len());

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
                err_on_signal()?;
                //await!(Delay::new(Duration::from_millis(20))).ignore();
            }
            err_on_signal()?;
            await!(progress::handle_progress(config.verbose, &mut upload_threads));
        }
        if let Ok(rfile) = rfile {
            rfiles.remove(rfile);
        }
    }

    // Tell our threads to stop as they become idle
    let mut thread_id = upload_threads.len() - 1;
    loop {
        err_on_signal()?;
        if thread_id < upload_threads.len() {
            let result = &upload_threads[thread_id].tx.try_send(None);
            if result.is_err() {
                await!(progress::handle_progress(config.verbose, &mut upload_threads));
                //await!(Delay::new(Duration::from_millis(20))).ignore();
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
        err_on_signal()?;
        await!(progress::handle_progress(config.verbose, &mut upload_threads));
        //await!(Delay::new(Duration::from_millis(20))).ignore();
    }

    Ok(())
}
