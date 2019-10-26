use std::error::Error;
use std::path::{Path, PathBuf};
use std::sync::{Arc, mpsc::Receiver};
use std::cmp::max;
use clap::ArgMatches;
use futures::stream::StreamExt;
use tokio_executor::threadpool;
use crate::action::{scoped_runtime, upload};
use crate::net::rate_limiter::RateLimiter;
use crate::box_result::BoxResult;
use crate::config::Config;
use crate::net::b2;
use crate::data::root::{self, BackupRoot};
use crate::data::file::{LocalFile, RemoteFile};
use crate::data::paths::path_from_arg;
use crate::dirdb::{DirDB, diff::DirDiff, diff::FileDiff};
use crate::termio::progress::{self, ProgressDataReader};
use crate::signal::*;

pub async fn backup(config: &Config, args: &ArgMatches<'_>) -> BoxResult<()> {
    let path = path_from_arg(args, "source")?;
    if !path.is_dir() {
        return Err(From::from(format!("{} is not a folder!", &path.display())))
    }
    let target = path_from_arg(args, "destination").unwrap_or_else(|_| path.clone());
    let keys = config.get_app_keys()?;

    println!("Connecting to Backblaze B2");
    let b2 = b2::B2::authenticate(config, &keys).await?;

    println!("Downloading backup metadata");
    let mut roots = root::fetch_roots(&b2).await?;
    let root = root::open_create_root(&b2, &mut roots, &target).await?;

    let arc_b2 = Arc::new(b2.clone());
    let mut arc_root = Arc::new(root.clone());
    let arc_path = Arc::new(path.clone());

    let result = backup_one_root(config, args, path, arc_b2, arc_root.clone()).await;

    if let Some(root) = Arc::get_mut(&mut arc_root) {
        root.unlock().await?;
    } else {
        eprintln!("Error: Failed to unlock the backup root (Arc still has {} holders!)", Arc::strong_count(&arc_root));
    }

    return result;
}

pub async fn backup_one_root(config: &Config, args: &ArgMatches<'_>, path: PathBuf, b2: Arc<b2::B2>, root: Arc<BackupRoot>) -> BoxResult<()> {
    println!("Starting diff");
    let local_dirdb = Arc::new(DirDB::new_from_local(&path)?);
    let dirdb_path = "dirdb/".to_string()+&root.path_hash;
    let remote_dirdb = b2.download_file(&dirdb_path).await.and_then(|data| {
        DirDB::new_from_packed(&data, &b2.key)
    }).ok();
    let mut dir_diff = DirDiff::new(root.clone(), b2.clone(), local_dirdb.clone(), remote_dirdb)?;
    let path = Arc::new(path);

    println!("Uploading pessimistic dirdb");
    let dirdb_data = dir_diff.get_pessimistic_dirdb_data(&b2.key)?;
    b2.upload_file_simple(&dirdb_path, dirdb_data).await?;

    println!("Starting backup");
    let num_threads = num_cpus::get().max(1);
    progress::start_output(config.verbose, num_threads);
    // This is scoped to shutdown when we're done running actions on the backup folder
    let action_runtime = scoped_runtime::Builder::new()
        .name_prefix("backup-")
        .pool_size(num_threads)
        .build()?;
    let rate_limiter = Arc::new(RateLimiter::new(&config));
    let keep_existing = args.is_present("keep-existing");
    while let Some(item) = dir_diff.next().await {
        let item = item?;

        match item {
            FileDiff{local: None, remote: Some(rfile)} => {
                if keep_existing {
                    continue
                }
                println!("# Would delete {}", rfile.rel_path.display());
                //worker_runtime.spawn(delete(arc_root.clone(), arc_b2.clone(), rfile))
            },
            FileDiff{local: Some(lfile), remote} => {
                if let Some(rfile) = remote {
                    if rfile.last_modified >= lfile.last_modified {
                        continue
                    }
                }
                action_runtime.spawn(upload(rate_limiter.clone(), root.clone(), b2.clone(), config.compression_level, path.clone(), lfile));
            },
            FileDiff{local: None, remote: None} => unreachable!()
        }
    };
    action_runtime.shutdown_on_idle().await;

    b2.upload_file_simple(&dirdb_path, local_dirdb.to_packed(&b2.key)?).await?;

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
            progress::handle_progress(config.verbose, &mut delete_threads).await;
            //Delay::new(Duration::from_millis(20)).await.ignore();
        }
        err_on_signal()?;
        progress::handle_progress(config.verbose, &mut delete_threads).await;
    }

    // Tell our delete threads to stop as they become idle
    let mut thread_id = delete_threads.len() - 1;
    loop {
        err_on_signal()?;
        if thread_id < delete_threads.len() {
            let result = &delete_threads[thread_id].tx.try_send(None);
            if result.is_err() {
                progress::handle_progress(config.verbose, &mut delete_threads).await;
                //Delay::new(Duration::from_millis(20)).await.ignore();
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
        progress::handle_progress(config.verbose, &mut delete_threads).await;
        //Delay::new(Duration::from_millis(20)).await.ignore();
    }

    Ok(())
}
