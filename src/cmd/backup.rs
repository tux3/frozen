use std::error::Error;
use std::path::PathBuf;
use std::sync::{Arc, mpsc::Receiver};
use std::cmp::max;
use clap::ArgMatches;
use futures::stream::StreamExt;
use tokio_executor::threadpool;
use crate::action::{self, scoped_runtime};
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

    result
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

    println!("Uploading pessimistic DirDB");
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
                if !keep_existing {
                    action_runtime.spawn(action::delete(rate_limiter.clone(), root.clone(), b2.clone(), rfile));
                }
            },
            FileDiff{local: Some(lfile), remote} => {
                if let Some(rfile) = remote {
                    if rfile.last_modified >= lfile.last_modified {
                        continue
                    }
                }
                action_runtime.spawn(action::upload(rate_limiter.clone(), root.clone(), b2.clone(), config.compression_level, path.clone(), lfile));
            },
            FileDiff{local: None, remote: None} => unreachable!()
        }
    };
    let packed_local_dirdb = local_dirdb.to_packed(&b2.key)?;
    action_runtime.shutdown_on_idle().await;

    b2.upload_file_simple(&dirdb_path, packed_local_dirdb).await?;

    Ok(())
}
