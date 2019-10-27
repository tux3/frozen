use std::error::Error;
use std::fs;
use std::sync::Arc;
use std::path::PathBuf;
use futures::stream::StreamExt;
use clap::ArgMatches;
use ignore_result::Ignore;
use crate::config::Config;
use crate::data::{root, paths::path_from_arg};
use crate::net::b2::B2;
use crate::termio::progress;
use crate::signal::*;
use crate::dirdb::{DirDB, diff::{FileDiff, DirDiff}};
use crate::action::{self, scoped_runtime};
use crate::net::rate_limiter::RateLimiter;

pub async fn restore(config: &Config, args: &ArgMatches<'_>) -> Result<(), Box<dyn Error + 'static>> {
    let path = path_from_arg(args, "source")?;
    let target = path_from_arg(args, "destination").unwrap_or_else(|_| path.clone());
    fs::create_dir_all(&target)?;

    let keys = config.get_app_keys()?;

    println!("Connecting to Backblaze B2");
    let b2 = B2::authenticate(config, &keys).await?;

    println!("Downloading backup metadata");
    let mut roots = root::fetch_roots(&b2).await?;
    let root = root::open_root(&b2, &mut roots, &path).await?;

    let arc_b2 = Arc::new(b2.clone());
    let mut arc_root = Arc::new(root.clone());
    let arc_path = Arc::new(path.clone());

    let result = restore_one_root(config, args, path, target, arc_b2, arc_root.clone()).await;

    if let Some(root) = Arc::get_mut(&mut arc_root) {
        root.unlock().await?;
    } else {
        eprintln!("Error: Failed to unlock the backup root (Arc still has {} holders!)", Arc::strong_count(&arc_root));
    }

    result
}

pub async fn restore_one_root(config: &Config, args: &ArgMatches<'_>, path: PathBuf, target: PathBuf,
                              b2: Arc<B2>, root: Arc<root::BackupRoot>) -> Result<(), Box<dyn Error + 'static>> {
    println!("Starting diff");
    let target_dirdb = Arc::new(DirDB::new_from_local(&target)?);
    let dirdb_path = "dirdb/".to_string()+&root.path_hash;
    let remote_dirdb = b2.download_file(&dirdb_path).await.and_then(|data| {
        DirDB::new_from_packed(&data, &b2.key)
    }).ok();
    let mut dir_diff = DirDiff::new(root.clone(), b2.clone(), target_dirdb.clone(), remote_dirdb)?;
    let target = Arc::new(target);

    println!("Starting download");
    let num_threads = num_cpus::get().max(1);
    progress::start_output(config.verbose, num_threads);
    // This is scoped to shutdown when we're done running actions on the backup folder
    let action_runtime = scoped_runtime::Builder::new()
        .name_prefix("restore-")
        .pool_size(num_threads)
        .build()?;
    let rate_limiter = Arc::new(RateLimiter::new(&config));
    while let Some(item) = dir_diff.next().await {
        let item = item?;

        match item {
            FileDiff{local, remote: Some(rfile)} => {
                if let Some(lfile) = local {
                    if lfile.last_modified >= rfile.last_modified {
                        continue
                    }
                }
                action_runtime.spawn(action::download(rate_limiter.clone(), root.clone(), b2.clone(), target.clone(), rfile));
            },
            FileDiff{local: Some(_), remote: None} => (),
            FileDiff{local: None, remote: None} => unreachable!()
        }
    };
    action_runtime.shutdown_on_idle().await;

    Ok(())
}