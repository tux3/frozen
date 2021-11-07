use crate::action;
use crate::config::Config;
use crate::data::paths::path_from_bytes;
use crate::data::{paths::path_from_arg, root};
use crate::dirdb::dirstat::DirStat;
use crate::dirdb::{
    diff::{DirDiff, FileDiff},
    DirDB,
};
use crate::net::b2::B2;
use crate::net::rate_limiter::RateLimiter;
use crate::progress::{Progress, ProgressType};
use crate::signal::interruptible;
use clap::ArgMatches;
use eyre::{bail, Result};
use futures::stream::{FuturesUnordered, StreamExt};
use futures::task::SpawnExt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::task::spawn_blocking;

pub async fn restore(config: &Config, args: &ArgMatches<'_>) -> Result<()> {
    let path = path_from_arg(args, "source")?;
    let target = path_from_arg(args, "destination").unwrap_or_else(|_| path.clone());
    fs::create_dir_all(&target)?;

    let keys = config.get_app_keys()?;

    println!("Connecting to Backblaze B2");
    let b2 = B2::authenticate(config, &keys).await?;

    println!("Downloading backup metadata");
    let mut roots = root::fetch_roots(&b2).await?;
    let mut root = root::open_root(&b2, &mut roots, &path).await?;
    let arc_root = Arc::new(root.clone());

    let restore_fut = restore_one_root(config, target, b2, arc_root);
    let result = interruptible(restore_fut).await;

    root.unlock().await?;
    result
}

pub async fn restore_one_root(config: &Config, target: PathBuf, mut b2: B2, root: Arc<root::BackupRoot>) -> Result<()> {
    println!("Starting diff");
    let progress = Progress::new(config.verbose);
    let diff_progress = progress.show_progress_bar(ProgressType::Diff, 3);
    let download_progress = progress.get_progress_handler(ProgressType::Download);

    b2.progress.replace(diff_progress.clone());
    let b2 = Arc::new(b2);

    let target_dirdb = Arc::new(DirDB::new_from_local(&target, &b2.key)?);
    diff_progress.report_success();

    let dirdb_path = "dirdb/".to_string() + &root.path_hash;
    let remote_dirdb = b2
        .download_file(&dirdb_path)
        .await
        .ok()
        .and_then(|data| DirDB::new_from_packed(&data, &b2.key).ok());
    diff_progress.report_success();

    let mut dir_diff = DirDiff::new(root.clone(), b2.clone(), target_dirdb.clone(), &remote_dirdb)?;
    let target = Arc::new(target);

    diff_progress.println("Starting download");
    // Lets us wait for all backup actions to complete
    let action_futs = FuturesUnordered::new();

    let mut num_download_actions = 0;
    let rate_limiter = Arc::new(RateLimiter::new(config, &b2));
    while let Some(item) = dir_diff.next().await {
        let item = item?;

        match item {
            FileDiff {
                local,
                remote: Some(rfile),
            } => {
                if let Some(lfile) = local {
                    if lfile.last_modified >= rfile.last_modified {
                        continue;
                    }
                }
                num_download_actions += 1;
                action_futs.spawn(action::download(
                    rate_limiter.clone(),
                    download_progress.clone(),
                    target.clone(),
                    rfile,
                ))?;
            }
            FileDiff {
                local: Some(_),
                remote: None,
            } => (),
            FileDiff {
                local: None,
                remote: None,
            } => unreachable!(),
        }
    }

    let download_progress = progress.show_progress_bar(ProgressType::Download, num_download_actions);
    diff_progress.report_success();
    diff_progress.finish();

    let empty_folders_task = remote_dirdb.map(|dirdb| {
        let target = target.clone();
        spawn_blocking(move || {
            // Note how the root folder doesn't have a folder name, it's just the relative root "/"
            for subfolder in dirdb.root.subfolders {
                restore_empty_folders(subfolder, &target);
            }
        })
    });

    action_futs.for_each(|()| futures::future::ready(())).await;
    download_progress.finish();
    progress.join();
    if let Some(task) = empty_folders_task {
        task.await?;
    }

    if !progress.is_complete() {
        bail!("Couldn't complete all operations, {} error(s)", progress.errors_count())
    }
    Ok(())
}

fn restore_empty_folders(dir: DirStat, target: &Path) {
    let dir_path = if let Some(dir_name) = dir.dir_name {
        target.join(path_from_bytes(&dir_name).unwrap())
    } else {
        return;
    };

    if dir.total_files_count == 0 {
        let _ = fs::create_dir(&dir_path);
    }

    for subfolder in dir.subfolders {
        restore_empty_folders(subfolder, &dir_path);
    }
}
