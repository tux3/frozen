use std::fs;
use std::sync::Arc;
use std::path::PathBuf;
use futures::stream::StreamExt;
use clap::ArgMatches;
use crate::config::Config;
use crate::data::{root, paths::path_from_arg};
use crate::net::b2::B2;
use crate::dirdb::{DirDB, diff::{FileDiff, DirDiff}};
use crate::action::{self, scoped_runtime};
use crate::net::rate_limiter::RateLimiter;
use crate::box_result::BoxResult;
use crate::progress::{Progress, ProgressType};

pub async fn restore(config: &Config, args: &ArgMatches<'_>) -> BoxResult<()> {
    let path = path_from_arg(args, "source")?;
    let target = path_from_arg(args, "destination").unwrap_or_else(|_| path.clone());
    fs::create_dir_all(&target)?;

    let keys = config.get_app_keys()?;

    println!("Connecting to Backblaze B2");
    let b2 = B2::authenticate(config, &keys).await?;

    println!("Downloading backup metadata");
    let mut roots = root::fetch_roots(&b2).await?;
    let root = root::open_root(&b2, &mut roots, &path).await?;

    let mut arc_root = Arc::new(root.clone());

    let result = restore_one_root(config, target, b2, arc_root.clone()).await;

    if let Some(root) = Arc::get_mut(&mut arc_root) {
        root.unlock().await?;
    } else {
        eprintln!("Error: Failed to unlock the backup root (Arc still has {} holders!)", Arc::strong_count(&arc_root));
    }

    result
}

pub async fn restore_one_root(config: &Config, target: PathBuf,
                              mut b2: B2, root: Arc<root::BackupRoot>) -> BoxResult<()> {
    println!("Starting diff");
    let progress = Progress::new(config.verbose);
    let diff_progress = progress.show_progress_bar(ProgressType::Diff, 3);
    let download_progress = progress.get_progress_handler(ProgressType::Download);

    b2.progress.replace(diff_progress.clone());
    let b2 = Arc::new(b2);

    // TODO: Factor out the code in backup.rs that fetches/creates DirDBs and the DirDiff, reuse here (with the right target path, ofc)

    let target_dirdb = Arc::new(DirDB::new_from_local(&target)?);
    diff_progress.report_success();

    let dirdb_path = "dirdb/".to_string()+&root.path_hash;
    let remote_dirdb = b2.download_file(&dirdb_path).await.ok().and_then(|data| {
        DirDB::new_from_packed(&data, &b2.key).ok()
    });
    diff_progress.report_success();

    let mut dir_diff = DirDiff::new(root.clone(), b2.clone(), target_dirdb.clone(), remote_dirdb)?;
    let target = Arc::new(target);

    diff_progress.println("Starting download");
    // This is scoped to shutdown when we're done running actions on the backup folder
    let action_runtime = scoped_runtime::Builder::new()
        .name_prefix("restore-")
        .pool_size(num_cpus::get().max(1))
        .build()?;
    let mut num_download_actions = 0;
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
                num_download_actions += 1;
                action_runtime.spawn(action::download(rate_limiter.clone(), download_progress.clone(),
                                                      root.clone(), b2.clone(), target.clone(), rfile))?;
            },
            FileDiff{local: Some(_), remote: None} => (),
            FileDiff{local: None, remote: None} => unreachable!()
        }
    };

    let download_progress = progress.show_progress_bar(ProgressType::Download, num_download_actions);
    diff_progress.report_success();
    diff_progress.finish();

    action_runtime.shutdown_on_idle().await;
    download_progress.finish();
    progress.join();

    if progress.is_complete() {
        Ok(())
    } else {
        Err(From::from(format!("Couldn't complete all operations, {} error(s)", progress.errors_count())))
    }
}