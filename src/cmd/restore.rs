use crate::action::{self, scoped_runtime};
use crate::box_result::BoxResult;
use crate::config::Config;
use crate::data::{paths::path_from_arg, root};
use crate::dirdb::{
    diff::{DirDiff, FileDiff},
    DirDB,
};
use crate::net::b2::B2;
use crate::net::rate_limiter::RateLimiter;
use crate::progress::{Progress, ProgressType};
use crate::signal::SignalHandler;
use clap::ArgMatches;
use futures::stream::StreamExt;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

pub async fn restore(config: &Config, args: &ArgMatches<'_>) -> BoxResult<()> {
    let path = path_from_arg(args, "source")?;
    let target = path_from_arg(args, "destination").unwrap_or_else(|_| path.clone());
    fs::create_dir_all(&target)?;

    let keys = config.get_app_keys()?;

    println!("Connecting to Backblaze B2");
    let b2 = B2::authenticate(config, &keys).await?;

    println!("Downloading backup metadata");
    let mut roots = root::fetch_roots(&b2).await?;
    let mut sighandler = SignalHandler::new()?; // Start catching signals before we hold the backup lock
    let mut root = root::open_root(&b2, &mut roots, &path).await?;
    let arc_root = Arc::new(root.clone());

    let restore_fut = restore_one_root(config, target, b2, arc_root);
    let result = sighandler.interruptible(restore_fut).await;

    root.unlock().await?;
    result
}

pub async fn restore_one_root(
    config: &Config,
    target: PathBuf,
    mut b2: B2,
    root: Arc<root::BackupRoot>,
) -> BoxResult<()> {
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
            FileDiff {
                local,
                remote: Some(rfile),
            } => {
                if let Some(lfile) = local {
                    diff_progress.println(format!(
                        "LOCAL: {}, REMOTE: {}",
                        &lfile.full_path_hash, &rfile.full_path_hash
                    ));
                    if lfile.last_modified >= rfile.last_modified {
                        continue;
                    }
                }
                num_download_actions += 1;
                action_runtime.spawn(action::download(
                    rate_limiter.clone(),
                    download_progress.clone(),
                    b2.clone(),
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

    action_runtime.shutdown_on_idle().await;
    download_progress.finish();
    progress.join();

    if progress.is_complete() {
        Ok(())
    } else {
        Err(From::from(format!(
            "Couldn't complete all operations, {} error(s)",
            progress.errors_count()
        )))
    }
}
