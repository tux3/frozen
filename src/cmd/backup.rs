use std::path::PathBuf;
use std::sync::Arc;
use clap::ArgMatches;
use futures::StreamExt;
use crate::action::{self, scoped_runtime};
use crate::net::rate_limiter::RateLimiter;
use crate::box_result::BoxResult;
use crate::config::Config;
use crate::net::b2;
use crate::data::root::{self, BackupRoot};
use crate::data::paths::path_from_arg;
use crate::dirdb::{DirDB, diff::DirDiff, diff::FileDiff};
use crate::progress::{Progress, ProgressType};
use crate::signal::SignalHandler;

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
    let mut sighandler = SignalHandler::new()?; // Start catching signals before we hold the backup lock
    let mut root = root::open_create_root(&b2, &mut roots, &target).await?;
    let arc_root = Arc::new(root.clone());

    let backup_fut = backup_one_root(config, args, path, b2, arc_root);
    let result = sighandler.interruptible(backup_fut).await;

    root.unlock().await?;
    result
}

pub async fn backup_one_root(config: &Config, args: &ArgMatches<'_>, path: PathBuf, mut b2: b2::B2, root: Arc<BackupRoot>) -> BoxResult<()> {
    println!("Starting diff");
    let progress = Progress::new(config.verbose);
    let diff_progress = progress.show_progress_bar(ProgressType::Diff, 4);
    let upload_progress = progress.get_progress_handler(ProgressType::Upload);
    let delete_progress = progress.get_progress_handler(ProgressType::Delete);

    b2.progress.replace(diff_progress.clone());
    let b2 = Arc::new(b2);

    // This is scoped to shutdown when we're done running actions on the backup folder
    let mut action_runtime = scoped_runtime::Builder::new()
        .name_prefix("backup-")
        .pool_size(num_cpus::get().max(1))
        .build()?;

    let dirdb_path = "dirdb/".to_string()+&root.path_hash;
    let remote_dirdb_fut = {
        let b2 = b2.clone();
        let dirdb_path = dirdb_path.clone();
        action_runtime.spawn_with_handle(async move {
            b2.download_file(&dirdb_path).await
        })?
    };

    let local_dirdb = Arc::new(DirDB::new_from_local(&path)?);
    diff_progress.report_success();

    let remote_dirdb = remote_dirdb_fut.await.ok().and_then(|data| {
        DirDB::new_from_packed(&data, &b2.key).ok()
    });

    let mut dir_diff = DirDiff::new(root.clone(), b2.clone(), local_dirdb.clone(), remote_dirdb)?;
    let path = Arc::new(path);
    diff_progress.report_success();

    diff_progress.println("Uploading pessimistic DirDB");
    let dirdb_data = dir_diff.get_pessimistic_dirdb_data(&b2.key)?;
    b2.upload_file_simple(&dirdb_path, dirdb_data).await?;
    diff_progress.report_success();

    diff_progress.println("Starting backup");
    let mut num_upload_actions = 0;
    let mut num_delete_actions = 0;
    let rate_limiter = Arc::new(RateLimiter::new(&config));
    let keep_existing = args.is_present("keep-existing");
    while let Some(item) = dir_diff.next().await {
        let item = item?;

        match item {
            FileDiff{local: Some(lfile), remote} => {
                if let Some(rfile) = remote {
                    if rfile.last_modified >= lfile.last_modified {
                        continue
                    }
                }
                num_upload_actions += 1;
                action_runtime.spawn(action::upload(rate_limiter.clone(), upload_progress.clone(),
                                                    root.clone(), b2.clone(), config.compression_level, path.clone(), lfile))?;
            },
            FileDiff{local: None, remote: Some(rfile)} => {
                if keep_existing {
                    continue
                }
                num_delete_actions += 1;
                action_runtime.spawn(action::delete(rate_limiter.clone(), delete_progress.clone(),
                                                    root.clone(), b2.clone(), rfile))?;
            },
            FileDiff{local: None, remote: None} => unreachable!()
        }
    };

    let upload_progress = progress.show_progress_bar(ProgressType::Upload, num_upload_actions);
    let delete_progress = progress.show_progress_bar(ProgressType::Delete, num_delete_actions);
    diff_progress.report_success();
    diff_progress.finish();

    let packed_local_dirdb = local_dirdb.to_packed(&b2.key)?;
    action_runtime.shutdown_on_idle().await;
    upload_progress.finish();
    delete_progress.finish();
    progress.join();

    if progress.is_complete() {
        println!("Uploading new DirDB");
        b2.upload_file_simple(&dirdb_path, packed_local_dirdb).await?;
        Ok(())
    } else {
        Err(From::from(format!("Couldn't complete all operations, {} error(s)", progress.errors_count())))
    }
}
