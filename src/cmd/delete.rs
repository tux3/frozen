use crate::action;
use crate::box_result::BoxResult;
use crate::config::Config;
use crate::data::{paths::path_from_arg, root};
use crate::net::b2::{FileListDepth, B2};
use crate::net::rate_limiter::RateLimiter;
use crate::progress::{Progress, ProgressType};
use crate::signal::interruptible;
use clap::ArgMatches;
use futures::stream::{FuturesUnordered, StreamExt};
use futures::task::SpawnExt;
use std::path::Path;
use std::sync::Arc;

pub async fn delete(config: &Config, args: &ArgMatches<'_>) -> BoxResult<()> {
    let path = path_from_arg(args, "target")?;
    let keys = config.get_app_keys()?;

    println!("Connecting to Backblaze B2");
    let mut b2 = B2::authenticate(config, &keys).await?;

    println!("Downloading backup metadata");
    let mut roots = root::fetch_roots(&b2).await?;

    println!("Deleting backup folder {}", path.display());
    let mut root = root::open_root(&b2, &mut roots, &path).await?;
    let result = interruptible(delete_one_root(config, &mut b2, &path, &root, &mut roots)).await;

    root.unlock().await?;
    result
}

async fn delete_one_root(
    config: &Config,
    b2: &mut B2,
    path: &Path,
    root: &root::BackupRoot,
    roots: &mut Vec<root::BackupRoot>,
) -> BoxResult<()> {
    // We can't start removing files without pessimizing the DirDB (or removing it entirely!)
    let dirdb_path = "dirdb/".to_string() + &root.path_hash;
    if let err @ Err(_) = b2.hide_file(&dirdb_path).await {
        // If the dirdb doesn't actually exist (or is already hidden), we can continue safely
        if !b2
            .list_remote_files(&dirdb_path, FileListDepth::Shallow)
            .await?
            .is_empty()
        {
            return err;
        }
    }

    println!("Listing remote files");
    let rfiles = root.list_remote_files(b2).await?;

    // Give it some time to commit the hide before listing versions (best effort)
    let dirdb_versions = b2.list_remote_file_versions(&dirdb_path).await?;
    println!("Deleting {} versions of the DirDB", dirdb_versions.len());
    for dirdb_version in dirdb_versions.iter().rev() {
        b2.delete_file_version(&dirdb_version).await?;
    }

    let progress = Progress::new(config.verbose);
    let delete_progress = progress.show_progress_bar(ProgressType::Delete, rfiles.len());
    b2.progress.replace(delete_progress.clone());

    // Lets us wait for all backup actions to complete
    let action_futs = FuturesUnordered::new();

    let rate_limiter = Arc::new(RateLimiter::new(&config));
    for rfile in rfiles {
        action_futs.spawn(action::delete(
            rate_limiter.clone(),
            delete_progress.clone(),
            b2.clone(),
            rfile,
        ))?;
    }
    action_futs.for_each(|()| futures::future::ready(())).await;
    delete_progress.finish();
    progress.join();

    println!("Deleting backup root");
    root::delete_root(b2, roots, &path).await?;

    if progress.is_complete() {
        Ok(())
    } else {
        Err(From::from(format!(
            "Couldn't complete all operations, {} error(s)",
            progress.errors_count()
        )))
    }
}
