use std::sync::Arc;
use std::path::Path;
use clap::ArgMatches;
use crate::config::Config;
use crate::data::{root, paths::path_from_arg};
use crate::net::b2::B2;
use crate::action::{self, scoped_runtime};
use crate::net::rate_limiter::RateLimiter;
use crate::box_result::BoxResult;
use crate::progress::{Progress, ProgressType};
use crate::signal::SignalHandler;

pub async fn delete(config: &Config, args: &ArgMatches<'_>) -> BoxResult<()> {
    let path = path_from_arg(args, "target")?;
    let keys = config.get_app_keys()?;

    println!("Connecting to Backblaze B2");
    let mut b2 = B2::authenticate(config, &keys).await?;

    println!("Downloading backup metadata");
    let mut roots = root::fetch_roots(&b2).await?;

    println!("Deleting backup folder {}", path.display());
    let mut sighandler = SignalHandler::new()?; // Start catching signals before we hold the backup lock
    let mut root = root::open_root(&b2, &mut roots, &path).await?;
    let result = sighandler.interruptible(delete_one_root(config, &mut b2, &path, &root, &mut roots)).await;

    root.unlock().await?;
    result
}

async fn delete_one_root(config: &Config, b2: &mut B2, path: &Path,
                root: &root::BackupRoot, roots: &mut Vec<root::BackupRoot>) -> BoxResult<()> {
    // We can't start removing files without pessimizing the DirDB (or removing it entirely!)
    let dirdb_path = "dirdb/".to_string()+&root.path_hash;
    b2.hide_file(&dirdb_path).await?;

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

    // This is scoped to shutdown when we're done running actions on the folder
    let action_runtime = scoped_runtime::Builder::new()
        .name_prefix("delete-")
        .pool_size(num_cpus::get().max(1))
        .build()?;

    let rate_limiter = Arc::new(RateLimiter::new(&config));
    for rfile in rfiles {
        action_runtime.spawn(action::delete(rate_limiter.clone(), delete_progress.clone(), b2.clone(), rfile))?;
    }
    action_runtime.shutdown_on_idle().await;
    delete_progress.finish();
    progress.join();

    println!("Deleting backup root");
    root::delete_root(b2, roots, &path).await?;

    if progress.is_complete() {
        Ok(())
    } else {
        Err(From::from(format!("Couldn't complete all operations, {} error(s)", progress.errors_count())))
    }
}