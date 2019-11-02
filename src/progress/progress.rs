use std::vec::Vec;
use std::io::{self, stdout, Write, Read, ErrorKind};
use std::error::Error;
use std::cmp;
use std::pin::Pin;
use std::thread::JoinHandle;
use std::cell::Cell;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use futures::task::Context;
use pretty_bytes::converter::convert;
use bytes::Bytes;
use futures::{stream::Stream, Poll};
use hyper::Chunk;
use indicatif::{MultiProgress, ProgressDrawTarget, ProgressBar, ProgressStyle};
use crate::progress::ProgressHandler;
use std::borrow::Borrow;

#[derive(Copy, Clone)]
pub enum ProgressType {
    Diff,
    Upload,
    Download,
    Delete,
}

impl ProgressType {
    fn style_template(&self) -> &str {
        match self {
            ProgressType::Diff => "Diff folder [{bar:50}]",
            ProgressType::Upload => "Upload file [{bar:50.blue}] {pos}/{len}",
            ProgressType::Download => "Download file [{bar:50.green}] {pos}/{len}",
            ProgressType::Delete => "Delete file [{bar:50.red}] {pos}/{len}",
        }
    }
}

pub struct Progress {
    multi_progress: Arc<MultiProgress>,
    diff_progress: ProgressHandler,
    upload_progress: ProgressHandler,
    download_progress: ProgressHandler,
    delete_progress: ProgressHandler,
    progress_thread: Cell<Option<JoinHandle<()>>>,
    verbose: bool,
}

impl Progress {
    pub fn new(verbose: bool) -> Self {
        let multi_progress = Arc::new(MultiProgress::with_draw_target(ProgressDrawTarget::stdout()));
        let diff_progress = Self::create_progress_bar(ProgressType::Diff, verbose);
        let upload_progress = Self::create_progress_bar(ProgressType::Upload, verbose);
        let download_progress = Self::create_progress_bar(ProgressType::Download, verbose);
        let delete_progress = Self::create_progress_bar(ProgressType::Delete, verbose);

        Self {
            multi_progress,
            diff_progress,
            upload_progress,
            download_progress,
            delete_progress,
            progress_thread: Cell::new(None),
            verbose,
        }
    }

    fn create_progress_bar(bar_type: ProgressType, verbose: bool) -> ProgressHandler {
        let bar = ProgressBar::with_draw_target(1, ProgressDrawTarget::hidden())
            .with_style(ProgressStyle::default_bar()
                .template(bar_type.style_template())
                .progress_chars("=> "));
        ProgressHandler::new(bar, verbose)
    }

    /// Returns a handler to report progress with
    pub fn get_progress_handler(&self, bar_type: ProgressType) -> &ProgressHandler {
        match bar_type {
            ProgressType::Diff => &self.diff_progress,
            ProgressType::Upload => &self.upload_progress,
            ProgressType::Download => &self.download_progress,
            ProgressType::Delete => &self.delete_progress,
        }
    }

    /// Displays the progress bar iff there are any action to be done
    pub fn show_progress_bar(&self, bar_type: ProgressType, num_to_do: usize) -> ProgressHandler {
        let bar = self.get_progress_handler(bar_type).clone();
        if num_to_do == 0 {
            return bar;
        }

        bar.set_length(num_to_do);
        self.multi_progress.add(bar.bar.clone());

        let mut progress_thread = self.progress_thread.take();
        if progress_thread.is_none() {
            let multi_progress_clone = self.multi_progress.clone();
            progress_thread = Some(std::thread::spawn(move || {
                multi_progress_clone.join().expect("Failed to join MultiProgress");
            }));
        };
        self.progress_thread.set(progress_thread);

        bar.bar.tick();
        bar
    }

    /// Returns the number of progress errors logged since the output started
    pub fn errors_count(&self) -> usize {
        self.diff_progress.errors_count()
            + self.upload_progress.errors_count()
            + self.download_progress.errors_count()
            + self.delete_progress.errors_count()
    }

    /// Returns whether all operations have been completed successfully
    pub fn is_complete(&self) -> bool {
        self.diff_progress.is_complete()
            && self.upload_progress.is_complete()
            && self.download_progress.is_complete()
            && self.delete_progress.is_complete()
    }

    /// Must only be called after all progress handles are finished, or will block forever
    /// After join returns, it is okay to print output directly again
    pub fn join(&self) {
        if let Some(progress_thread) = self.progress_thread.take() {
            progress_thread.join().expect("Failed to join progress thread");
        }
    }
}

impl Drop for Progress {
    fn drop(&mut self) {
        self.join();
    }
}
