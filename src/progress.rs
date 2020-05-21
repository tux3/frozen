use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use std::cell::Cell;
use std::sync::Arc;
use std::thread::JoinHandle;

mod progress_handler;
pub use progress_handler::*;

#[derive(Copy, Clone)]
pub enum ProgressType {
    Diff,
    Cleanup,
    Upload,
    Download,
    Delete,
}

impl ProgressType {
    fn style_template(&self) -> &str {
        match self {
            ProgressType::Diff => "Diff folder [{bar:50}]",
            ProgressType::Cleanup => "Cleanup [{bar:50}] {pos}/{len}",
            ProgressType::Upload => "Upload file [{bar:50.green}] {pos}/{len}",
            ProgressType::Download => "Download file [{bar:50.blue}] {pos}/{len}",
            ProgressType::Delete => "Delete file [{bar:50.red}] {pos}/{len}",
        }
    }
}

pub struct Progress {
    multi_progress: Arc<MultiProgress>,
    diff_progress: ProgressHandler,
    cleanup_progress: ProgressHandler,
    upload_progress: ProgressHandler,
    download_progress: ProgressHandler,
    delete_progress: ProgressHandler,
    progress_thread: Cell<Option<JoinHandle<()>>>,
}

impl Progress {
    pub fn new(verbose: bool) -> Self {
        Self {
            multi_progress: Arc::new(MultiProgress::with_draw_target(ProgressDrawTarget::stdout())),
            diff_progress: Self::create_progress_bar(ProgressType::Diff, verbose),
            cleanup_progress: Self::create_progress_bar(ProgressType::Cleanup, verbose),
            upload_progress: Self::create_progress_bar(ProgressType::Upload, verbose),
            download_progress: Self::create_progress_bar(ProgressType::Download, verbose),
            delete_progress: Self::create_progress_bar(ProgressType::Delete, verbose),
            progress_thread: Cell::new(None),
        }
    }

    fn create_progress_bar(bar_type: ProgressType, verbose: bool) -> ProgressHandler {
        let progress_bar = ProgressBar::with_draw_target(1, ProgressDrawTarget::hidden()).with_style(
            ProgressStyle::default_bar()
                .template(bar_type.style_template())
                .progress_chars("=> "),
        );
        ProgressHandler::new(progress_bar, verbose)
    }

    /// Returns a handler to report progress with
    pub fn get_progress_handler(&self, bar_type: ProgressType) -> &ProgressHandler {
        match bar_type {
            ProgressType::Diff => &self.diff_progress,
            ProgressType::Cleanup => &self.cleanup_progress,
            ProgressType::Upload => &self.upload_progress,
            ProgressType::Download => &self.download_progress,
            ProgressType::Delete => &self.delete_progress,
        }
    }

    /// Displays the progress bar iff there are any action to be done
    pub fn show_progress_bar(&self, bar_type: ProgressType, num_to_do: usize) -> ProgressHandler {
        let bar_handler = self.get_progress_handler(bar_type).clone();
        if num_to_do == 0 {
            return bar_handler;
        }

        bar_handler.set_length(num_to_do);
        self.multi_progress.add(bar_handler.progress_bar.clone());

        let mut progress_thread = self.progress_thread.take();
        if progress_thread.is_none() {
            let multi_progress_clone = self.multi_progress.clone();
            progress_thread = Some(std::thread::spawn(move || {
                multi_progress_clone.join().expect("Failed to join MultiProgress");
            }));
        };
        self.progress_thread.set(progress_thread);

        bar_handler.progress_bar.tick();
        bar_handler
    }

    /// Returns the number of progress errors logged since the output started
    pub fn errors_count(&self) -> usize {
        self.diff_progress.errors_count()
            + self.cleanup_progress.errors_count()
            + self.upload_progress.errors_count()
            + self.download_progress.errors_count()
            + self.delete_progress.errors_count()
    }

    /// Returns whether all operations have been completed successfully
    pub fn is_complete(&self) -> bool {
        self.diff_progress.is_complete()
            && self.cleanup_progress.is_complete()
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
        self.diff_progress.finish();
        self.cleanup_progress.finish();
        self.upload_progress.finish();
        self.download_progress.finish();
        self.delete_progress.finish();
        self.join();

        // After we drop multi_progress, our progress_handlers must stop drawing to it or they'll panic on unwrap
        self.diff_progress
            .progress_bar
            .set_draw_target(ProgressDrawTarget::hidden());
        self.cleanup_progress
            .progress_bar
            .set_draw_target(ProgressDrawTarget::hidden());
        self.upload_progress
            .progress_bar
            .set_draw_target(ProgressDrawTarget::hidden());
        self.download_progress
            .progress_bar
            .set_draw_target(ProgressDrawTarget::hidden());
        self.delete_progress
            .progress_bar
            .set_draw_target(ProgressDrawTarget::hidden());
    }
}
