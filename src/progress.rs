use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressFinish, ProgressStyle};
use std::sync::Arc;

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
        }
    }

    fn create_progress_bar(bar_type: ProgressType, verbose: bool) -> ProgressHandler {
        let progress_bar = ProgressBar::with_draw_target(None, ProgressDrawTarget::hidden())
            .with_style(
                ProgressStyle::default_bar()
                    .template(bar_type.style_template())
                    .unwrap()
                    .progress_chars("=> "),
            )
            .with_finish(ProgressFinish::Abandon);
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
}

impl Drop for Progress {
    fn drop(&mut self) {
        self.diff_progress.finish();
        self.cleanup_progress.finish();
        self.upload_progress.finish();
        self.download_progress.finish();
        self.delete_progress.finish();
    }
}
