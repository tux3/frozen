use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use indicatif::ProgressBar;

#[derive(Clone)]
pub struct ProgressHandler {
    pub(super) progress_bar: ProgressBar,
    bar_len: Arc<AtomicUsize>,
    errors_count: Arc<AtomicUsize>,
    verbose: bool,
}

impl ProgressHandler {
    pub(super) fn new(progress_bar: ProgressBar, verbose: bool) -> Self {
        Self {
            progress_bar,
            bar_len: Arc::new(AtomicUsize::new(0)),
            errors_count: Arc::new(AtomicUsize::new(0)),
            verbose,
        }
    }

    pub(super) fn set_length(&self, len: usize) {
        self.bar_len.store(len, Ordering::Release);
        self.progress_bar.set_length(len as u64);
    }

    pub fn report_success(&self) {
        self.progress_bar.inc(1);
    }

    pub fn report_error(&self, msg: impl AsRef<str>) {
        self.errors_count.fetch_add(1, Ordering::AcqRel);
        self.progress_bar.println("Error: ".to_string()+msg.as_ref());
    }

    pub fn println(&self, msg: impl Into<String>) {
        self.progress_bar.println(msg);
    }

    pub fn finish(&self) {
        self.progress_bar.finish_at_current_pos();
    }

    /// When true, it is okay to println() verbose progress information
    pub fn verbose(&self) -> bool {
        self.verbose
    }

    /// Returns the number of progress errors logged since the output started
    pub fn errors_count(&self) -> usize {
        self.errors_count.load(Ordering::Acquire)
    }

    /// Returns whether all operations have been completed successfully
    pub fn is_complete(&self) -> bool {
        self.errors_count() == 0
            && self.progress_bar.position() == self.bar_len.load(Ordering::Acquire) as u64
    }
}
