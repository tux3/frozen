use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use indicatif::ProgressBar;

#[derive(Clone)]
pub struct ProgressHandler {
    pub(super) bar: ProgressBar,
    bar_len: Arc<AtomicUsize>,
    errors_count: Arc<AtomicUsize>,
}

impl ProgressHandler {
    pub(super) fn new(bar: ProgressBar) -> Self {
        Self {
            bar,
            bar_len: Arc::new(AtomicUsize::new(0)),
            errors_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub(super) fn set_length(&self, len: usize) {
        self.bar_len.store(len, Ordering::Release);
        self.bar.set_length(len as u64);
    }

    pub fn report_success(&self) {
        // TODO: Should print a message if verbose == true
        self.bar.inc(1);
    }

    pub fn report_error(&self, msg: impl AsRef<str>) {
        self.errors_count.fetch_add(1, Ordering::AcqRel);
        self.bar.println("Error: ".to_string()+msg.as_ref());
    }

    pub fn println(&self, msg: impl Into<String>) {
        self.bar.println(msg);
    }

    pub fn finish(&self) {
        self.bar.finish_at_current_pos();
    }

    /// Returns the number of progress errors logged since the output started
    pub fn errors_count(&self) -> usize {
        self.errors_count.load(Ordering::Acquire)
    }

    /// Returns whether all operations have been completed successfully
    pub fn is_complete(&self) -> bool {
        self.errors_count() == 0
            && self.bar.position() == self.bar_len.load(Ordering::Acquire) as u64
    }
}
