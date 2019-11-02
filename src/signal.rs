use std::sync::atomic::{AtomicBool, Ordering};
use crate::box_result::BoxResult;

static SIGNAL_FLAG: AtomicBool = AtomicBool::new(false);

pub fn setup_signal_handler() {
    ctrlc::set_handler(|| {
        SIGNAL_FLAG.store(true, Ordering::Release);
    }).expect("Error setting Ctrl-C handler");
}

pub fn caught_signal() -> bool {
    SIGNAL_FLAG.load(Ordering::Acquire)
}

pub fn err_on_signal() -> BoxResult<()> {
    if caught_signal() {
        Err(From::from("Interrupted by signal"))
    } else {
        Ok(())
    }
}