use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use ctrlc;

pub fn setup_signal_flag() -> Arc<AtomicBool> {
    let flag = Arc::new(AtomicBool::new(false));

    let r = flag.clone();
    ctrlc::set_handler(move || {
        r.store(true, Ordering::SeqCst);
    }).expect("Error setting Ctrl-C handler");

    flag
}

pub fn caught_signal(flag: &Arc<AtomicBool>) -> bool {
    flag.compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst).is_ok()
}

pub fn err_on_signal(flag: &Arc<AtomicBool>) -> Result<(), Box<Error>> {
    if caught_signal(flag) {
        Err(From::from("Interrupted by signal"))
    } else {
        Ok(())
    }
}