use std::io;
use std::io::prelude::*;
use std::error::Error;
use std::time::{SystemTime, UNIX_EPOCH};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use rpassword;
use ctrlc;

fn prompt_readline() -> String {
    let mut input = String::new();
    io::stdin().read_line(&mut input).unwrap();
    let len = input.len()-1;
    if len > 0 {
        input.truncate(len);
    }
    input
}

pub fn prompt(msg: &str) -> String {
    print!("{}: ", msg);
    io::stdout().flush().unwrap();
    prompt_readline()
}

pub fn prompt_password(msg: &str) -> String {
    print!("{}: ", msg);
    io::stdout().flush().unwrap();
    rpassword::read_password().unwrap_or_else(|_| prompt_readline())
}

pub fn prompt_yes_no(msg: &str) -> bool {
    loop {
        print!("{} (y/n): ", msg);
        io::stdout().flush().unwrap();
        let mut input = String::new();
        io::stdin().read_line(&mut input).unwrap();
        if input == "y\n" {
            return true;
        } else if input == "n\n" {
            return false;
        } else {
            println!("Please enter 'y' or 'n' at the prompt")
        }
    }
}

pub fn to_timestamp(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH).unwrap().as_secs()
}

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