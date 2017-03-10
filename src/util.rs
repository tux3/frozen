use std::io;
use std::io::prelude::*;
use std::time::{SystemTime, UNIX_EPOCH};
use rpassword;

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