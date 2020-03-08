use std::io::{stdin, stdout, Write};

fn prompt_readline() -> String {
    let mut input = String::new();
    stdin().read_line(&mut input).unwrap();
    let len = input.len() - 1;
    if len > 0 {
        input.truncate(len);
    }
    input
}

pub fn prompt(msg: &str) -> String {
    print!("{}: ", msg);
    stdout().flush().unwrap();
    prompt_readline()
}

pub fn prompt_password(msg: &str) -> String {
    print!("{}: ", msg);
    stdout().flush().unwrap();
    rpassword::read_password().unwrap_or_else(|_| prompt_readline())
}

pub fn prompt_yes_no(msg: &str) -> bool {
    loop {
        print!("{} (y/n): ", msg);
        stdout().flush().unwrap();
        let mut input = String::new();
        stdin().read_line(&mut input).unwrap();
        if input == "y\n" {
            return true;
        } else if input == "n\n" {
            return false;
        } else {
            println!("Please enter 'y' or 'n' at the prompt")
        }
    }
}
