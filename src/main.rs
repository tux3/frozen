use std::env;
use std::process::exit;

mod cmd;

fn help_and_die(selfname: &String) {
    println!("Usage: {} command path", selfname);
    exit(1);
}

fn main() {
    let args:Vec<_> = env::args().collect();
    if args.len() < 3 {
        help_and_die(&args[0]);
    }

    match args[1].as_ref() {
        "sync" => cmd::sync(&args[2]),
        "restore" => cmd::restore(&args[2]),
        _ => help_and_die(&args[0]),
    }
}
