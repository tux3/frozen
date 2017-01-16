use std::error::Error;
use config::Config;
use data::root;
use b2api;

pub fn backup(config: &Config, path: &String) -> Result<(), Box<Error>> {
    println!("Connecting to Backblaze B2");
    let mut b2 = &mut b2api::authenticate(config)?;

    println!("Downloading backup metadata");
    let mut roots = root::fetch_roots(b2);

    println!("Opening backup folder {}", path);
    let root = root::open_root(b2, &mut roots, path)?;
    println!("Found {} roots", roots.len());
    println!("Opened root {} hash {}", root.path, root.path_hash);



    panic!("Backup not implemented yet!");
}
