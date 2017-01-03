use std::error::Error;
use config::Config;
use b2api;

pub fn backup(config: &Config, path: &String) -> Result<(), Box<Error>> {
    let b2 = &b2api::authenticate(config)?;
    /** TODO: Store the public key of a keypair in the config file
     *  Ask for a password on config-generation, every time we read the config
     *  ask for a password to generate the private key matching the stored public key.
     *  The Config struct should have a complete keypair, but only store the public key.
     *
     *   Create a data::root module that represents the backup roots and public key
     *   and can interact with the b2api module to read/write with B2.
     *   At first make a fetch_roots(b2: &B2) -> BackupRoots which downloads the root file
     *   and parses it in encrypted format.
     *   Then we can have functions to work with the roots, like checking if a root
     *   folder exists, or adding a new root folder to the roots, or getting the root Directory
     *   of the root, which is a Directory structure, which contains file names and can be used
     *   to upload/download files or backup/restore recursively.
     */
    let root_file_data = b2api::download_file(b2, "backup_root")?;

    panic!("Backup not implemented yet!");
}
