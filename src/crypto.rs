use std::vec::Vec;
use sodiumoxide::crypto::hash;
use sodiumoxide::crypto::pwhash;
use sodiumoxide::crypto::secretbox;

pub use sodiumoxide::crypto::secretbox::Key;

/// Derives a secret key from the user password and the account ID (used as a salt)
pub fn derive_key(pwd: &String, acc_id: &String) -> Key {
    let mut key = Key([0; secretbox::KEYBYTES]);
    let hash = hash::sha256::hash(&Vec::from(acc_id.as_str()));
    let salt = pwhash::Salt::from_slice(hash.as_ref()).unwrap();
    {
        let secretbox::Key(ref mut kb) = key;
        pwhash::derive_key(kb, pwd.as_ref(), &salt,
                           pwhash::OPSLIMIT_INTERACTIVE,
                           pwhash::MEMLIMIT_INTERACTIVE).unwrap();
    }
    return key;
}

pub fn encrypt(plaintext: &Vec<u8>, key: &Key) -> Vec<u8> {
    let nonce = secretbox::gen_nonce();
    let mut cipher = secretbox::seal(&plaintext, &nonce, &key);
    let secretbox::Nonce(nonceb) = nonce;
    cipher.extend_from_slice(&nonceb);
    return cipher;
}

pub fn decrypt(cipher: &Vec<u8>, key: &Key) -> Result<Vec<u8>, ()> {
    if cipher.len() < secretbox::NONCEBYTES {
        return Err(());
    }
    let nonce_index = cipher.len() - secretbox::NONCEBYTES;
    let mut nonce = [0; secretbox::NONCEBYTES];
    for (dst, src) in nonce.iter_mut().zip(cipher[nonce_index..].iter()) {
        *dst = *src;
    }

    secretbox::open(&cipher[0..nonce_index], &secretbox::Nonce(nonce), &key)
}