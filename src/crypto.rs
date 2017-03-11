use std::vec::Vec;
use std::error::Error;
use sodiumoxide::crypto::hash;
use sodiumoxide::crypto::pwhash;
use sodiumoxide::crypto::secretbox;
use rustc_serialize::hex::{ToHex, FromHex};
use bincode;
use bincode::rustc_serialize::{encode, decode};
use sha1::Sha1;

pub use sodiumoxide::crypto::secretbox::Key;

/// Derives a secret key from the user password and the account ID (used as a salt)
pub fn derive_key(pwd: &str, acc_id: &str) -> Key {
    let mut key = Key([0; secretbox::KEYBYTES]);
    let hash = hash::sha256::hash(&Vec::from(acc_id));
    let salt = pwhash::Salt::from_slice(hash.as_ref()).unwrap();
    {
        let secretbox::Key(ref mut kb) = key;
        pwhash::derive_key(kb, pwd.as_ref(), &salt,
                           pwhash::OPSLIMIT_INTERACTIVE,
                           pwhash::MEMLIMIT_INTERACTIVE).unwrap();
    }
    key
}

pub fn encrypt(plaintext: &[u8], key: &Key) -> Vec<u8> {
    let nonce = secretbox::gen_nonce();
    let mut cipher = secretbox::seal(plaintext, &nonce, key);
    let secretbox::Nonce(nonceb) = nonce;
    cipher.extend_from_slice(&nonceb);
    cipher
}

pub fn decrypt(cipher: &[u8], key: &Key) -> Result<Vec<u8>, Box<Error>> {
    if cipher.len() < secretbox::NONCEBYTES {
        return Err(From::from("Decryption failed, input too small"));
    }
    let nonce_index = cipher.len() - secretbox::NONCEBYTES;
    let mut nonce = [0; secretbox::NONCEBYTES];
    for (dst, src) in nonce.iter_mut().zip(cipher[nonce_index..].iter()) {
        *dst = *src;
    }

    let maybe_plain = secretbox::open(&cipher[0..nonce_index], &secretbox::Nonce(nonce), key);
    if maybe_plain.is_ok() {
        Ok(maybe_plain.unwrap())
    } else {
        Err(From::from("Decryption failed"))
    }
}

pub fn hash_path(secret: &str, key: &Key) -> String {
    let &Key(keydata) = key;
    let mut data = Vec::from(secret.as_bytes());
    data.extend_from_slice(&keydata);
    let hash = hash::sha256::hash(data.as_ref());
    hash.as_ref().to_hex()
}

pub fn sha1_string(data: &[u8]) -> String {
    let mut hash = Sha1::new();
    hash.update(data);
    hash.digest().to_string()
}

pub fn encode_meta(key: &Key, filename: &str, time: u64) -> String {
    let data = (filename, time);
    let encoded = encode(&data, bincode::SizeLimit::Infinite).unwrap();
    encrypt(&encoded, key).to_hex()
}

pub fn decode_meta(key: &Key, meta_enc: &str) -> Result<(String, u64), Box<Error>> {
    let plain = decrypt(&meta_enc.from_hex()?, key)?;
    Ok(decode(&plain[..]).unwrap())
}