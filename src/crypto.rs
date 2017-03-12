use std::vec::Vec;
use std::error::Error;
use sodiumoxide::crypto::hash;
use sodiumoxide::crypto::pwhash;
use sodiumoxide::crypto::secretbox;
use rustc_serialize::hex::{ToHex, FromHex};
use rustc_serialize::base64::{self, ToBase64, FromBase64};
use bincode;
use bincode::rustc_serialize::{encode, decode};
use sha1::Sha1;
use libsodium_sys;

pub use sodiumoxide::crypto::secretbox::Key;

const BASE64_CONFIG: base64::Config = base64::Config {
    char_set: base64::CharacterSet::UrlSafe,
    newline: base64::Newline::LF,
    pad: true,
    line_length: None,
};

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

pub fn encrypt(plain: &[u8], &Key(ref key): &Key) -> Vec<u8> {
    let nonce = secretbox::gen_nonce();
    let secretbox::Nonce(nonceb) = nonce;

    let clen = plain.len() + secretbox::MACBYTES;
    let mut cipher = Vec::with_capacity(clen + secretbox::NONCEBYTES);
    unsafe {
        cipher.set_len(clen);
        libsodium_sys::crypto_secretbox_easy(cipher.as_mut_ptr(),
                                   plain.as_ptr(),
                                   plain.len() as u64,
                                   &nonceb,
                                   key);
    }

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

pub fn encode_meta(key: &Key, filename: &str, time: u64, is_symlink: bool) -> String {
    let data = (filename, time, is_symlink);
    let encoded = encode(&data, bincode::SizeLimit::Infinite).unwrap();
    encrypt(&encoded, key).to_base64(BASE64_CONFIG)
}

pub fn decode_meta(key: &Key, meta_enc: &str) -> Result<(String, u64, bool), Box<Error>> {
    let data = if let Ok(decoded) = meta_enc.from_hex() {
        // Old hex format (have to try it first!)
        decoded
    } else {
        meta_enc.from_base64()?
    };
    let plain = decrypt(&data, key)?;
    let meta = decode(&plain[..]);
    if meta.is_ok() {
        Ok(meta.unwrap())
    } else {
        // Old format
        let (filename, time): (String, u64) = decode(&plain[..]).unwrap();
        Ok((filename, time, false))
    }
}