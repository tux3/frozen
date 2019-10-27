use std::vec::Vec;
use std::error::Error;
use std::path::{PathBuf, Path};
use sodiumoxide::crypto::{hash, pwhash, secretbox};
use sodiumoxide::randombytes;
use libsodium_sys;
use bincode::{serialize, deserialize};
use data_encoding::{BASE64URL_NOPAD, HEXLOWER_PERMISSIVE};
use blake2::VarBlake2b;
use sha1::Sha1;
use digest::{Digest, Input, VariableOutput};

pub use sodiumoxide::crypto::secretbox::Key;

pub struct AppKeys {
    pub b2_key_id: String,
    pub b2_key: String,
    pub encryption_key: Key,
}

/// Derives a secret key from the user password and the salt
pub fn derive_key(pwd: &str, salt: &str) -> Key {
    let mut key = Key([0; secretbox::KEYBYTES]);
    let hash = hash::sha256::hash(&Vec::from(salt));
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
        // Safe because:
        // 1. We set the capacity >= clen
        // 2. crypto_secretbox_easy writes exactly clen
        libsodium_sys::crypto_secretbox_easy(cipher.as_mut_ptr(),
                                   plain.as_ptr(),
                                   plain.len() as u64,
                                   nonceb.as_ptr(),
                                   key.as_ptr());
        cipher.set_len(clen);
    }

    cipher.extend_from_slice(&nonceb);
    cipher
}

pub fn decrypt(cipher: &[u8], key: &Key) -> Result<Vec<u8>, Box<dyn Error>> {
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

pub fn raw_hash(data: &[u8], output_size: usize, output: &mut [u8]) -> Result<(), Box<dyn Error>> {
    let mut hasher = VarBlake2b::new(output_size)?;
    hasher.input(data);
    hasher.variable_result(|result| output.copy_from_slice(result));
    Ok(())
}

pub fn hash_path(secret_path: &Path, key: &Key) -> String {
    let &Key(keydata) = key;
    let mut hasher = VarBlake2b::new_keyed(&keydata, 20);
    hasher.input(serialize(secret_path).unwrap());
    BASE64URL_NOPAD.encode(&hasher.vec_result())
}

pub fn sha1_string(data: &[u8]) -> String {
    let mut hash = Sha1::default();
    <Sha1 as Input>::input(&mut hash, data);
    HEXLOWER_PERMISSIVE.encode(&hash.result())
}

pub fn randombytes(count: usize) -> Vec<u8> {
    randombytes::randombytes(count)
}

pub fn encode_meta(key: &Key, filename: &Path, time: u64, mode: u32, is_symlink: bool) -> String {
    let data = (filename, time, mode, is_symlink);
    let encoded = serialize(&data).unwrap();
    BASE64URL_NOPAD.encode(&encrypt(&encoded, key))
}

pub fn decode_meta(key: &Key, meta_enc: &str) -> Result<(PathBuf, u64, u32, bool), Box<dyn Error>> {
    let data = BASE64URL_NOPAD.decode(meta_enc.as_bytes())?;
    let plain = decrypt(&data, key)?;
    Ok(deserialize(&plain[..])?)
}
