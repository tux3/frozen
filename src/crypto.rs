use base64::Engine;
use bincode::{deserialize, serialize};
use blake2::{Blake2bMac, Digest};
use data_encoding::{BASE64URL_NOPAD, HEXLOWER_PERMISSIVE};
use digest::generic_array::GenericArray;
use digest::{FixedOutput, Mac, Update};
use eyre::{bail, eyre, Result};
use sha1::Sha1;
use sodiumoxide::crypto::secretstream::{Header, Pull, Push, Stream as SecretStream};
use sodiumoxide::crypto::{hash, pwhash, secretbox};
use sodiumoxide::randombytes;
use std::path::{Path, PathBuf};
use std::vec::Vec;

pub use sodiumoxide::crypto::secretbox::Key;
pub use sodiumoxide::crypto::secretstream::Key as SecretStreamKey;

// TODO: Whenever the digest lib offers const generics we can remove these typenums...
const DIRNAME_PATH_HASH_LEN: usize = 8;
//const FILENAME_PATH_HASH_LEN: usize = 12;
type DirnamePathHashLenTypenum = digest::consts::U8;
type FilenamePathHashLenTypenum = digest::consts::U12;

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
        pwhash::derive_key(
            kb,
            pwd.as_ref(),
            &salt,
            pwhash::OPSLIMIT_INTERACTIVE,
            pwhash::MEMLIMIT_INTERACTIVE,
        )
        .unwrap();
    }
    key
}

pub fn create_secretstream(Key(key): &Key) -> (SecretStream<Push>, Header) {
    let secretstream_key = SecretStreamKey(key.to_owned());
    SecretStream::init_push(&secretstream_key).unwrap()
}

pub fn open_secretstream(header: &[u8], Key(key): &Key) -> SecretStream<Pull> {
    let secretstream_key = SecretStreamKey(key.to_owned());
    let header = Header::from_slice(header).expect("Invalid secretstream header size");
    SecretStream::init_pull(&header, &secretstream_key).unwrap()
}

pub fn encrypt(plain: &[u8], Key(key): &Key) -> Vec<u8> {
    let nonce = secretbox::gen_nonce();
    let secretbox::Nonce(nonceb) = nonce;

    let clen = plain.len() + secretbox::MACBYTES;
    let mut cipher = Vec::with_capacity(clen + secretbox::NONCEBYTES);
    unsafe {
        // Safe because:
        // 1. We set the capacity >= clen
        // 2. crypto_secretbox_easy writes exactly clen
        libsodium_sys::crypto_secretbox_easy(
            cipher.as_mut_ptr(),
            plain.as_ptr(),
            plain.len() as u64,
            nonceb.as_ptr(),
            key.as_ptr(),
        );
        cipher.set_len(clen);
    }

    cipher.extend_from_slice(&nonceb);
    cipher
}

pub fn decrypt(cipher: &[u8], key: &Key) -> Result<Vec<u8>> {
    if cipher.len() < secretbox::NONCEBYTES {
        bail!("Decryption failed, input too small");
    }
    let nonce_index = cipher.len() - secretbox::NONCEBYTES;
    let mut nonce = [0; secretbox::NONCEBYTES];
    for (dst, src) in nonce.iter_mut().zip(cipher[nonce_index..].iter()) {
        *dst = *src;
    }

    secretbox::open(&cipher[0..nonce_index], &secretbox::Nonce(nonce), key).map_err(|()| eyre!("Decryption failed"))
}

pub fn hash_path_dir_into(
    dir_path_hash: &str,
    secret_dirname: &[u8],
    key: &Key,
    out: &mut [u8; DIRNAME_PATH_HASH_LEN],
) {
    let &Key(keydata) = key;
    let mut hasher = Blake2bMac::<DirnamePathHashLenTypenum>::new_with_salt_and_personal(&keydata, &[], &[]).unwrap();
    Mac::update(&mut hasher, dir_path_hash.as_bytes());
    Mac::update(&mut hasher, secret_dirname);
    hasher.finalize_into(GenericArray::from_mut_slice(out));
}

pub fn hash_path_filename_into(parent_hash: &[u8], secret_filename: &[u8], key: &Key, out: &mut String) {
    let &Key(keydata) = key;
    let mut hasher = Blake2bMac::<FilenamePathHashLenTypenum>::new_with_salt_and_personal(&keydata, &[], &[]).unwrap();
    Mac::update(&mut hasher, parent_hash);
    Mac::update(&mut hasher, secret_filename);
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode_string(hasher.finalize().into_bytes(), out);
}

pub fn hash_path_root(secret_root_path: &Path, key: &Key) -> String {
    let &Key(keydata) = key;
    let mut hasher = Blake2bMac::<DirnamePathHashLenTypenum>::new_with_salt_and_personal(&keydata, &[], &[]).unwrap();
    Mac::update(&mut hasher, &serialize(secret_root_path).unwrap());
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(hasher.finalize().into_bytes())
}

pub fn sha1_string(data: &[u8]) -> String {
    let mut hash = Sha1::default();
    <Sha1 as Update>::update(&mut hash, data);
    HEXLOWER_PERMISSIVE.encode(&hash.finalize())
}

pub fn randombytes(count: usize) -> Vec<u8> {
    randombytes::randombytes(count)
}

pub fn encode_meta(key: &Key, filename: &Path, time: u64, mode: u32, is_symlink: bool) -> String {
    let data = (filename, time, mode, is_symlink);
    let encoded = serialize(&data).unwrap();
    BASE64URL_NOPAD.encode(&encrypt(&encoded, key))
}

pub fn decode_meta(key: &Key, meta_enc: &str) -> Result<(PathBuf, u64, u32, bool)> {
    let data = BASE64URL_NOPAD.decode(meta_enc.as_bytes())?;
    let plain = decrypt(&data, key)?;
    Ok(deserialize(&plain[..])?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sodiumoxide::crypto::secretstream::ABYTES;

    #[test]
    fn derive_key_depends_on_salt() {
        let a = derive_key("pass", "a");
        let b = derive_key("pass", "b");
        assert_ne!(a, b);
    }

    #[test]
    fn derive_key_depends_on_pass() {
        let a = derive_key("a", "salt");
        let b = derive_key("b", "salt");
        assert_ne!(a, b);
    }

    #[test]
    fn derive_key_is_deterministic() {
        let a = derive_key("x", "salt");
        let b = derive_key("x", "salt");
        assert_eq!(a, b);
    }

    #[test]
    fn metadata_roundtrip() {
        let key = derive_key("pass", "salt");
        let filename = PathBuf::from("/foo");
        let time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let mode = 0o755;
        let is_symlink = true;

        let meta = encode_meta(&key, &filename, time, mode, is_symlink);
        let (dec_filename, dec_time, dec_mode, dec_is_symlink) = decode_meta(&key, &meta).unwrap();
        assert_eq!(filename, dec_filename);
        assert_eq!(time, dec_time);
        assert_eq!(mode, dec_mode);
        assert_eq!(is_symlink, dec_is_symlink);
    }

    #[test]
    fn secretstream_roundtrip() {
        use sodiumoxide::crypto::secretstream::Tag;

        let msg1 = "some message 1";
        let msg2 = "other message";

        // initialize encrypt secret stream
        let key = derive_key("test", "salt");
        let (mut enc_stream, header) = create_secretstream(&key);

        let ciphertext1 = enc_stream.push(msg1.as_bytes(), None, Tag::Push).unwrap();
        let ciphertext2 = enc_stream.push(msg2.as_bytes(), None, Tag::Message).unwrap();
        let ciphertext_final = enc_stream.finalize(None).unwrap();
        assert_eq!(ciphertext1.len(), msg1.len() + ABYTES);

        // initialize decrypt secret stream
        let mut dec_stream = open_secretstream(header.as_ref(), &key);

        // decrypt first message.
        assert!(!dec_stream.is_finalized());
        let (decrypted1, tag1) = dec_stream.pull(&ciphertext1, None).unwrap();
        assert_eq!(tag1, Tag::Push);
        assert_eq!(msg1.as_bytes(), &decrypted1[..]);

        // decrypt second message.
        assert!(!dec_stream.is_finalized());
        let (decrypted2, tag2) = dec_stream.pull(&ciphertext2, None).unwrap();
        assert_eq!(tag2, Tag::Message);
        assert_eq!(msg2.as_bytes(), &decrypted2[..]);

        // decrypt final message.
        assert!(!dec_stream.is_finalized());
        let (msg_final, tag_final) = dec_stream.pull(&ciphertext_final, None).unwrap();
        assert_eq!(tag_final, Tag::Final);
        assert!(msg_final.is_empty());
        assert!(dec_stream.is_finalized());
    }
}
