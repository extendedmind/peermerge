use chacha20poly1305::{
    aead::{Aead, KeyInit, OsRng},
    XChaCha20Poly1305, XNonce,
};
use std::convert::TryInto;
use std::fmt::Debug;

pub(crate) struct EntryCipher {
    cipher: XChaCha20Poly1305,
}

impl Debug for EntryCipher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "EntryCipher")
    }
}

impl EntryCipher {
    pub(crate) fn from_encryption_key(key: &[u8]) -> Self {
        EntryCipher {
            cipher: XChaCha20Poly1305::new_from_slice(key).unwrap(),
        }
    }

    pub(crate) fn from_generated_key() -> (Self, Vec<u8>) {
        let key = XChaCha20Poly1305::generate_key(&mut OsRng);
        let entry_cipher = EntryCipher {
            cipher: XChaCha20Poly1305::new(&key),
        };
        (entry_cipher, key.to_vec())
    }

    pub(crate) fn encrypt(&self, public_key: &[u8; 32], index: u64, data: &[u8]) -> Vec<u8> {
        let nonce = generate_nonce(public_key, index);
        self.cipher.encrypt(&nonce, data).unwrap()
    }
}

pub(crate) fn keys_to_doc_url(public_key: &[u8; 32], encryption_key: Option<Vec<u8>>) -> String {
    let encoded_public_key = hex::encode(public_key);
    let postfix: String = if let Some(encryption_key) = encryption_key {
        let nonce = generate_nonce(&public_key, 0);
        let cipher = XChaCha20Poly1305::new_from_slice(&encryption_key).unwrap();
        let enc = cipher.encrypt(&nonce, public_key.as_slice()).unwrap();
        let encoded_enc = hex::encode(enc);
        format!("?enc={}", encoded_enc)
    } else {
        "".to_string()
    };
    format!("hypermerge:/{}{}", encoded_public_key, postfix)
}

pub(crate) fn doc_url_to_public_key(
    doc_url: &str,
    encryption_key_to_validate: Option<Vec<u8>>,
) -> ([u8; 32], bool) {
    let public_key_hex = doc_url[12..76].to_string();
    let public_key = hex::decode(public_key_hex).unwrap().try_into().unwrap();
    let encrypted = if doc_url.len() > 81 && doc_url[76..81] == "?enc=".to_string() {
        // The url indicates that its encrypted. If an encryption key is given, test that it works.
        if let Some(encryption_key) = encryption_key_to_validate {
            let enc: Vec<u8> = hex::decode(doc_url[81..].to_string()).unwrap();
            let nonce = generate_nonce(&public_key, 0);
            let cipher = XChaCha20Poly1305::new_from_slice(&encryption_key).unwrap();
            let reference_public_key: [u8; 32] =
                cipher.decrypt(&nonce, &*enc).unwrap().try_into().unwrap();
            if public_key != reference_public_key {
                panic!("Invalid encryption key");
            }
        }
        true
    } else {
        false
    };
    (public_key, encrypted)
}

fn generate_nonce(public_key: &[u8; 32], index: u64) -> XNonce {
    let mut nonce = public_key[..16].to_vec();
    nonce.extend(index.to_le_bytes());
    XNonce::clone_from_slice(&nonce)
}
