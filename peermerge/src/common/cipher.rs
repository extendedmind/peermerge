use chacha20poly1305::{
    aead::{Aead, KeyInit, OsRng},
    XChaCha20Poly1305, XNonce,
};
use std::convert::TryInto;
use std::fmt::Debug;

use crate::DocumentId;

const DOC_URL_PREFIX: &str = "peermerge:/";
const DOC_URL_VERSION: u8 = 1;
const DOC_URL_HYPERCORE: u8 = 1;

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

    pub(crate) fn decrypt(&self, public_key: &[u8; 32], index: u64, data: &[u8]) -> Vec<u8> {
        let nonce = generate_nonce(public_key, index);
        self.cipher.decrypt(&nonce, data).unwrap()
    }
}

pub(crate) fn keys_to_doc_url(public_key: &[u8; 32], encryption_key: &Option<Vec<u8>>) -> String {
    let mut domain: Vec<u8> = Vec::with_capacity(32 + 2);
    domain.push(DOC_URL_VERSION);
    domain.push(DOC_URL_HYPERCORE);
    domain.extend(public_key);
    let encoded_domain = data_encoding::BASE32_NOPAD.encode(&domain);
    let postfix: String = if let Some(encryption_key) = encryption_key {
        let nonce = generate_nonce(&public_key, 0);
        let cipher = XChaCha20Poly1305::new_from_slice(&encryption_key).unwrap();
        let enc = cipher.encrypt(&nonce, public_key.as_slice()).unwrap();
        let encoded_enc = data_encoding::BASE32_NOPAD.encode(&enc);
        format!("?enc={}", encoded_enc)
    } else {
        "".to_string()
    };
    format!("peermerge:/{}{}", encoded_domain, postfix)
}

pub(crate) fn doc_url_to_public_key(
    doc_url: &str,
    encryption_key_to_validate: &Option<Vec<u8>>,
) -> ([u8; 32], bool) {
    let (_, _, public_key_vec, enc) = decode_domain(doc_url);
    let public_key = public_key_vec.try_into().unwrap();
    let encrypted = enc.is_some();
    if let Some(enc) = enc {
        // The url indicates that its encrypted. If an encryption key is given, test that it works.
        if let Some(encryption_key) = encryption_key_to_validate {
            let nonce = generate_nonce(&public_key, 0);
            let cipher = XChaCha20Poly1305::new_from_slice(&encryption_key).unwrap();
            let reference_public_key: [u8; 32] =
                cipher.decrypt(&nonce, &*enc).unwrap().try_into().unwrap();
            if public_key != reference_public_key {
                panic!("Invalid encryption key");
            }
        }
    }
    (public_key, encrypted)
}

pub(crate) fn encode_document_id(document_id: &DocumentId) -> String {
    data_encoding::BASE32_NOPAD.encode(document_id)
}

pub fn doc_url_encrypted(doc_url: &str) -> bool {
    get_domain_end_and_enc_start_end(doc_url).1.is_some()
}

fn decode_domain(doc_url: &str) -> (u8, u8, Vec<u8>, Option<Vec<u8>>) {
    assert_eq!(&doc_url[..DOC_URL_PREFIX.len()], DOC_URL_PREFIX);
    let (domain_end, enc_start_end) = get_domain_end_and_enc_start_end(doc_url);
    let enc = if let Some((enc_start, enc_end)) = enc_start_end {
        let enc_base32 = doc_url[enc_start..enc_end].as_bytes();
        let mut enc = vec![
            0;
            data_encoding::BASE32_NOPAD
                .decode_len(enc_base32.len())
                .unwrap()
        ];
        data_encoding::BASE32_NOPAD
            .decode_mut(enc_base32, &mut enc)
            .unwrap();
        Some(enc)
    } else {
        None
    };
    let domain_base32 = doc_url[DOC_URL_PREFIX.len()..domain_end].as_bytes();
    let mut domain = vec![
        0;
        data_encoding::BASE32_NOPAD
            .decode_len(domain_base32.len())
            .unwrap()
    ];
    let decoded_len = data_encoding::BASE32_NOPAD
        .decode_mut(domain_base32, &mut domain)
        .unwrap();
    assert_eq!(domain[0], DOC_URL_VERSION);
    assert_eq!(domain[1], DOC_URL_HYPERCORE);
    assert_eq!(decoded_len, 32 + 2);
    (domain[0], domain[1], domain[2..].to_vec(), enc)
}

fn get_domain_end_and_enc_start_end(doc_url: &str) -> (usize, Option<(usize, usize)>) {
    if let Some(query_param_index) = doc_url.find('?') {
        let enc_start_end = if doc_url.len() > query_param_index + 5
            && &doc_url[query_param_index..query_param_index + 5] == "?enc="
        {
            let enc_start = query_param_index + 5;
            let enc_end = if let Some(next_param_index) = doc_url[enc_start..].find('&') {
                enc_start + next_param_index
            } else {
                doc_url.len()
            };
            Some((enc_start, enc_end))
        } else {
            None
        };
        (query_param_index, enc_start_end)
    } else {
        (doc_url.len(), None)
    }
}

fn generate_nonce(public_key: &[u8; 32], index: u64) -> XNonce {
    let mut nonce = public_key[..16].to_vec();
    nonce.extend(index.to_le_bytes());
    XNonce::clone_from_slice(&nonce)
}
