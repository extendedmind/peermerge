use chacha20poly1305::{
    aead::{Aead, KeyInit, OsRng},
    XChaCha20Poly1305, XNonce,
};
use compact_encoding::{CompactEncoding, State};
use std::convert::TryInto;
use std::fmt::Debug;

use crate::{DocUrlInfo, DocumentId, FeedType, NameDescription};

const DOC_URL_PREFIX: &str = "peermerge:/";
const DOC_URL_VERSION: u8 = 1;
const PLAINTEXT_PARAM: &str = "?pt=";
const CIPHERTEXT_PARAM: &str = "?ct=";

/// Get public information about a document URL.
pub fn get_doc_url_info(doc_url: &str) -> DocUrlInfo {
    let (domain_end, header_info) = get_domain_end_and_document_header_start_end_encrypted(doc_url);
    let (domain, _) = decode_domain(doc_url, domain_end);
    let version = domain[0];
    let feed_type: FeedType = domain[1].try_into().unwrap();
    if let Some((_, _, encrypted)) = header_info {
        DocUrlInfo::new(version, feed_type, encrypted)
    } else {
        DocUrlInfo::new_proxy_only(version, feed_type)
    }
}

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

pub(crate) fn encode_doc_url(
    root_public_key: &[u8; 32],
    document_header: &NameDescription,
    encryption_key: &Option<Vec<u8>>,
) -> String {
    let encoded_domain = encode_domain(root_public_key);
    let mut enc_state = State::new();
    enc_state.preencode(document_header);
    let mut document_header_buffer = enc_state.create_buffer();
    enc_state.encode(document_header, &mut document_header_buffer);

    let postfix: String = if let Some(encryption_key) = encryption_key {
        let nonce = generate_nonce(&root_public_key, 0);
        let cipher = XChaCha20Poly1305::new_from_slice(&encryption_key).unwrap();
        let ciphertext = cipher.encrypt(&nonce, &*document_header_buffer).unwrap();
        let encoded_ciphertext = data_encoding::BASE32_NOPAD.encode(&ciphertext);
        format!("{}{}", CIPHERTEXT_PARAM, encoded_ciphertext)
    } else {
        let encoded_plaintext = data_encoding::BASE32_NOPAD.encode(&document_header_buffer);
        format!("{}{}", PLAINTEXT_PARAM, encoded_plaintext)
    };
    format!("peermerge:/{}{}", encoded_domain, postfix)
}

pub(crate) fn encode_proxy_doc_url(doc_url: &str) -> String {
    let (domain_end, _) = get_domain_end_and_document_header_start_end_encrypted(doc_url);
    doc_url[..domain_end].to_string()
}

pub(crate) struct DecodedDocUrl {
    pub(crate) root_public_key: [u8; 32],
    pub(crate) document_header: Option<NameDescription>,
    pub(crate) encrypted: Option<bool>,
    pub(crate) plain_doc_url: String,
}

impl DecodedDocUrl {
    pub(crate) fn new(
        root_public_key: [u8; 32],
        document_header: NameDescription,
        encrypted: bool,
    ) -> Self {
        let plain_doc_url = encode_doc_url(&root_public_key, &document_header, &None);
        Self {
            root_public_key,
            document_header: Some(document_header),
            encrypted: Some(encrypted),
            plain_doc_url,
        }
    }
}

pub(crate) fn decode_doc_url(doc_url: &str, encryption_key: &Option<Vec<u8>>) -> DecodedDocUrl {
    assert_eq!(&doc_url[..DOC_URL_PREFIX.len()], DOC_URL_PREFIX);
    let (domain_end, header_info) = get_domain_end_and_document_header_start_end_encrypted(doc_url);
    let (domain, decoded_len) = decode_domain(doc_url, domain_end);
    assert_eq!(domain[0], DOC_URL_VERSION);
    assert_eq!(domain[1], FeedType::Hypercore as u8);
    assert_eq!(decoded_len, 32 + 2);
    let root_public_key: [u8; 32] = domain[2..].to_vec().try_into().unwrap();

    let (document_header, plain_doc_url, encrypted): (
        Option<NameDescription>,
        String,
        Option<bool>,
    ) = if let Some((header_start, header_end, encrypted)) = header_info {
        let base32 = doc_url[header_start..header_end].as_bytes();
        let mut buffer = vec![
            0;
            data_encoding::BASE32_NOPAD
                .decode_len(base32.len())
                .unwrap()
        ];
        data_encoding::BASE32_NOPAD
            .decode_mut(base32, &mut buffer)
            .unwrap();
        let header_buffer: Option<Vec<u8>> = if encrypted {
            // The url indicates that its encrypted. If an encryption key is given, use it to unwrap the header
            if let Some(encryption_key) = encryption_key {
                let nonce = generate_nonce(&root_public_key, 0);
                let cipher = XChaCha20Poly1305::new_from_slice(&encryption_key).unwrap();
                Some(cipher.decrypt(&nonce, &*buffer).unwrap())
            } else {
                None
            }
        } else {
            Some(buffer)
        };
        if let Some(header_buffer) = header_buffer {
            let mut dec_state = State::from_buffer(&header_buffer);
            let document_header: NameDescription = dec_state.decode(&header_buffer);
            let plain_doc_url = encode_doc_url(&root_public_key, &document_header, &None);
            (Some(document_header), plain_doc_url, Some(encrypted))
        } else {
            (None, doc_url[..domain_end].to_string(), Some(encrypted))
        }
    } else {
        (None, doc_url[..domain_end].to_string(), None)
    };
    DecodedDocUrl {
        root_public_key,
        document_header,
        plain_doc_url,
        encrypted,
    }
}

pub(crate) fn encode_document_id(document_id: &DocumentId) -> String {
    data_encoding::BASE32_NOPAD.encode(document_id)
}

pub(crate) fn encode_encryption_key(encryption_key: &[u8]) -> String {
    data_encoding::BASE32_NOPAD.encode(encryption_key)
}

pub(crate) fn encode_key_pair(key_pair: &[u8]) -> String {
    data_encoding::BASE32_NOPAD.encode(key_pair)
}

pub(crate) fn decode_encryption_key(encryption_key: &Option<String>) -> Option<Vec<u8>> {
    encryption_key.as_ref().map(|key| decode_keys(key, 32))
}

pub(crate) fn decode_key_pair(key_pair: &str) -> Vec<u8> {
    decode_keys(key_pair, 64)
}

fn decode_keys(keys: &str, expected_len: usize) -> Vec<u8> {
    let keys_base32 = keys.as_bytes();
    let mut keys = vec![
        0;
        data_encoding::BASE32_NOPAD
            .decode_len(keys_base32.len())
            .unwrap()
    ];
    let decoded_len = data_encoding::BASE32_NOPAD
        .decode_mut(keys_base32, &mut keys)
        .unwrap();
    assert_eq!(decoded_len, expected_len);
    keys
}

fn encode_domain(root_public_key: &[u8; 32]) -> String {
    let mut domain: Vec<u8> = Vec::with_capacity(32 + 2);
    domain.push(DOC_URL_VERSION);
    domain.push(FeedType::Hypercore as u8);
    domain.extend(root_public_key);
    data_encoding::BASE32_NOPAD.encode(&domain)
}

fn decode_domain(doc_url: &str, domain_end: usize) -> (Vec<u8>, usize) {
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
    (domain, decoded_len)
}

fn get_domain_end_and_document_header_start_end_encrypted(
    doc_url: &str,
) -> (usize, Option<(usize, usize, bool)>) {
    if let Some(query_param_index) = doc_url.find('?') {
        let result: Option<(usize, usize, bool)> = if doc_url.len() > query_param_index + 4 {
            let encrypted: Option<bool> =
                if &doc_url[query_param_index..query_param_index + 4] == CIPHERTEXT_PARAM {
                    Some(true)
                } else if &doc_url[query_param_index..query_param_index + 4] == PLAINTEXT_PARAM {
                    Some(false)
                } else {
                    None
                };
            if let Some(encrypted) = encrypted {
                let header_start = query_param_index + 4;
                let header_end = if let Some(next_param_index) = doc_url[header_start..].find('&') {
                    header_start + next_param_index
                } else {
                    doc_url.len()
                };
                Some((header_start, header_end, encrypted))
            } else {
                None
            }
        } else {
            None
        };
        (query_param_index, result)
    } else {
        (doc_url.len(), None)
    }
}

fn generate_nonce(public_key: &[u8; 32], index: u64) -> XNonce {
    let mut nonce = public_key[..16].to_vec();
    nonce.extend(index.to_le_bytes());
    XNonce::clone_from_slice(&nonce)
}
