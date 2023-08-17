use chacha20poly1305::{
    aead::{Aead, KeyInit, OsRng},
    XChaCha20Poly1305, XNonce,
};
use compact_encoding::{CompactEncoding, State};
use std::convert::TryInto;
use std::fmt::Debug;
use uuid::Uuid;

use crate::{
    common::keys::{discovery_key_from_public_key, document_id_from_discovery_key},
    feed::{FeedDiscoveryKey, FeedPublicKey},
    DocUrlInfo, DocumentId, FeedType, NameDescription, PeerId, PeermergeError,
};

use super::constants::PEERMERGE_VERSION;

const DOC_URL_PREFIX: &str = "peermerge:/";
const PLAINTEXT_PARAM: &str = "?pt=";
const CIPHERTEXT_PARAM: &str = "?ct=";

/// Generate new v4 UUID. Convenience method to generate UUIDs using
/// just peermerge without importing the uuid crate and configuring
/// WASM support.
pub fn new_uuid_v4() -> [u8; 16] {
    *Uuid::new_v4().as_bytes()
}

/// Encode with Base64 nopad. Convenience method to create short
/// strings from byte arrays to use as automerge keys.
pub fn encode_base64_nopad(value: &[u8]) -> String {
    data_encoding::BASE64_NOPAD.encode(value)
}

/// Decode with Base64 nopad. Convenience method to create
/// byte arrays from automerge keys.
pub fn decode_base64_nopad(value: &str) -> Result<Vec<u8>, PeermergeError> {
    let encoded_base64 = value.as_bytes();
    let mut decoded = vec![
        0;
        data_encoding::BASE64_NOPAD
            .decode_len(encoded_base64.len())
            .map_err(|err| PeermergeError::BadArgument {
                context: format!("Could not Base64 decode length, {err:?}")
            })?
    ];
    data_encoding::BASE32_NOPAD
        .decode_mut(encoded_base64, &mut decoded)
        .map_err(|err| PeermergeError::BadArgument {
            context: format!("Could not Base64 decode content, {err:?}"),
        })?;
    Ok(decoded)
}

/// Get public information about a document URL.
pub fn get_doc_url_info(doc_url: &str) -> DocUrlInfo {
    get_doc_url_info_and_appendix_position(doc_url).0
}

pub(crate) fn get_doc_url_info_and_appendix_position(
    doc_url: &str,
) -> (DocUrlInfo, Option<(usize, usize, bool)>) {
    let (domain_end, appendix_position) = get_domain_end_and_appendix_start_end_encrypted(doc_url);
    let (version, child, feed_type, doc_public_key, doc_discovery_key, document_id) =
        decode_domain(doc_url, domain_end);
    if let Some((_, _, encrypted)) = &appendix_position {
        (
            DocUrlInfo::new(
                version,
                child,
                feed_type,
                doc_public_key,
                doc_discovery_key,
                document_id,
                *encrypted,
            ),
            appendix_position,
        )
    } else {
        (
            DocUrlInfo::new_proxy_only(
                version,
                child,
                feed_type,
                doc_public_key,
                doc_discovery_key,
                document_id,
            ),
            appendix_position,
        )
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
    doc_public_key: &FeedPublicKey,
    child: bool,
    doc_url_appendix: &Option<DocUrlAppendix>,
    encryption_key: &Option<Vec<u8>>,
) -> String {
    let encoded_domain = encode_domain(doc_public_key, child);
    if let Some(doc_url_appendix) = doc_url_appendix {
        let mut enc_state = State::new();
        enc_state
            .preencode(doc_url_appendix)
            .expect("Pre-encoding doc url should not fail");
        let mut appendix_buffer = enc_state.create_buffer();
        enc_state
            .encode(doc_url_appendix, &mut appendix_buffer)
            .expect("Encoding doc url should not fail");

        let postfix: String = if let Some(encryption_key) = encryption_key {
            let nonce = generate_nonce(doc_public_key, 0);
            let cipher = XChaCha20Poly1305::new_from_slice(encryption_key).unwrap();
            let ciphertext = cipher.encrypt(&nonce, &*appendix_buffer).unwrap();
            let encoded_ciphertext = data_encoding::BASE32_NOPAD.encode(&ciphertext);
            format!("{CIPHERTEXT_PARAM}{encoded_ciphertext}")
        } else {
            let encoded_plaintext = data_encoding::BASE32_NOPAD.encode(&appendix_buffer);
            format!("{PLAINTEXT_PARAM}{encoded_plaintext}")
        };
        format!("peermerge:/{encoded_domain}{postfix}")
    } else {
        format!("peermerge:/{encoded_domain}")
    }
}

pub(crate) fn encode_proxy_doc_url(doc_url: &str) -> String {
    let (domain_end, _) = get_domain_end_and_appendix_start_end_encrypted(doc_url);
    doc_url[..domain_end].to_string()
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct DocUrlAppendix {
    pub meta_doc_data: Vec<u8>,
    pub document_type: String,
    pub document_header: Option<NameDescription>,
}

#[derive(Debug, Clone)]
pub(crate) struct DecodedDocUrl {
    pub(crate) doc_url_info: DocUrlInfo,
    pub(crate) doc_url_appendix: Option<DocUrlAppendix>,
}

pub(crate) fn decode_doc_url(doc_url: &str, encryption_key: &Option<Vec<u8>>) -> DecodedDocUrl {
    assert_eq!(&doc_url[..DOC_URL_PREFIX.len()], DOC_URL_PREFIX);

    let (doc_url_info, appendix_position) = get_doc_url_info_and_appendix_position(doc_url);
    let doc_url_appendix: Option<DocUrlAppendix> =
        if let Some((appendix_start, appendix_end, encrypted)) = appendix_position {
            let base32 = doc_url[appendix_start..appendix_end].as_bytes();
            let mut buffer = vec![
                0;
                data_encoding::BASE32_NOPAD
                    .decode_len(base32.len())
                    .unwrap()
            ];
            data_encoding::BASE32_NOPAD
                .decode_mut(base32, &mut buffer)
                .unwrap();
            let appendix_buffer: Option<Vec<u8>> = if encrypted {
                // The url indicates that its encrypted. If an encryption key is given, use it to unwrap the header
                if let Some(encryption_key) = encryption_key {
                    let nonce = generate_nonce(&doc_url_info.doc_public_key, 0);
                    let cipher = XChaCha20Poly1305::new_from_slice(encryption_key).unwrap();
                    Some(cipher.decrypt(&nonce, &*buffer).unwrap())
                } else {
                    None
                }
            } else {
                Some(buffer)
            };
            if let Some(appendix_buffer) = appendix_buffer {
                let mut dec_state = State::from_buffer(&appendix_buffer);
                let doc_url_appendix: DocUrlAppendix = dec_state
                    .decode(&appendix_buffer)
                    .expect("Invalid URL appendix");
                Some(doc_url_appendix)
            } else {
                None
            }
        } else {
            None
        };
    DecodedDocUrl {
        doc_url_info,
        doc_url_appendix,
    }
}

pub(crate) fn encode_document_id(document_id: &DocumentId) -> String {
    data_encoding::BASE32_NOPAD.encode(document_id)
}

pub(crate) fn encode_encryption_key(encryption_key: &[u8]) -> String {
    data_encoding::BASE32_NOPAD.encode(encryption_key)
}

pub(crate) fn encode_key_pair(peer_id: &PeerId, key_pair: &[u8]) -> String {
    let mut data = peer_id.to_vec();
    data.extend(key_pair);
    data_encoding::BASE32_NOPAD.encode(&data)
}

pub(crate) fn decode_encryption_key(encryption_key: &Option<String>) -> Option<Vec<u8>> {
    encryption_key.as_ref().map(|key| decode_base32(key, 32))
}

pub(crate) fn decode_key_pair(key_pair: &str) -> (PeerId, Vec<u8>) {
    let decoded = decode_base32(key_pair, 16 + 32);
    (decoded[..16].try_into().unwrap(), decoded[16..].to_vec())
}

fn decode_base32(encoded: &str, expected_len: usize) -> Vec<u8> {
    let encoded_base32 = encoded.as_bytes();
    let mut decoded = vec![
        0;
        data_encoding::BASE32_NOPAD
            .decode_len(encoded_base32.len())
            .unwrap()
    ];
    let decoded_len = data_encoding::BASE32_NOPAD
        .decode_mut(encoded_base32, &mut decoded)
        .unwrap();
    assert_eq!(decoded_len, expected_len);
    decoded
}

fn encode_domain(doc_public_key: &[u8; 32], child: bool) -> String {
    let mut domain: Vec<u8> = Vec::with_capacity(32 + 2);
    domain.push(PEERMERGE_VERSION);
    let feed_type = FeedType::Hypercore as u8;
    let header: u8 = if child { feed_type | 0x80 } else { feed_type };
    domain.push(header);
    domain.extend(doc_public_key);
    data_encoding::BASE32_NOPAD.encode(&domain)
}

fn decode_domain(
    doc_url: &str,
    domain_end: usize,
) -> (
    u8,
    bool,
    FeedType,
    FeedPublicKey,
    FeedDiscoveryKey,
    DocumentId,
) {
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
    assert_eq!(decoded_len, 32 + 2);
    let version = domain[0];
    assert_eq!(domain[0], PEERMERGE_VERSION);
    let header = domain[1];
    // The highest bit is the child bit, the rest feed type.
    // NB: Other info can be encoded later in between because
    // 7 bits for feed type is a lot, two bits should do.
    let child = header & 0x80 == 0x80;
    let feed_type: FeedType = (header & 0x7F).try_into().unwrap();
    assert_eq!(feed_type, FeedType::Hypercore);
    // URL contains the document's root public key, document id is
    // the discovery key
    let doc_public_key: FeedPublicKey = domain[2..].try_into().unwrap();
    let doc_discovery_key: FeedDiscoveryKey = discovery_key_from_public_key(&doc_public_key);
    let document_id: DocumentId = document_id_from_discovery_key(&doc_discovery_key);
    (
        version,
        child,
        feed_type,
        doc_public_key,
        doc_discovery_key,
        document_id,
    )
}

fn get_domain_end_and_appendix_start_end_encrypted(
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
                let appendix_start = query_param_index + 4;
                let appendix_end =
                    if let Some(next_param_index) = doc_url[appendix_start..].find('&') {
                        appendix_start + next_param_index
                    } else {
                        doc_url.len()
                    };
                Some((appendix_start, appendix_end, encrypted))
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
