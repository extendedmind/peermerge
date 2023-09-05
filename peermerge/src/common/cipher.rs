use chacha20poly1305::{
    aead::{Aead, KeyInit, OsRng},
    XChaCha20Poly1305, XNonce,
};
use compact_encoding::{CompactEncoding, State};
use hypercore_protocol::hypercore::{sign, verify, Signature, SigningKey, VerifyingKey};
use std::convert::TryInto;
use std::fmt::Debug;
use uuid::Uuid;

use crate::{
    common::keys::{discovery_key_from_public_key, document_id_from_discovery_key},
    feeds::{FeedDiscoveryKey, FeedPublicKey},
    DocumentId, DocumentInfo, FeedType, NameDescription, PeerId, PeermergeError,
    StaticDocumentInfo,
};

use super::{
    constants::PEERMERGE_VERSION,
    types::{AccessType, DynamicDocumentInfo},
};

const DOC_URL_PREFIX: &str = "peermerge:/";
const PLAINTEXT_PARAM: &str = "?pt=";
const CIPHERTEXT_PARAM: &str = "?ct=";
const SIGNATURE_PARAM: &str = "s=";
const DOCUMENT_SECRET_PARAM: &str = "&ds=";

/// Generate new v4 UUID. Convenience method to generate UUIDs using
/// just peermerge without importing the uuid crate and configuring
/// WASM support.
pub fn new_uuid_v4() -> [u8; 16] {
    *Uuid::new_v4().as_bytes()
}

/// Encode with Base64 URL-compatible nopad. Convenience method to
/// create short strings from byte arrays to use as automerge keys.
/// strings from byte arrays to use as automerge keys.
pub fn encode_base64_nopad(value: &[u8]) -> String {
    data_encoding::BASE64URL_NOPAD.encode(value)
}

/// Decode with Base64 URL-compatible nopad. Convenience method to
/// create byte arrays from automerge keys.
pub fn decode_base64_nopad(value: &str) -> Result<Vec<u8>, PeermergeError> {
    let encoded_base64 = value.as_bytes();
    let mut decoded = vec![
        0;
        data_encoding::BASE64URL_NOPAD
            .decode_len(encoded_base64.len())
            .map_err(|err| PeermergeError::BadArgument {
                context: format!("Could not Base64 decode length, {err:?}")
            })?
    ];
    data_encoding::BASE64URL_NOPAD
        .decode_mut(encoded_base64, &mut decoded)
        .map_err(|err| PeermergeError::BadArgument {
            context: format!("Could not Base64 decode content, {err:?}"),
        })?;
    Ok(decoded)
}

/// Get document information from document URL and optional document secret.
pub fn get_document_info(
    document_url: &str,
    document_secret: Option<String>,
) -> Result<DocumentInfo, PeermergeError> {
    let document_secret: Option<DocumentSecret> = document_secret
        .map(|secret| decode_document_secret(&secret))
        .transpose()?;
    Ok(decode_doc_url(document_url, &document_secret)?.into())
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
    doc_signature_signing_key: &SigningKey,
    child: bool,
    doc_url_appendix: &Option<DocUrlAppendix>,
    encryption_key: &Option<Vec<u8>>,
) -> String {
    let encoded_domain = encode_domain(
        doc_public_key,
        &doc_signature_signing_key.verifying_key(),
        child,
    );
    let mut url = if let Some(doc_url_appendix) = doc_url_appendix {
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
            let encoded_ciphertext = encode_base64_nopad(&ciphertext);
            format!("{CIPHERTEXT_PARAM}{encoded_ciphertext}")
        } else {
            let encoded_plaintext = encode_base64_nopad(&appendix_buffer);
            format!("{PLAINTEXT_PARAM}{encoded_plaintext}")
        };
        format!("peermerge:/{encoded_domain}{postfix}&{SIGNATURE_PARAM}")
    } else {
        format!("peermerge:/{encoded_domain}?{SIGNATURE_PARAM}")
    };

    // Lastly sign the entire string url and append signature to the end
    sign_url(&mut url, doc_signature_signing_key);
    url
}

pub(crate) fn proxy_doc_url_from_doc_url(
    doc_url: &str,
    doc_signature_signing_key: &SigningKey,
) -> String {
    let (domain_end, _) = get_domain_end_and_appendix_start_end_encrypted(doc_url);
    let mut url = format!("{}?{}", &doc_url[..domain_end], SIGNATURE_PARAM);

    // Lastly sign the entire string URL and append signature to the end
    sign_url(&mut url, doc_signature_signing_key);
    url
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct DocUrlAppendix {
    pub meta_doc_data: Vec<u8>,
    pub document_type: String,
    pub document_header: Option<NameDescription>,
}

#[derive(Debug, Clone)]
pub(crate) struct DecodedDocUrl {
    pub(crate) access_type: AccessType,
    pub(crate) encrypted: Option<bool>,
    pub(crate) static_info: StaticDocumentInfo,
    pub(crate) doc_url_appendix: Option<DocUrlAppendix>,
    pub(crate) document_secret: DocumentSecret,
}

impl Into<DocumentInfo> for DecodedDocUrl {
    fn into(self) -> DocumentInfo {
        DocumentInfo {
            access_type: self.access_type,
            encrypted: self.encrypted,
            parent_document_id: None,
            static_info: self.static_info,
            dynamic_info: self.doc_url_appendix.map(|appendix| DynamicDocumentInfo {
                document_type: appendix.document_type,
                document_header: appendix.document_header,
            }),
        }
    }
}

pub(crate) fn decode_doc_url(
    doc_url: &str,
    document_secret: &Option<DocumentSecret>,
) -> Result<DecodedDocUrl, PeermergeError> {
    if &doc_url[..DOC_URL_PREFIX.len()] != DOC_URL_PREFIX {
        return Err(PeermergeError::BadArgument {
            context: format!("Given doc URL did not start with {DOC_URL_PREFIX}"),
        });
    }

    let (static_info, appendix_position, document_secret) =
        get_static_document_info_and_appendix_position(doc_url, document_secret)?;
    let document_secret = document_secret.unwrap_or_else(|| DocumentSecret::empty());
    if let Some(doc_signature_signing_key) = &document_secret.doc_signature_signing_key {
        if static_info.doc_signature_verifying_key != doc_signature_signing_key.verifying_key() {
            return Err(PeermergeError::BadArgument {
                context: "Verifying key in doc URL does not match that given as document secret"
                    .to_string(),
            });
        }
    }

    let (doc_url_appendix, encrypted, access_type): (
        Option<DocUrlAppendix>,
        Option<bool>,
        AccessType,
    ) = if let Some((appendix_start, appendix_end, encrypted)) = appendix_position {
        let buffer = decode_base64_nopad(&doc_url[appendix_start..appendix_end])?;
        let appendix_buffer: Option<Vec<u8>> = if encrypted {
            // The url indicates that its encrypted. If an encryption key is given, use it to unwrap the header
            if let Some(encryption_key) = &document_secret.encryption_key {
                let nonce = generate_nonce(&static_info.doc_public_key, 0);
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
            let access_type = if document_secret.doc_signature_signing_key.is_some() {
                AccessType::ReadWrite
            } else {
                AccessType::ReadOnly
            };
            (Some(doc_url_appendix), Some(encrypted), access_type)
        } else {
            (None, None, AccessType::Proxy)
        }
    } else {
        (None, None, AccessType::Proxy)
    };
    Ok(DecodedDocUrl {
        encrypted,
        access_type,
        static_info,
        doc_url_appendix,
        document_secret,
    })
}

pub(crate) fn encode_document_id(document_id: &DocumentId) -> String {
    encode_base64_nopad(document_id)
}

pub(crate) fn encode_document_secret(document_secret: &DocumentSecret) -> String {
    let mut enc_state = State::new();
    enc_state
        .preencode(document_secret)
        .expect("Pre-encoding document secret should not fail");
    let mut buffer = enc_state.create_buffer();
    enc_state
        .encode(document_secret, &mut buffer)
        .expect("Encoding document secret should not fail");
    encode_base64_nopad(&buffer)
}

pub(crate) fn encode_reattach_secret(peer_id: &PeerId, key_pair: &[u8]) -> String {
    let mut data = peer_id.to_vec();
    data.extend(key_pair);
    encode_base64_nopad(&data)
}

#[derive(Debug, Clone)]
pub(crate) struct DocumentSecret {
    pub(crate) version: u8,
    pub(crate) encryption_key: Option<Vec<u8>>,
    pub(crate) doc_signature_signing_key: Option<SigningKey>,
}

impl DocumentSecret {
    pub(crate) fn new(
        encryption_key: Option<Vec<u8>>,
        doc_signature_signing_key: Option<SigningKey>,
    ) -> Self {
        Self {
            version: PEERMERGE_VERSION,
            encryption_key,
            doc_signature_signing_key,
        }
    }

    pub(crate) fn empty() -> Self {
        Self {
            version: PEERMERGE_VERSION,
            encryption_key: None,
            doc_signature_signing_key: None,
        }
    }
}

pub(crate) fn decode_document_secret(
    document_secret: &str,
) -> Result<DocumentSecret, PeermergeError> {
    let document_secret = decode_base64_nopad(document_secret)?;
    let mut dec_state = State::from_buffer(&document_secret);
    let document_secret: DocumentSecret = dec_state.decode(&document_secret)?;
    if document_secret.version != PEERMERGE_VERSION {
        return Err(PeermergeError::BadArgument {
            context: format!(
                "Invalid document secret version, expected {PEERMERGE_VERSION}, got {}",
                document_secret.version
            ),
        });
    }
    Ok(document_secret)
}

pub(crate) fn decode_reattach_secret(
    reattach_secret: &str,
) -> Result<(PeerId, Vec<u8>), PeermergeError> {
    let decoded = decode_base64_nopad(reattach_secret)?;
    if decoded.len() != 16 + 32 {
        return Err(PeermergeError::BadArgument {
            context: format!(
                "Invalid reattach_secret byte length {}, expected {}",
                decoded.len(),
                16 + 32
            ),
        });
    }
    Ok((decoded[..16].try_into().unwrap(), decoded[16..].to_vec()))
}

pub(crate) fn add_signature(data: &mut Vec<u8>, doc_signature_signing_key: &SigningKey) {
    let signature: [u8; 64] = sign(doc_signature_signing_key, data).to_bytes();
    data.extend(signature);
}

pub(crate) fn verify_data_signature(
    data: &[u8],
    doc_signature_verifying_key: &VerifyingKey,
) -> Result<(), PeermergeError> {
    if data.len() <= 64 {
        return Err(PeermergeError::InvalidOperation {
            context: "Signed entry is too short".to_string(),
        });
    }
    let signature: [u8; 64] =
        data[data.len() - 64..]
            .try_into()
            .map_err(|err| PeermergeError::BadArgument {
                context: format!("Invalid signature in data, {err}"),
            })?;
    let signature = Signature::from_bytes(&signature);
    Ok(verify(
        doc_signature_verifying_key,
        &data[..data.len() - 64],
        Some(&signature),
    )?)
}

fn encode_domain(
    doc_public_key: &FeedPublicKey,
    doc_signature_verifying_key: &VerifyingKey,
    child: bool,
) -> String {
    let mut domain: Vec<u8> = Vec::with_capacity(32 + 2);
    domain.push(PEERMERGE_VERSION);
    let feed_type = FeedType::Hypercore as u8;
    let header: u8 = if child { feed_type | 0x80 } else { feed_type };
    domain.push(header);
    domain.extend(doc_public_key);
    domain.extend(doc_signature_verifying_key.to_bytes());
    encode_base64_nopad(&domain)
}

fn decode_domain_and_document_secret(
    doc_url: &str,
    domain_end: usize,
    appendix_position: Option<(usize, usize, bool)>,
    document_secret: &Option<DocumentSecret>,
) -> Result<(StaticDocumentInfo, Option<DocumentSecret>), PeermergeError> {
    let domain = decode_base64_nopad(&doc_url[DOC_URL_PREFIX.len()..domain_end])?;
    if domain.len() != 64 + 2 {
        return Err(PeermergeError::BadArgument {
            context: format!(
                "Invalid URL domain length {}, expected {}",
                domain.len(),
                64 + 2
            ),
        });
    }
    let version = domain[0];
    if domain[0] != PEERMERGE_VERSION {
        return Err(PeermergeError::BadArgument {
            context: format!(
                "Invalid URL peermerge version {}, expected {}",
                domain[0], PEERMERGE_VERSION
            ),
        });
    }
    let header = domain[1];
    // The highest bit is the child bit, the rest feed type.
    // NB: Other info can be encoded later in between because
    // 7 bits for feed type is a lot, two bits should do.
    let child = header & 0x80 == 0x80;
    let feed_type: FeedType = (header & 0x7F).try_into().unwrap();
    if feed_type != FeedType::Hypercore {
        return Err(PeermergeError::BadArgument {
            context: "Invalid URL feed type, only hypercore supported".to_string(),
        });
    }
    // URL contains the document's doc public key, from which
    // the discovery key and document id are derived
    let doc_public_key: FeedPublicKey = domain[2..2 + 32].try_into().unwrap();
    let doc_discovery_key: FeedDiscoveryKey = discovery_key_from_public_key(&doc_public_key);
    let document_id: DocumentId = document_id_from_discovery_key(&doc_discovery_key);

    // URL ends with the document's signature verifying key, which
    // is used to verify the signature in the end.
    let doc_signature_verifying_key: [u8; 32] = domain[2 + 32..].try_into().unwrap();
    let doc_signature_verifying_key = VerifyingKey::from_bytes(&doc_signature_verifying_key)
        .map_err(|err| PeermergeError::BadArgument {
            context: format!("Could not parse valid signature verifying key from doc URL, {err}"),
        })?;

    let (signature_position, expected_signature_param) =
        if let Some((_, appendix_end, _)) = appendix_position {
            (appendix_end, format!("&{SIGNATURE_PARAM}"))
        } else {
            (domain_end, format!("?{SIGNATURE_PARAM}"))
        };

    // Verify the signature at the end of the doc URL, and extract a possible
    // document secret from the end if it is not given as param.
    let mut document_secret: Option<DocumentSecret> = document_secret.clone();
    let (unsigned_url, signature_base64) = if doc_url[signature_position..].len() < 4
        || doc_url[signature_position..signature_position + 3] != expected_signature_param
    {
        return Err(PeermergeError::BadArgument {
            context: "Invalid URL, missing signature".to_string(),
        });
    } else {
        let signature_end_index = if let Some(next_param_index) =
            doc_url[signature_position + 3..].find('&')
        {
            let signature_end_index = signature_position + 3 + next_param_index;
            if doc_url[signature_end_index..].len() < DOCUMENT_SECRET_PARAM.len()
                || &doc_url[signature_end_index..signature_end_index + DOCUMENT_SECRET_PARAM.len()]
                    != DOCUMENT_SECRET_PARAM
            {
                return Err(PeermergeError::BadArgument {
                    context: "Invalid URL, unexpected parameter after signature".to_string(),
                });
            }
            if document_secret.is_none() {
                document_secret = Some(decode_document_secret(
                    &doc_url[signature_end_index + DOCUMENT_SECRET_PARAM.len()..],
                )?);
            }
            signature_end_index
        } else {
            doc_url.len()
        };

        (
            &doc_url[..signature_position + 3],
            &doc_url[signature_position + 3..signature_end_index],
        )
    };
    let signature = decode_base64_nopad(signature_base64)?;
    verify_url_signature(unsigned_url, &signature, &doc_signature_verifying_key)?;

    Ok((
        StaticDocumentInfo {
            version,
            child,
            feed_type,
            doc_public_key,
            doc_discovery_key,
            document_id,
            doc_signature_verifying_key,
        },
        document_secret,
    ))
}

fn get_static_document_info_and_appendix_position(
    doc_url: &str,
    document_secret: &Option<DocumentSecret>,
) -> Result<
    (
        StaticDocumentInfo,
        Option<(usize, usize, bool)>,
        Option<DocumentSecret>,
    ),
    PeermergeError,
> {
    let (domain_end, appendix_position) = get_domain_end_and_appendix_start_end_encrypted(doc_url);
    let (static_document_info, document_secret) =
        decode_domain_and_document_secret(doc_url, domain_end, appendix_position, document_secret)?;
    Ok((static_document_info, appendix_position, document_secret))
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

fn sign_url(unsigned_url: &mut String, doc_signature_signing_key: &SigningKey) {
    let url_bytes = unsigned_url.as_bytes();
    let signature = sign(doc_signature_signing_key, url_bytes).to_bytes();
    unsigned_url.push_str(&encode_base64_nopad(&signature));
}

fn verify_url_signature(
    unsigned_url: &str,
    signature: &[u8],
    doc_signature_verifying_key: &VerifyingKey,
) -> Result<(), PeermergeError> {
    let signature: [u8; 64] = signature
        .try_into()
        .map_err(|err| PeermergeError::BadArgument {
            context: format!("Invalid signature in doc URL, {err}"),
        })?;
    let signature = Signature::from_bytes(&signature);
    Ok(verify(
        doc_signature_verifying_key,
        unsigned_url.as_bytes(),
        Some(&signature),
    )?)
}
