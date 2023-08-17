use hypercore_protocol::{
    discovery_key as derivate,
    hypercore::{generate_signing_key, PartialKeypair, SECRET_KEY_LENGTH},
};

pub(crate) use hypercore_protocol::hypercore::SigningKey;

pub(crate) fn generate_keys() -> (SigningKey, [u8; 32]) {
    let signing_key = generate_signing_key();
    let discovery_key = derivate(&signing_key.verifying_key().to_bytes());
    (signing_key, discovery_key)
}

pub(crate) fn discovery_key_from_public_key(public_key: &[u8; 32]) -> [u8; 32] {
    derivate(public_key)
}

pub(crate) fn document_id_from_discovery_key(discovery_key: &[u8; 32]) -> [u8; 32] {
    derivate(discovery_key)
}

pub(crate) fn partial_key_pair_to_bytes(key_pair: PartialKeypair) -> Vec<u8> {
    let signing_key = SigningKey::from_bytes(&key_pair.secret.unwrap());
    signing_key.to_bytes().to_vec()
}

pub(crate) fn signing_key_from_bytes(signing_key_bytes: &[u8]) -> SigningKey {
    let signing_key_bytes: [u8; SECRET_KEY_LENGTH] = signing_key_bytes.try_into().unwrap();
    SigningKey::from_bytes(&signing_key_bytes)
}

// TODO: p2panda versions of these
