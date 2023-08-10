use hypercore_protocol::{
    discovery_key as derivate,
    hypercore::{generate_keypair, PartialKeypair},
};

pub(crate) use hypercore_protocol::hypercore::Keypair;

pub(crate) fn generate_keys() -> (Keypair, [u8; 32]) {
    let key_pair = generate_keypair();
    let discovery_key = derivate(&key_pair.public.to_bytes());
    (key_pair, discovery_key)
}

pub(crate) fn discovery_key_from_public_key(public_key: &[u8; 32]) -> [u8; 32] {
    derivate(public_key)
}

pub(crate) fn document_id_from_discovery_key(discovery_key: &[u8; 32]) -> [u8; 32] {
    derivate(discovery_key)
}

pub(crate) fn partial_key_pair_to_bytes(key_pair: PartialKeypair) -> Vec<u8> {
    let key_pair = Keypair {
        public: key_pair.public.clone(),
        secret: key_pair.secret.unwrap(),
    };
    key_pair.to_bytes().to_vec()
}

pub(crate) fn key_pair_from_bytes(key_pair_bytes: &Vec<u8>) -> Keypair {
    Keypair::from_bytes(key_pair_bytes).unwrap()
}

// TODO: p2panda versions of these
