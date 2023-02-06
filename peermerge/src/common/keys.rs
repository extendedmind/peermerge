use hypercore_protocol::{discovery_key, hypercore::generate_keypair};

pub(crate) use hypercore_protocol::hypercore::Keypair;

pub(crate) fn generate_keys() -> (Keypair, [u8; 32]) {
    let key_pair = generate_keypair();
    let discovery_key = discovery_key(&key_pair.public.to_bytes());
    (key_pair, discovery_key)
}

pub(crate) fn discovery_key_from_public_key(public_key: &[u8; 32]) -> [u8; 32] {
    discovery_key(public_key)
}

// TODO: p2panda versions of these
