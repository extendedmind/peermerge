/// An BroadcastMessage transmits all of the active public keys the peer knows to the other peer
#[derive(Debug)]
pub(crate) struct BroadcastMessage {
    pub(crate) write_public_key: Option<[u8; 32]>,
    pub(crate) peer_public_keys: Vec<[u8; 32]>,
}
impl BroadcastMessage {
    pub(crate) fn new(public_key: Option<[u8; 32]>, peer_public_keys: Vec<[u8; 32]>) -> Self {
        Self {
            write_public_key: public_key,
            peer_public_keys,
        }
    }
}

/// An NewPeersCreatedMessage is an internal message that contains all of the
/// public keys of created peer hypercores.
#[derive(Debug)]
pub(crate) struct NewPeersCreatedMessage {
    pub(crate) doc_discovery_key: [u8; 32],
    pub(crate) public_keys: Vec<[u8; 32]>,
}
impl NewPeersCreatedMessage {
    pub(crate) fn new(doc_discovery_key: [u8; 32], public_keys: Vec<[u8; 32]>) -> Self {
        Self {
            doc_discovery_key,
            public_keys,
        }
    }
}

/// An PeerSyncedMessage is an internal message that contains new length
/// of a hypercore for inter-protocol signaling.
#[derive(Debug)]
pub(crate) struct PeerSyncedMessage {
    pub(crate) contiguous_length: u64,
}
impl PeerSyncedMessage {
    pub(crate) fn new(contiguous_length: u64) -> Self {
        Self { contiguous_length }
    }
}
