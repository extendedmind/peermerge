use crate::feed::FeedDiscoveryKey;

use super::state::DocumentPeer;

/// An BroadcastMessage transmits all of the active public keys the peer knows to the other peer
#[derive(Debug)]
pub(crate) struct BroadcastMessage {
    pub(crate) write_peer: Option<DocumentPeer>,
    pub(crate) peers: Vec<DocumentPeer>,
}
impl BroadcastMessage {
    pub(crate) fn new(write_peer: Option<DocumentPeer>, peers: Vec<DocumentPeer>) -> Self {
        Self { write_peer, peers }
    }
}

/// An NewPeersCreatedMessage is an internal message that contains all of the
/// ids and public keys of created and changed peer hypercores.
#[derive(Debug)]
pub(crate) struct PeersChangedMessage {
    pub(crate) doc_discovery_key: FeedDiscoveryKey,
    pub(crate) incoming_peers: Vec<DocumentPeer>,
    pub(crate) replaced_peers: Vec<DocumentPeer>,
    pub(crate) peers_to_create: Vec<DocumentPeer>,
}
impl PeersChangedMessage {
    pub(crate) fn new(
        doc_discovery_key: FeedDiscoveryKey,
        incoming_peers: Vec<DocumentPeer>,
        replaced_peers: Vec<DocumentPeer>,
        peers_to_create: Vec<DocumentPeer>,
    ) -> Self {
        Self {
            doc_discovery_key,
            incoming_peers,
            replaced_peers,
            peers_to_create,
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
