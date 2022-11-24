/// A PeerState stores information about a connected peer.
#[derive(Debug)]
pub(super) struct PeerState {
    pub(super) peer_public_keys: Vec<[u8; 32]>,
    pub(super) can_upgrade: bool,
    pub(super) remote_fork: u64,
    pub(super) remote_length: u64,
    pub(super) remote_can_upgrade: bool,
    pub(super) remote_uploading: bool,
    pub(super) remote_downloading: bool,
    pub(super) remote_synced: bool,
    pub(super) length_acked: u64,
}
impl PeerState {
    pub fn new(peer_public_keys: Vec<[u8; 32]>) -> Self {
        PeerState {
            peer_public_keys,
            can_upgrade: true,
            remote_fork: 0,
            remote_length: 0,
            remote_can_upgrade: false,
            remote_uploading: true,
            remote_downloading: true,
            remote_synced: false,
            length_acked: 0,
        }
    }
}

#[derive(Clone, Debug)]
pub(super) enum PeerEvent {
    PeersAdvertised(Vec<String>),
    PeerSynced([u8; 32]),
}
