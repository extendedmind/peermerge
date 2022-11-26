/// A PeerState stores information about a connected peer.
#[derive(Debug)]
pub(super) struct PeerState {
    pub(super) public_keys: Vec<[u8; 32]>,
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
    pub fn new(public_keys: Vec<[u8; 32]>) -> Self {
        PeerState {
            public_keys,
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

    pub fn filter_new_peer_public_keys(&self, peer_public_keys: &Vec<[u8; 32]>) -> Vec<[u8; 32]> {
        peer_public_keys
            .iter()
            .filter(|public_key| !self.public_keys.contains(public_key))
            .map(|public_key| public_key.clone())
            .collect()
    }
}
