/// A PeerState stores information about a connected peer.
#[derive(Debug)]
pub(super) struct PeerState {
    pub(super) public_key: Option<[u8; 32]>,
    pub(super) peer_public_keys: Vec<[u8; 32]>,
    pub(super) can_upgrade: bool,
    pub(super) remote_can_write: Option<bool>,
    pub(super) remote_fork: u64,
    pub(super) remote_length: u64,
    pub(super) remote_can_upgrade: bool,
    pub(super) remote_uploading: bool,
    pub(super) remote_downloading: bool,
    pub(super) remote_sync_received: bool,
    pub(super) length_acked: u64,
}
impl PeerState {
    pub fn new(
        public_key: Option<[u8; 32]>,
        peer_public_keys: Vec<[u8; 32]>,
        remote_can_write: Option<bool>,
    ) -> Self {
        PeerState {
            public_key,
            peer_public_keys,
            can_upgrade: true,
            remote_can_write,
            remote_fork: 0,
            remote_length: 0,
            remote_can_upgrade: false,
            remote_uploading: true,
            remote_downloading: true,
            remote_sync_received: false,
            length_acked: 0,
        }
    }

    pub fn filter_new_peer_public_keys(
        &self,
        remote_public_key: &Option<[u8; 32]>,
        remote_peer_public_keys: &Vec<[u8; 32]>,
    ) -> Vec<[u8; 32]> {
        let mut new_remote_public_keys: Vec<[u8; 32]> = remote_peer_public_keys
            .iter()
            .filter(|remote_peer_public_key| {
                let writable_matches = self
                    .public_key
                    .map_or(false, |public_key| &public_key == *remote_peer_public_key);
                !writable_matches && !self.peer_public_keys.contains(remote_peer_public_key)
            })
            .map(|public_key| public_key.clone())
            .collect();
        if let Some(remote_public_key) = remote_public_key {
            if !self.peer_public_keys.contains(remote_public_key) {
                new_remote_public_keys.push(remote_public_key.clone());
            }
        }

        new_remote_public_keys
    }
}
