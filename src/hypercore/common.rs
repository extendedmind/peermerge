use hypercore_protocol::schema::Request;

/// A PeerState stores information about a connected peer.
#[derive(Debug)]
pub(super) struct PeerState {
    pub(super) public_key: Option<[u8; 32]>,
    pub(super) peer_public_keys: Vec<[u8; 32]>,
    pub(super) can_upgrade: bool,
    pub(super) remote_can_write: bool,
    pub(super) remote_fork: u64,
    pub(super) remote_length: u64,
    pub(super) remote_can_upgrade: bool,
    pub(super) remote_uploading: bool,
    pub(super) remote_downloading: bool,
    pub(super) length_acked: u64,
    pub(super) requested_upgrade_length: u64,
    pub(super) request_block_index_queue: Vec<u64>,
}
impl PeerState {
    pub fn new(public_key: Option<[u8; 32]>, peer_public_keys: Vec<[u8; 32]>) -> Self {
        PeerState {
            public_key,
            peer_public_keys,
            can_upgrade: true,
            remote_can_write: false,
            remote_fork: 0,
            remote_length: 0,
            remote_can_upgrade: false,
            remote_uploading: true,
            remote_downloading: true,
            length_acked: 0,
            requested_upgrade_length: 0,
            request_block_index_queue: vec![],
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

    /// Do the public keys in the PeerState match those given
    pub fn peer_public_keys_match(
        &self,
        remote_public_key: &Option<[u8; 32]>,
        remote_peer_public_keys: &Vec<[u8; 32]>,
    ) -> bool {
        let mut remote_public_keys = remote_peer_public_keys.clone();
        if let Some(remote_public_key) = remote_public_key {
            remote_public_keys.push(remote_public_key.clone());
        }
        remote_public_keys.sort();

        let mut public_keys = self.peer_public_keys.clone();
        if let Some(public_key) = self.public_key {
            public_keys.push(public_key.clone());
        }
        public_keys.sort();
        remote_public_keys == public_keys
    }
}
