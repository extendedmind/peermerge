use hypercore_protocol::{
    hypercore::{RequestBlock, RequestUpgrade},
    schema::Request,
};

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
    pub(super) inflight: InflightTracker,
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
            inflight: InflightTracker::default(),
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

#[derive(Debug, Clone, Default)]
pub(super) struct InflightTracker {
    freed_ids: Vec<u64>,
    requests: Vec<Option<Request>>,
}

impl InflightTracker {
    pub fn add(&mut self, request: &mut Request) {
        let id = self
            .freed_ids
            .pop()
            .unwrap_or_else(|| self.requests.len() as u64 + 1);
        request.id = id;
        self.requests.push(Some(request.clone()));
    }

    pub fn get(&self, id: u64) -> Option<Request> {
        if id as usize <= self.requests.len() {
            self.requests[id as usize - 1].clone()
        } else {
            None
        }
    }

    pub fn get_highest_upgrade(&self) -> Option<RequestUpgrade> {
        self.requests
            .iter()
            .filter_map(|request| request.as_ref().and_then(|request| request.upgrade.clone()))
            .reduce(|accum, upgrade| {
                if upgrade.start + upgrade.length > accum.start + accum.length {
                    upgrade
                } else {
                    accum
                }
            })
    }

    pub fn get_highest_block(&self) -> Option<RequestBlock> {
        self.requests
            .iter()
            .filter_map(|request| request.as_ref().and_then(|request| request.block.clone()))
            .reduce(|accum, block| {
                if block.index > accum.index {
                    block
                } else {
                    accum
                }
            })
    }

    pub fn remove(&mut self, id: u64) {
        if id as usize <= self.requests.len() {
            self.requests[id as usize - 1] = None;
            self.freed_ids.push(id);
        }
    }
}
