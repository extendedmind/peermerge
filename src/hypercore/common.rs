use std::fmt::Debug;

use hypercore_protocol::{
    hypercore::{RequestBlock, RequestUpgrade},
    schema::Request,
};

/// A PeerState stores information about a connected peer.
#[derive(Debug)]
pub(super) struct PeerState {
    pub(super) is_doc: bool,
    pub(super) write_public_key: Option<[u8; 32]>,
    pub(super) peer_public_keys: Vec<[u8; 32]>,
    pub(super) can_upgrade: bool,
    pub(super) remote_fork: u64,
    pub(super) remote_length: u64,
    pub(super) remote_contiguous_length: u64,
    pub(super) remote_can_upgrade: bool,
    pub(super) remote_uploading: bool,
    pub(super) remote_downloading: bool,
    pub(super) length_acked: u64,

    /// Inflight requests
    pub(super) inflight: InflightTracker,

    // Receiving message state
    /// The length up to which the remote peer has given us contiguous
    /// data.
    pub(super) synced_contiguous_length: u64,
    /// The length to which we have sent the remote peer contiguous as
    /// demonstrated by a received Range, and have notified it as
    /// a PeerEvent.
    pub(super) notified_remote_synced_contiguous_length: u64,

    // Sending messaging state
    /// Has the initial Synchronize message been sent to the remote.
    pub(super) sync_sent: bool,
    /// The largest contiguous Range that has been sent to the remote.
    pub(super) contiguous_range_sent: u64,
}
impl PeerState {
    pub fn new(
        is_doc: bool,
        write_public_key: Option<[u8; 32]>,
        peer_public_keys: Vec<[u8; 32]>,
    ) -> Self {
        PeerState {
            is_doc,
            write_public_key,
            peer_public_keys,
            can_upgrade: true,
            remote_fork: 0,
            remote_length: 0,
            remote_contiguous_length: 0,
            remote_can_upgrade: false,
            remote_uploading: true,
            remote_downloading: true,
            length_acked: 0,
            inflight: InflightTracker::default(),
            synced_contiguous_length: 0,
            notified_remote_synced_contiguous_length: 0,
            sync_sent: false,
            contiguous_range_sent: 0,
        }
    }

    pub fn filter_new_peer_public_keys(
        &self,
        remote_write_public_key: &Option<[u8; 32]>,
        remote_peer_public_keys: &Vec<[u8; 32]>,
    ) -> Vec<[u8; 32]> {
        let mut new_remote_public_keys: Vec<[u8; 32]> = remote_peer_public_keys
            .iter()
            .filter(|remote_peer_public_key| {
                let writable_matches = self
                    .write_public_key
                    .map_or(false, |public_key| &public_key == *remote_peer_public_key);
                !writable_matches && !self.peer_public_keys.contains(remote_peer_public_key)
            })
            .map(|public_key| public_key.clone())
            .collect();
        if let Some(remote_public_key) = remote_write_public_key {
            if !self.peer_public_keys.contains(remote_public_key) {
                new_remote_public_keys.push(remote_public_key.clone());
            }
        }

        new_remote_public_keys
    }

    /// Do the public keys in the PeerState match those given
    pub fn peer_public_keys_match(
        &self,
        remote_write_public_key: &Option<[u8; 32]>,
        remote_peer_public_keys: &Vec<[u8; 32]>,
    ) -> bool {
        let mut remote_public_keys = remote_peer_public_keys.clone();
        if let Some(remote_public_key) = remote_write_public_key {
            remote_public_keys.push(remote_public_key.clone());
        }
        remote_public_keys.sort();

        let mut public_keys = self.peer_public_keys.clone();
        if let Some(public_key) = self.write_public_key {
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
        let id = self.freed_ids.pop().unwrap_or_else(|| {
            self.requests.push(None);
            return self.requests.len() as u64;
        });
        request.id = id;
        self.requests[id as usize - 1] = Some(request.clone());
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
