use std::fmt::Debug;

use hypercore_protocol::{
    hypercore::{RequestBlock, RequestUpgrade, VerifyingKey},
    schema::Request,
};

use crate::{
    common::state::{ChildDocumentInfo, DocumentFeedsState},
    FeedDiscoveryKey, PeerId,
};

/// A PeerState stores information about a connected peer.
#[derive(Debug)]
pub(super) struct PeerState {
    pub(super) local_peer_id: PeerId,
    pub(super) is_doc: bool,
    /// Set only when is_doc is true
    pub(super) feeds_state: Option<DocumentFeedsState>,
    /// Non-empty only when is_doc is true
    pub(super) child_documents: Vec<ChildDocumentInfo>,
    /// Set only when is_doc is false
    pub(super) peer_id: Option<PeerId>,
    pub(super) doc_discovery_key: FeedDiscoveryKey,
    pub(super) doc_signature_verifying_key: VerifyingKey,
    pub(super) max_write_feed_length: u64,
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
    /// The largest contiguous Range that has been sent to the remote.
    pub(super) contiguous_range_sent: u64,
}
impl PeerState {
    pub(crate) fn new(
        local_peer_id: PeerId,
        is_doc: bool,
        doc_discovery_key: FeedDiscoveryKey,
        doc_signature_verifying_key: VerifyingKey,
        max_write_feed_length: u64,
        feeds_state: Option<DocumentFeedsState>,
        child_documents: Vec<ChildDocumentInfo>,
        peer_id: Option<PeerId>,
    ) -> Self {
        PeerState {
            local_peer_id,
            is_doc,
            doc_discovery_key,
            doc_signature_verifying_key,
            max_write_feed_length,
            feeds_state,
            child_documents,
            peer_id,
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
            contiguous_range_sent: 0,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub(super) struct InflightTracker {
    freed_ids: Vec<u64>,
    requests: Vec<Option<Request>>,
}

impl InflightTracker {
    pub(crate) fn add(&mut self, request: &mut Request) {
        let id = self.freed_ids.pop().unwrap_or_else(|| {
            self.requests.push(None);
            return self.requests.len() as u64;
        });
        request.id = id;
        self.requests[id as usize - 1] = Some(request.clone());
    }

    pub(crate) fn get_highest_upgrade(&self) -> Option<RequestUpgrade> {
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

    pub(crate) fn get_highest_block(&self) -> Option<RequestBlock> {
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

    pub(crate) fn remove(&mut self, id: u64) {
        if id as usize <= self.requests.len() {
            self.requests[id as usize - 1] = None;
            self.freed_ids.push(id);
        }
    }
}
