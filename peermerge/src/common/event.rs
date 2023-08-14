use automerge::Patch;

use crate::{feed::FeedDiscoveryKey, DocumentId, NameDescription, PeerId};

use super::state::DocumentFeedInfo;

#[derive(Clone, Debug)]
pub struct StateEvent {
    pub document_id: DocumentId,
    pub content: StateEventContent,
}

impl StateEvent {
    pub fn new(document_id: [u8; 32], content: StateEventContent) -> Self {
        Self {
            document_id,
            content,
        }
    }
}

#[derive(Clone, Debug)]
pub enum StateEventContent {
    PeerSynced {
        peer_id: PeerId,
        discovery_key: FeedDiscoveryKey,
        contiguous_length: u64,
    },
    RemotePeerSynced {
        peer_id: PeerId,
        discovery_key: FeedDiscoveryKey,
        contiguous_length: u64,
    },
    Reattached {
        peer_header: NameDescription,
    },
    DocumentInitialized {
        new_document: bool,
        parent_document_id: Option<DocumentId>,
    },
    DocumentChanged {
        change_id: Option<Vec<u8>>,
        patches: Vec<Patch>,
    },
}

#[derive(Clone, Debug)]
pub(crate) struct FeedEvent {
    pub doc_discovery_key: [u8; 32],
    pub content: FeedEventContent,
}

impl FeedEvent {
    pub fn new(doc_discovery_key: [u8; 32], content: FeedEventContent) -> Self {
        Self {
            doc_discovery_key,
            content,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) enum FeedEventContent {
    NewFeedsBroadcasted {
        new_feeds: Vec<DocumentFeedInfo>,
    },
    FeedSynced {
        peer_id: Option<PeerId>,
        discovery_key: FeedDiscoveryKey,
        contiguous_length: u64,
    },
    FeedDisconnected {
        channel: u64,
    },
    RemoteFeedSynced {
        peer_id: Option<PeerId>,
        discovery_key: FeedDiscoveryKey,
        contiguous_length: u64,
    },
}
