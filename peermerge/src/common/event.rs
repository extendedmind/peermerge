use automerge::Patch;

use crate::{feed::FeedDiscoveryKey, DocumentId, NameDescription, PeerId};

use super::state::DocumentPeer;

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
    PeerSynced((PeerId, FeedDiscoveryKey, u64)),
    RemotePeerSynced((PeerId, FeedDiscoveryKey, u64)),
    Reattached(NameDescription),
    DocumentInitialized(bool, Option<DocumentId>),
    DocumentChanged((Option<Vec<u8>>, Vec<Patch>)),
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
    NewFeedsBroadcasted(Vec<DocumentPeer>),
    FeedSynced((Option<PeerId>, FeedDiscoveryKey, u64)),
    FeedDisconnected(u64),
    RemoteFeedSynced((Option<PeerId>, FeedDiscoveryKey, u64)),
}
