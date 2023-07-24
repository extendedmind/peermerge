use automerge::Patch;

use crate::{DocumentId, NameDescription};

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
    PeerSynced((Option<String>, [u8; 32], u64)),
    RemotePeerSynced(([u8; 32], u64)),
    Reattached(NameDescription),
    DocumentInitialized(bool, Option<DocumentId>),
    DocumentChanged((Option<Vec<u8>>, Vec<Patch>)),
}

#[derive(Clone, Debug)]
pub struct PeerEvent {
    // FIXME: Rename to document_id: DocumentId
    pub doc_discovery_key: [u8; 32],
    pub content: PeerEventContent,
}

impl PeerEvent {
    pub fn new(doc_discovery_key: [u8; 32], content: PeerEventContent) -> Self {
        Self {
            doc_discovery_key,
            content,
        }
    }
}

#[derive(Clone, Debug)]
pub enum PeerEventContent {
    NewPeersBroadcasted(Vec<[u8; 32]>),
    PeerSynced(([u8; 32], u64)),
    PeerDisconnected(u64),
    RemotePeerSynced(([u8; 32], u64)),
}
