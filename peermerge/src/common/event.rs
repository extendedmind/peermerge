use automerge::Patch;

#[derive(Clone, Debug)]
pub struct StateEvent {
    // FIXME: Rename to document_id: DocumentId
    pub doc_discovery_key: [u8; 32],
    pub content: StateEventContent,
}

impl StateEvent {
    pub fn new(doc_discovery_key: [u8; 32], content: StateEventContent) -> Self {
        Self {
            doc_discovery_key,
            content,
        }
    }
}

#[derive(Clone, Debug)]
pub enum StateEventContent {
    PeerSynced((Option<String>, [u8; 32], u64)),
    RemotePeerSynced(([u8; 32], u64)),
    DocumentChanged(Vec<Patch>),
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
