use automerge::{AutoCommit, Prop};
use std::fmt::Debug;

/// A RepoState stores serialized information about the Repo.
#[derive(Debug)]
pub(crate) struct RepoState {
    pub(crate) version: u8,
    pub(crate) doc_public_keys: Vec<[u8; 32]>,
}
impl Default for RepoState {
    fn default() -> Self {
        Self {
            version: 1,
            doc_public_keys: Vec::new(),
        }
    }
}

/// A DocState stores serialized information about a single document.
#[derive(Debug)]
pub(crate) struct DocState {
    pub(crate) version: u8,
    pub(crate) peers: Vec<DocPeerState>,
    /// Public key of personal writeable hypercore. None means the
    /// document is read-only.
    pub(crate) public_key: Option<[u8; 32]>,
    /// Content of the document. None means content hasn't been synced yet.
    pub(crate) content: Option<DocContent>,
    /// Transient watch variable
    pub(crate) watched_root_props: Vec<Prop>,
}
impl DocState {
    pub fn new(
        peers: Vec<DocPeerState>,
        public_key: Option<[u8; 32]>,
        content: Option<DocContent>,
    ) -> Self {
        Self {
            version: 1,
            peers,
            public_key,
            content,
            watched_root_props: vec![],
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct DocPeerState {
    pub(crate) public_key: [u8; 32],
    pub(crate) synced: bool,
}
impl DocPeerState {
    pub fn new(public_key: [u8; 32], synced: bool) -> Self {
        Self { public_key, synced }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct DocCursor {
    pub(crate) discovery_key: [u8; 32],
    pub(crate) length: u64,
}
impl DocCursor {
    pub fn new(discovery_key: [u8; 32], length: u64) -> Self {
        Self {
            discovery_key,
            length,
        }
    }
}

#[derive(Debug)]
pub(crate) struct DocContent {
    pub(crate) data: Vec<u8>,
    pub(crate) cursors: Vec<DocCursor>,
    /// Transient reflection of the saved state, created from
    /// data the first time it is accessed.
    pub(crate) doc: Option<AutoCommit>,
}
impl DocContent {
    pub fn new(data: Vec<u8>, cursors: Vec<DocCursor>) -> Self {
        Self {
            data,
            cursors,
            doc: None,
        }
    }
}
