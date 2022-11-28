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
    pub(crate) public_key: [u8; 32], // Public key of personal writeable hypercore
    pub(crate) peers: Vec<DocPeerState>,
}
impl DocState {
    pub fn new(public_key: [u8; 32], peers: Vec<DocPeerState>) -> Self {
        Self {
            version: 1,
            public_key,
            peers,
        }
    }
}

#[derive(Debug)]
pub(crate) struct DocPeerState {
    pub(crate) public_key: [u8; 32],
    pub(crate) synced: bool,
}
impl DocPeerState {
    pub fn new(public_key: [u8; 32], synced: bool) -> Self {
        Self { public_key, synced }
    }
}
