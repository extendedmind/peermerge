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
    pub(crate) public_key: [u8; 32],
    pub(crate) peer_public_keys: Vec<[u8; 32]>,
}
impl DocState {
    pub fn new(public_key: [u8; 32], peer_public_keys: Vec<[u8; 32]>) -> Self {
        Self {
            version: 1,
            public_key,
            peer_public_keys,
        }
    }
}
