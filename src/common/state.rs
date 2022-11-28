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

#[derive(Debug)]
pub(crate) struct DocCursor {
    pub(crate) public_key: [u8; 32],
    pub(crate) index: u64,
}
impl DocCursor {
    pub fn new(public_key: [u8; 32], index: u64) -> Self {
        Self { public_key, index }
    }
}

#[derive(Debug)]
pub(crate) struct DocContent {
    pub(crate) doc: Vec<u8>,
    pub(crate) cursors: Vec<DocCursor>,
}
impl DocContent {
    pub fn new(doc: Vec<u8>, cursors: Vec<DocCursor>) -> Self {
        Self { doc, cursors }
    }
}
