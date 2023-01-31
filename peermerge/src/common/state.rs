use automerge::ObjId;
use std::fmt::Debug;
use tracing::debug;

use crate::{automerge::AutomergeDoc, hypercore::discovery_key_from_public_key};

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
    pub(crate) doc_public_key: [u8; 32],
    pub(crate) doc_discovery_key: [u8; 32],
    pub(crate) peers: Vec<DocPeerState>,
    /// Public key of personal writeable hypercore. None means the
    /// document is read-only.
    pub(crate) write_public_key: Option<[u8; 32]>,
    /// Content of the document. None means content hasn't been synced yet.
    pub(crate) content: Option<DocContent>,
    /// Transient watch variable
    pub(crate) watched_ids: Vec<ObjId>,
}
impl DocState {
    pub fn new(
        doc_public_key: [u8; 32],
        peers: Vec<DocPeerState>,
        write_public_key: Option<[u8; 32]>,
        content: Option<DocContent>,
    ) -> Self {
        Self::new_with_version(1, doc_public_key, peers, write_public_key, content)
    }

    pub fn new_with_version(
        version: u8,
        doc_public_key: [u8; 32],
        peers: Vec<DocPeerState>,
        write_public_key: Option<[u8; 32]>,
        content: Option<DocContent>,
    ) -> Self {
        Self {
            version,
            doc_public_key,
            doc_discovery_key: discovery_key_from_public_key(&doc_public_key),
            peers,
            write_public_key,
            content,
            watched_ids: vec![],
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct DocPeerState {
    pub(crate) public_key: [u8; 32],
    pub(crate) discovery_key: [u8; 32],
    pub(crate) name: Option<String>,
}
impl DocPeerState {
    pub fn new(public_key: [u8; 32], name: Option<String>) -> Self {
        let discovery_key = discovery_key_from_public_key(&public_key);
        Self {
            discovery_key,
            public_key,
            name,
        }
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
    pub(crate) doc: Option<AutomergeDoc>,
}
impl DocContent {
    pub fn new(data: Vec<u8>, cursors: Vec<DocCursor>, doc: AutomergeDoc) -> Self {
        Self {
            data,
            cursors,
            doc: Some(doc),
        }
    }

    pub fn cursor_length(&self, discovery_key: &[u8; 32]) -> u64 {
        if let Some(cursor) = self
            .cursors
            .iter()
            .find(|cursor| &cursor.discovery_key == discovery_key)
        {
            cursor.length
        } else {
            0
        }
    }

    pub fn set_cursor(&mut self, discovery_key: &[u8; 32], length: u64) {
        if let Some(cursor) = self
            .cursors
            .iter_mut()
            .find(|cursor| &cursor.discovery_key == discovery_key)
        {
            cursor.length = length;
        } else {
            self.cursors
                .push(DocCursor::new(discovery_key.clone(), length));
        }
    }
}
