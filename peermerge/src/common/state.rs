use automerge::ObjId;
use std::fmt::Debug;

use crate::{automerge::AutomergeDoc, DocumentId};

use super::{cipher::doc_url_to_public_key, keys::discovery_key_from_public_key};

/// PeermergeState stores serialized information about all of the documents.
#[derive(Debug)]
pub(crate) struct PeermergeState {
    pub(crate) version: u8,
    pub(crate) name: String, // This peer's name
    pub(crate) document_ids: Vec<DocumentId>,
}
impl PeermergeState {
    pub(crate) fn new(name: &str, document_ids: Vec<DocumentId>) -> Self {
        Self::new_with_version(1, name.to_string(), document_ids)
    }

    pub(crate) fn new_with_version(
        version: u8,
        name: String,
        document_ids: Vec<DocumentId>,
    ) -> Self {
        Self {
            version,
            name,
            document_ids,
        }
    }
}

/// DocumentState stores serialized information about a single document.
#[derive(Debug)]
pub(crate) struct DocumentState {
    pub(crate) version: u8,
    pub(crate) doc_url: String,
    pub(crate) doc_public_key: [u8; 32],
    pub(crate) doc_discovery_key: [u8; 32],
    pub(crate) name: String, // This peer's name
    pub(crate) proxy: bool,
    pub(crate) encrypted: bool,
    pub(crate) peers: Vec<DocumentPeerState>,
    /// Public key of personal writeable hypercore. None means the
    /// document is read-only.
    pub(crate) write_public_key: Option<[u8; 32]>,
    /// Content of the document. None means content hasn't been synced yet.
    pub(crate) content: Option<DocumentContent>,
    /// Transient watch variable
    pub(crate) watched_ids: Vec<ObjId>,
}
impl DocumentState {
    pub(crate) fn new(
        doc_url: &str,
        name: &str,
        proxy: bool,
        peers: Vec<DocumentPeerState>,
        write_public_key: Option<[u8; 32]>,
        content: Option<DocumentContent>,
    ) -> Self {
        Self::new_with_version(
            1,
            doc_url.to_string(),
            name.to_string(),
            proxy,
            peers,
            write_public_key,
            content,
        )
    }

    pub(crate) fn new_with_version(
        version: u8,
        doc_url: String,
        name: String,
        proxy: bool,
        peers: Vec<DocumentPeerState>,
        write_public_key: Option<[u8; 32]>,
        content: Option<DocumentContent>,
    ) -> Self {
        let (doc_public_key, encrypted) = doc_url_to_public_key(&doc_url, &None);
        Self {
            version,
            doc_url,
            doc_public_key,
            doc_discovery_key: discovery_key_from_public_key(&doc_public_key),
            name,
            proxy,
            encrypted,
            peers,
            write_public_key,
            content,
            watched_ids: vec![],
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct DocumentPeerState {
    pub(crate) public_key: [u8; 32],
    pub(crate) discovery_key: [u8; 32],
    pub(crate) name: Option<String>,
}
impl DocumentPeerState {
    pub(crate) fn new(public_key: [u8; 32], name: Option<String>) -> Self {
        let discovery_key = discovery_key_from_public_key(&public_key);
        Self {
            discovery_key,
            public_key,
            name,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct DocumentCursor {
    pub(crate) discovery_key: [u8; 32],
    pub(crate) length: u64,
}
impl DocumentCursor {
    pub(crate) fn new(discovery_key: [u8; 32], length: u64) -> Self {
        Self {
            discovery_key,
            length,
        }
    }
}

#[derive(Debug)]
pub(crate) struct DocumentContent {
    pub(crate) data: Vec<u8>,
    pub(crate) cursors: Vec<DocumentCursor>,
    /// Transient reflection of the saved state, created from
    /// data the first time it is accessed.
    pub(crate) automerge_doc: Option<AutomergeDoc>,
}
impl DocumentContent {
    pub(crate) fn new(
        data: Vec<u8>,
        cursors: Vec<DocumentCursor>,
        automerge_doc: AutomergeDoc,
    ) -> Self {
        Self {
            data,
            cursors,
            automerge_doc: Some(automerge_doc),
        }
    }

    pub(crate) fn cursor_length(&self, discovery_key: &[u8; 32]) -> u64 {
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

    pub(crate) fn set_cursor(&mut self, discovery_key: &[u8; 32], length: u64) {
        if let Some(cursor) = self
            .cursors
            .iter_mut()
            .find(|cursor| &cursor.discovery_key == discovery_key)
        {
            cursor.length = length;
        } else {
            self.cursors
                .push(DocumentCursor::new(discovery_key.clone(), length));
        }
    }
}
