use std::fmt::Debug;

use crate::{
    automerge::{save_automerge_doc, AutomergeDoc},
    DocUrlInfo, DocumentId, DocumentInfo, NameDescription,
};

use super::{
    cipher::{decode_doc_url, encode_doc_url, DecodedDocUrl},
    constants::PEERMERGE_VERSION,
    keys::discovery_key_from_public_key,
};

/// Stores serialized information about all of the documents.
#[derive(Debug)]
pub(crate) struct PeermergeState {
    pub(crate) version: u8,
    pub(crate) peer_header: NameDescription,
    pub(crate) document_ids: Vec<DocumentId>,
}
impl PeermergeState {
    pub(crate) fn new(peer_header: &NameDescription, document_ids: Vec<DocumentId>) -> Self {
        Self::new_with_version(PEERMERGE_VERSION, peer_header.clone(), document_ids)
    }

    pub(crate) fn new_with_version(
        version: u8,
        peer_header: NameDescription,
        document_ids: Vec<DocumentId>,
    ) -> Self {
        Self {
            version,
            peer_header,
            document_ids,
        }
    }
}

/// Stores serialized information about a single document.
#[derive(Debug)]
pub(crate) struct DocumentState {
    pub(crate) version: u8,
    /// Document URL, always plain. Contains many of the other fields.
    pub(crate) plain_doc_url: String,
    /// Document's name and description. None if proxy. Derived from doc_url.
    pub(crate) document_header: Option<NameDescription>,
    /// Root feed's public key. Derived from doc_url.
    pub(crate) root_public_key: [u8; 32],
    /// Root feed's discovery key. Derived from root_public_key.
    pub(crate) root_discovery_key: [u8; 32],
    /// Is the document encrypted. If None it is unknown and proxy must be true.
    /// Derived from doc_url.
    pub(crate) encrypted: Option<bool>,
    /// Is this a proxy document.
    pub(crate) proxy: bool,
    /// The state of the other peers.
    pub(crate) peers: Vec<DocumentPeerState>,
    /// Public key of personal writeable feed. None if proxy.
    pub(crate) write_public_key: Option<[u8; 32]>,
    /// Content of the document. None means content hasn't been synced yet.
    pub(crate) content: Option<DocumentContent>,
}
impl DocumentState {
    pub(crate) fn new(
        decoded_doc_url: DecodedDocUrl,
        proxy: bool,
        peers: Vec<DocumentPeerState>,
        write_public_key: Option<[u8; 32]>,
        content: Option<DocumentContent>,
    ) -> Self {
        Self::new_with_version(
            PEERMERGE_VERSION,
            decoded_doc_url,
            proxy,
            peers,
            write_public_key,
            content,
        )
    }

    pub(crate) fn new_from_plain_doc_url(
        version: u8,
        plain_doc_url: String,
        proxy: bool,
        encrypted: Option<bool>,
        peers: Vec<DocumentPeerState>,
        write_public_key: Option<[u8; 32]>,
        content: Option<DocumentContent>,
    ) -> Self {
        let decoded_doc_url = decode_doc_url(&plain_doc_url, &None);
        Self {
            version,
            plain_doc_url: decoded_doc_url.plain_doc_url,
            document_header: decoded_doc_url.document_header,
            root_public_key: decoded_doc_url.root_public_key,
            root_discovery_key: discovery_key_from_public_key(&decoded_doc_url.root_public_key),
            encrypted,
            proxy,
            peers,
            write_public_key,
            content,
        }
    }

    pub(crate) fn new_with_version(
        version: u8,
        decoded_doc_url: DecodedDocUrl,
        proxy: bool,
        peers: Vec<DocumentPeerState>,
        write_public_key: Option<[u8; 32]>,
        content: Option<DocumentContent>,
    ) -> Self {
        Self {
            version,
            plain_doc_url: decoded_doc_url.plain_doc_url,
            document_header: decoded_doc_url.document_header,
            root_public_key: decoded_doc_url.root_public_key,
            root_discovery_key: discovery_key_from_public_key(&decoded_doc_url.root_public_key),
            encrypted: decoded_doc_url.encrypted,
            proxy,
            peers,
            write_public_key,
            content,
        }
    }

    pub(crate) fn info(&self) -> DocumentInfo {
        DocumentInfo {
            doc_url_info: self.doc_url_info(),
            document_header: self.document_header.clone(),
            parent_document_id: None, // TODO: Support for document hierarchies
        }
    }

    pub(crate) fn doc_url(&self, encryption_key: &Option<Vec<u8>>) -> String {
        if let Some(document_header) = &self.document_header {
            encode_doc_url(&self.root_public_key, document_header, encryption_key)
        } else {
            self.plain_doc_url.clone()
        }
    }

    pub(crate) fn doc_url_info(&self) -> DocUrlInfo {
        if self.proxy {
            DocUrlInfo::new_proxy_only(
                self.version,
                true, // TODO: Child documents
                crate::FeedType::Hypercore,
                self.root_public_key,
                self.root_discovery_key,
            )
        } else {
            DocUrlInfo::new(
                self.version,
                true, // TODO: child documents
                crate::FeedType::Hypercore,
                self.root_public_key,
                self.root_discovery_key,
                self.encrypted.unwrap(),
            )
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct DocumentPeerState {
    pub(crate) public_key: [u8; 32],
    pub(crate) discovery_key: [u8; 32],
    pub(crate) peer_header: Option<NameDescription>,
}
impl DocumentPeerState {
    pub(crate) fn new(public_key: [u8; 32], peer_header: Option<NameDescription>) -> Self {
        let discovery_key = discovery_key_from_public_key(&public_key);
        Self {
            discovery_key,
            public_key,
            peer_header,
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

    pub(crate) fn set_cursor_and_save_data(&mut self, discovery_key: [u8; 32], length: u64) {
        self.set_cursors_and_save_data(vec![(discovery_key, length)]);
    }

    pub(crate) fn set_cursors_and_save_data(&mut self, cursor_changes: Vec<([u8; 32], u64)>) {
        for (discovery_key, length) in cursor_changes {
            if let Some(cursor) = self
                .cursors
                .iter_mut()
                .find(|cursor| cursor.discovery_key == discovery_key)
            {
                cursor.length = length;
            } else {
                self.cursors
                    .push(DocumentCursor::new(discovery_key, length));
            }
        }
        let automerge_doc = self
            .automerge_doc
            .as_mut()
            .expect("Document must be present when setting cursor");
        self.data = save_automerge_doc(automerge_doc);
    }
}
