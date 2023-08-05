use automerge::Change;

use crate::{DocumentId, NameDescription};

use super::constants::PEERMERGE_VERSION;

/// Content of an entry stored to a hypercore.
#[derive(Debug, Clone)]
pub(crate) enum EntryContent {
    /// First message of the doc feed.
    InitDoc {
        /// Number of Doc entries coming after this entry.
        doc_entry_count: u32,
    },
    /// First message of a write peer feed.
    InitPeer {
        peer_header: NameDescription,
        document_id: DocumentId,
        /// Document header, stored to each peer instead of
        /// InitDoc to make the values mutable. Changing the
        /// value on one peer cascades a new feed to all the
        /// other peers.
        document_header: NameDescription,
        /// Number of DocPart entries coming after this entry. If this is
        /// the introduction of a brand new peer, then this is 0 and
        /// previous_discovery_keys is empty.
        doc_entry_count: u32,
        /// Previous write feed discovery keys for this Peer,
        /// in reverse chronological order, meaning the first
        /// discovery key points to the feed that was active right
        /// before this.
        previous_discovery_keys: Vec<[u8; 32]>,
        /// Child documents to this document and their encryption
        /// keys if encrypted. Needed to be able to automatically
        /// unencrypt child documents if the main document encryption
        /// key is known. The encryption key can't be in the broadcast
        /// message because then proxies would know it, hence store it
        /// here.
        child_documents: Vec<(DocumentId, Option<Vec<u8>>)>,
    },
    DocPart {
        index: u32,
        data: Vec<u8>,
    },
    Change {
        change: Box<Change>,
        data: Vec<u8>,
    },
}

impl EntryContent {
    pub(crate) fn new_change(data: Vec<u8>) -> Self {
        let change = Change::from_bytes(data.clone()).unwrap();
        EntryContent::Change {
            change: Box::new(change),
            data,
        }
    }
}

/// A document is stored in pieces to hypercores.
#[derive(Debug, Clone)]
pub(crate) struct Entry {
    pub(crate) version: u8,
    pub(crate) content: EntryContent,
}
impl Entry {
    pub(crate) fn new_init_doc(doc_entry_count: u32) -> Self {
        Self::new(PEERMERGE_VERSION, EntryContent::InitDoc { doc_entry_count })
    }

    pub(crate) fn new_init_peer(
        peer_header: NameDescription,
        document_id: [u8; 32],
        document_header: NameDescription,
        doc_entry_count: u32,
        previous_discovery_keys: Vec<[u8; 32]>,
        child_documents: Vec<(DocumentId, Option<Vec<u8>>)>,
    ) -> Self {
        Self::new(
            PEERMERGE_VERSION,
            EntryContent::InitPeer {
                peer_header,
                document_id,
                document_header,
                doc_entry_count,
                previous_discovery_keys,
                child_documents,
            },
        )
    }

    pub(crate) fn new_doc_part(index: u32, data: Vec<u8>) -> Self {
        Self::new(PEERMERGE_VERSION, EntryContent::DocPart { index, data })
    }

    pub(crate) fn new_change(mut change: Change) -> Self {
        let data = change.bytes().to_vec();
        Self::new(
            PEERMERGE_VERSION,
            EntryContent::Change {
                data,
                change: Box::new(change),
            },
        )
    }

    pub(crate) fn new(version: u8, content: EntryContent) -> Self {
        Self { version, content }
    }
}
