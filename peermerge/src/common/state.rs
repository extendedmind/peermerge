use std::fmt::Debug;

use automerge::{ReadDoc, ROOT};
use uuid::Uuid;

use crate::{
    automerge::{save_automerge_doc, AutomergeDoc, DocsChangeResult},
    encode_base64_nopad,
    feed::{FeedDiscoveryKey, FeedPublicKey},
    DocUrlInfo, DocumentId, DocumentInfo, NameDescription, PeerId,
};

use super::{
    cipher::{encode_doc_url, DocUrlAppendix},
    constants::PEERMERGE_VERSION,
    keys::{discovery_key_from_public_key, document_id_from_discovery_key},
};

/// Stores serialized information about all of the documents.
#[derive(Debug)]
pub(crate) struct PeermergeState {
    pub(crate) version: u8,
    pub(crate) peer_id: PeerId,
    pub(crate) default_peer_header: NameDescription,
    pub(crate) document_ids: Vec<DocumentId>,
}
impl PeermergeState {
    pub(crate) fn new(
        default_peer_header: &NameDescription,
        document_ids: Vec<DocumentId>,
    ) -> Self {
        let peer_id: PeerId = *Uuid::new_v4().as_bytes();
        Self::new_with_version(
            PEERMERGE_VERSION,
            peer_id,
            default_peer_header.clone(),
            document_ids,
        )
    }

    pub(crate) fn new_with_version(
        version: u8,
        peer_id: PeerId,
        default_peer_header: NameDescription,
        document_ids: Vec<DocumentId>,
    ) -> Self {
        Self {
            version,
            peer_id,
            default_peer_header,
            document_ids,
        }
    }
}

/// Stores serialized information about a single document.
#[derive(Debug)]
pub(crate) struct DocumentState {
    pub(crate) version: u8,
    /// Doc feed's public key
    pub(crate) doc_public_key: FeedPublicKey,
    /// Doc feed's discovery key. Derived from doc_public_key.
    pub(crate) doc_discovery_key: FeedDiscoveryKey,
    /// Document id. Derived from doc_discovery_key.
    pub(crate) document_id: DocumentId,
    /// Is the document encrypted. If None it is unknown and proxy must be true.
    pub(crate) encrypted: Option<bool>,
    /// Is this a proxy document.
    pub(crate) proxy: bool,
    /// State for all of the peers.
    pub(crate) peers_state: DocumentPeersState,
    /// Content of the document. None for proxy.
    pub(crate) content: Option<DocumentContent>,
}
impl DocumentState {
    pub(crate) fn new(
        proxy: bool,
        doc_public_key: FeedPublicKey,
        encrypted: Option<bool>,
        peers_state: DocumentPeersState,
        content: Option<DocumentContent>,
    ) -> Self {
        Self::new_with_version(
            PEERMERGE_VERSION,
            proxy,
            doc_public_key,
            encrypted,
            peers_state,
            content,
        )
    }

    pub(crate) fn new_with_version(
        version: u8,
        proxy: bool,
        doc_public_key: FeedPublicKey,
        encrypted: Option<bool>,
        peers_state: DocumentPeersState,
        content: Option<DocumentContent>,
    ) -> Self {
        let doc_discovery_key = discovery_key_from_public_key(&doc_public_key);
        let document_id = document_id_from_discovery_key(&doc_discovery_key);
        Self {
            version,
            doc_public_key,
            doc_discovery_key,
            document_id,
            encrypted,
            proxy,
            peers_state,
            content,
        }
    }

    pub(crate) fn info(&self) -> DocumentInfo {
        let doc_url_info = self.doc_url_info();
        if let Some((document_type, document_header)) = self.document_type_and_header() {
            DocumentInfo {
                doc_url_info,
                document_type: Some(document_type),
                document_header,
                parent_document_id: None, // TODO: Support for document hierarchies
            }
        } else {
            DocumentInfo {
                doc_url_info,
                document_type: None,
                document_header: None,
                parent_document_id: None, // TODO: Support for document hierarchies
            }
        }
    }

    pub(crate) fn proxy_doc_url(&self) -> String {
        encode_doc_url(&self.doc_public_key, false, &None, &None)
    }

    pub(crate) fn doc_url(
        &self,
        initial_meta_doc_data: Vec<u8>,
        encryption_key: &Option<Vec<u8>>,
    ) -> String {
        if self.proxy {
            panic!("Can't encode doc url for proxy");
        }
        if let Some((document_type, document_header)) = self.document_type_and_header() {
            encode_doc_url(
                &self.doc_public_key,
                false,
                &Some(DocUrlAppendix {
                    meta_doc_data: initial_meta_doc_data,
                    document_type,
                    document_header,
                }),
                encryption_key,
            )
        } else {
            panic!("Can't encode doc url as there is no document type or header info");
        }
    }

    pub(crate) fn doc_url_info(&self) -> DocUrlInfo {
        if self.proxy {
            DocUrlInfo::new_proxy_only(
                self.version,
                false, // TODO: Child documents
                crate::FeedType::Hypercore,
                self.doc_public_key,
                self.doc_discovery_key,
                self.document_id,
            )
        } else {
            DocUrlInfo::new(
                self.version,
                false, // TODO: child documents
                crate::FeedType::Hypercore,
                self.doc_public_key,
                self.doc_discovery_key,
                self.document_id,
                self.encrypted.unwrap(),
            )
        }
    }

    pub(crate) fn document_type_and_header(&self) -> Option<(String, Option<NameDescription>)> {
        if let Some(content) = &self.content {
            if let Some(meta_automerge_doc) = &content.meta_automerge_doc {
                let document_header_id = meta_automerge_doc
                    .get(ROOT, "h")
                    .unwrap()
                    .map(|result| result.1)
                    .unwrap();

                let document_header_keys: Vec<_> =
                    meta_automerge_doc.keys(&document_header_id).collect();
                if document_header_keys.iter().any(|key| key == "t") {
                    let document_type: String = meta_automerge_doc
                        .get(&document_header_id, "t")
                        .unwrap()
                        .and_then(|result| result.0.to_scalar().cloned())
                        .unwrap()
                        .into_string()
                        .unwrap();

                    let document_header: Option<NameDescription> =
                        if document_header_keys.iter().any(|key| key == "n") {
                            let name: String = meta_automerge_doc
                                .get(&document_header_id, "n")
                                .unwrap()
                                .and_then(|result| result.0.to_scalar().cloned())
                                .unwrap()
                                .into_string()
                                .unwrap();
                            let description: Option<String> =
                                if document_header_keys.iter().any(|key| key == "d") {
                                    let description: String = meta_automerge_doc
                                        .get(&document_header_id, "d")
                                        .unwrap()
                                        .and_then(|result| result.0.to_scalar().cloned())
                                        .unwrap()
                                        .into_string()
                                        .unwrap();
                                    Some(description)
                                } else {
                                    None
                                };
                            Some(NameDescription { name, description })
                        } else {
                            None
                        };
                    Some((document_type, document_header))
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        }
    }

    pub(crate) fn peer_header(&self, peer_id: &PeerId) -> Option<NameDescription> {
        if let Some(content) = &self.content {
            if let Some(meta_automerge_doc) = &content.meta_automerge_doc {
                let peers_id = meta_automerge_doc
                    .get(ROOT, "p")
                    .unwrap()
                    .map(|result| result.1)
                    .unwrap();
                let peer_key = encode_base64_nopad(peer_id);
                let mut peers_id_keys = meta_automerge_doc.keys(&peers_id);
                if !peers_id_keys.any(|key| key == peer_key) {
                    let peer_id = meta_automerge_doc
                        .get(&peers_id, peer_key)
                        .unwrap()
                        .map(|result| result.1)
                        .unwrap();
                    let peer_keys: Vec<_> = meta_automerge_doc.keys(&peer_id).collect();

                    if peer_keys.iter().any(|key| key == "n") {
                        let name: String = meta_automerge_doc
                            .get(&peer_id, "n")
                            .unwrap()
                            .and_then(|result| result.0.to_scalar().cloned())
                            .unwrap()
                            .into_string()
                            .unwrap();
                        let description: Option<String> = if peer_keys.iter().any(|key| key == "d")
                        {
                            let description: String = meta_automerge_doc
                                .get(&peer_id, "d")
                                .unwrap()
                                .and_then(|result| result.0.to_scalar().cloned())
                                .unwrap()
                                .into_string()
                                .unwrap();
                            Some(description)
                        } else {
                            None
                        };
                        Some(NameDescription { name, description })
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct DocumentPeersState {
    /// Id and public key of personal writeable feed. None if proxy.
    pub(crate) write_peer: Option<DocumentPeer>,
    /// Keys of the other peers.
    pub(crate) peers: Vec<DocumentPeer>,
}

impl DocumentPeersState {
    pub(crate) fn new() -> Self {
        Self::new_from_data(None, vec![])
    }

    pub(crate) fn new_writer(peer_id: &PeerId, write_public_key: &FeedPublicKey) -> Self {
        Self::new_from_data(
            Some(DocumentPeer::new(*peer_id, *write_public_key, None)),
            vec![],
        )
    }

    pub(crate) fn new_from_data(
        mut write_peer: Option<DocumentPeer>,
        mut peers: Vec<DocumentPeer>,
    ) -> Self {
        // For state, populate the peers with discovery keys
        if let Some(write_peer) = write_peer.as_mut() {
            write_peer.populate_discovery_key();
        }
        for peer in peers.iter_mut() {
            peer.populate_discovery_key();
        }
        Self { write_peer, peers }
    }

    pub(crate) fn filter_new_peers(
        &self,
        remote_write_peer: &Option<DocumentPeer>,
        remote_peers: &[DocumentPeer],
    ) -> Vec<DocumentPeer> {
        let mut new_remote_peers: Vec<DocumentPeer> = remote_peers
            .iter()
            .filter(|remote_peer| {
                let writable_matches: bool = if let Some(write_peer) = &self.write_peer {
                    write_peer == *remote_peer
                } else {
                    false
                };
                !writable_matches && !self.peers.contains(remote_peer)
            })
            .cloned()
            .collect();
        if let Some(remote_write_peer) = remote_write_peer {
            if !self
                .peers
                .iter()
                .any(|peer| peer.public_key == remote_write_peer.public_key)
            {
                new_remote_peers.push(remote_write_peer.clone());
            }
        }
        new_remote_peers
    }

    /// Do the public keys match those given
    pub(crate) fn peers_match(
        &self,
        remote_write_peer: &Option<DocumentPeer>,
        remote_peers: &[DocumentPeer],
    ) -> bool {
        let mut remote_peers: Vec<DocumentPeer> = remote_peers.to_vec();
        if let Some(remote_write_peer) = remote_write_peer {
            remote_peers.push(remote_write_peer.clone());
        }
        remote_peers.sort();

        let mut peers: Vec<DocumentPeer> = self.peers.clone();

        if let Some(write_peer) = &self.write_peer {
            peers.push(write_peer.clone());
        }
        peers.sort();
        remote_peers == peers
    }

    /// Merge incoming peers into existing peers
    pub(crate) fn merge_incoming_peers(
        &mut self,
        incoming_peers: &[DocumentPeer],
    ) -> (bool, Vec<DocumentPeer>, Vec<DocumentPeer>) {
        let changed_peers: Vec<DocumentPeer> = incoming_peers
            .iter()
            .filter(|incoming_peer| {
                self.peers
                    .iter()
                    .any(|stored_peer| stored_peer != *incoming_peer)
            })
            .cloned()
            .collect();
        let peers_to_create: Vec<DocumentPeer> = changed_peers
            .iter()
            .filter(|changed_peer| changed_peer.replaced_by_public_key.is_none())
            .cloned()
            .collect();

        let (changed, replaced_peers): (bool, Vec<DocumentPeer>) = {
            if changed_peers.is_empty() {
                (false, vec![])
            } else {
                // Find out if there are values currently which
                // differ only in that replaced_by_public_key has
                // been set.
                let to_be_mutated_peers: Vec<&mut DocumentPeer> = self
                    .peers
                    .iter_mut()
                    .filter(|peer| {
                        changed_peers.iter().any(|changed_peer| {
                            changed_peer.id == peer.id
                                && changed_peer.public_key == peer.public_key
                                && peer.replaced_by_public_key
                                    != changed_peer.replaced_by_public_key
                                && changed_peer.replaced_by_public_key.is_some()
                        })
                    })
                    .collect();
                if to_be_mutated_peers.is_empty() {
                    // Just append the new peers to the end, they are all new
                    let to_add_peers: Vec<DocumentPeer> = changed_peers
                        .iter()
                        .map(|peer| {
                            let mut add_peer = peer.clone();
                            add_peer.populate_discovery_key();
                            add_peer
                        })
                        .collect();
                    self.peers.extend(to_add_peers);
                    (true, vec![])
                } else {
                    // Get the peers that were replaced
                    let replaced_peers: Vec<DocumentPeer> = to_be_mutated_peers
                        .into_iter()
                        .map(|to_be_mutated_peer| {
                            let replacement = changed_peers
                                .iter()
                                .find(|new_peer| {
                                    new_peer.id == to_be_mutated_peer.id
                                        && new_peer.public_key == to_be_mutated_peer.public_key
                                })
                                .unwrap()
                                .clone();
                            to_be_mutated_peer.replaced_by_public_key =
                                replacement.replaced_by_public_key;
                            replacement
                        })
                        .collect();
                    // The rest can just be pushed in
                    let to_add_peers: Vec<DocumentPeer> = changed_peers
                        .iter()
                        .filter(|changed_peer| !replaced_peers.contains(changed_peer))
                        .map(|peer| {
                            let mut add_peer = peer.clone();
                            add_peer.populate_discovery_key();
                            add_peer
                        })
                        .collect();
                    self.peers.extend(to_add_peers);

                    (true, replaced_peers)
                }
            }
        };
        (changed, replaced_peers, peers_to_create)
    }

    pub(crate) fn peer_id(&self, discovery_key: &FeedDiscoveryKey) -> PeerId {
        let peer = self
            .peers
            .iter()
            .find(|peer| &peer.discovery_key.unwrap() == discovery_key);
        if let Some(peer) = peer {
            return peer.id;
        } else if let Some(write_peer) = &self.write_peer {
            if &write_peer.discovery_key.unwrap() == discovery_key {
                return write_peer.id;
            }
        }
        panic!("We should always have a peer id for every discovery key")
    }
}

#[derive(Clone, Debug, PartialOrd, PartialEq, Eq, Ord)]
pub(crate) struct DocumentPeer {
    /// Id of the peer.
    pub(crate) id: PeerId,
    /// Public key of the peer's write feed.
    pub(crate) public_key: FeedPublicKey,
    /// Key that replaced the above public_key for the same
    /// id. Needed when write feed is rotated for efficiency,
    /// but needs to be stored/sent to make sure stale peers will
    /// always find the latest feed.
    pub(crate) replaced_by_public_key: Option<FeedPublicKey>,
    /// Transient discovery key of the peer's write feed, can be saved
    /// to speed up searching based on it.
    pub(crate) discovery_key: Option<FeedDiscoveryKey>,
}
impl DocumentPeer {
    pub(crate) fn new(
        id: PeerId,
        public_key: [u8; 32],
        replaced_by_public_key: Option<FeedPublicKey>,
    ) -> Self {
        Self {
            id,
            public_key,
            replaced_by_public_key,
            discovery_key: None,
        }
    }

    pub(crate) fn populate_discovery_key(&mut self) {
        if self.discovery_key.is_none() {
            self.discovery_key = Some(discovery_key_from_public_key(&self.public_key));
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct DocumentCursor {
    pub(crate) discovery_key: FeedDiscoveryKey,
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
    /// Cursors of feeds which have been read to get the below fields.
    /// Can also be empty when meta_doc_data is read from a doc URL.
    pub(crate) cursors: Vec<DocumentCursor>,
    /// Data blob containing meta_automerge_doc. None for proxy.
    pub(crate) meta_doc_data: Option<Vec<u8>>,
    /// Data blob containing user_doc_data. Is missing on
    /// peers until the doc feed is replicated.
    pub(crate) user_doc_data: Option<Vec<u8>>,

    /// Meta CRDT containing the name and description for the
    /// user document, name and description for all of the peers,
    /// and ids and encryption keys of child document databases.
    /// Transient reflection of the saved meta state, created from
    /// meta_doc_data the first time it is accessed.
    pub(crate) meta_automerge_doc: Option<AutomergeDoc>,
    /// CRDT for userspace data. Transient reflection of the saved
    /// meta state, created from user_doc_data the first time it is
    /// accessed.
    pub(crate) user_automerge_doc: Option<AutomergeDoc>,
}
impl DocumentContent {
    pub(crate) fn new_writer(
        doc_discovery_key: &FeedDiscoveryKey,
        doc_feed_length: usize,
        write_discovery_key: &FeedDiscoveryKey,
        write_feed_length: usize,
        meta_doc_data: Vec<u8>,
        user_doc_data: Option<Vec<u8>>,
        meta_automerge_doc: AutomergeDoc,
        user_automerge_doc: Option<AutomergeDoc>,
    ) -> Self {
        let cursors: Vec<DocumentCursor> = vec![
            DocumentCursor::new(*doc_discovery_key, doc_feed_length.try_into().unwrap()),
            DocumentCursor::new(*write_discovery_key, write_feed_length.try_into().unwrap()),
        ];
        Self {
            cursors,
            meta_doc_data: Some(meta_doc_data),
            user_doc_data,
            meta_automerge_doc: Some(meta_automerge_doc),
            user_automerge_doc,
        }
    }

    pub(crate) fn new_proxy(doc_discovery_key: &FeedDiscoveryKey) -> Self {
        let cursors: Vec<DocumentCursor> = vec![DocumentCursor::new(*doc_discovery_key, 0)];
        Self {
            cursors,
            meta_doc_data: None,
            user_doc_data: None,
            meta_automerge_doc: None,
            user_automerge_doc: None,
        }
    }

    pub(crate) fn new(
        cursors: Vec<DocumentCursor>,
        meta_doc_data: Vec<u8>,
        user_doc_data: Vec<u8>,
        meta_automerge_doc: AutomergeDoc,
        user_automerge_doc: AutomergeDoc,
    ) -> Self {
        Self {
            cursors,
            meta_doc_data: Some(meta_doc_data),
            user_doc_data: Some(user_doc_data),
            meta_automerge_doc: Some(meta_automerge_doc),
            user_automerge_doc: Some(user_automerge_doc),
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

    pub(crate) fn set_cursor_and_save_data(
        &mut self,
        discovery_key: [u8; 32],
        length: u64,
        change_result: DocsChangeResult,
    ) {
        self.set_cursors_and_save_data(vec![(discovery_key, length)], change_result);
    }

    pub(crate) fn set_cursors_and_save_data(
        &mut self,
        cursor_changes: Vec<([u8; 32], u64)>,
        change_result: DocsChangeResult,
    ) {
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
        if change_result.meta_changed {
            let meta_automerge_doc = self
                .meta_automerge_doc
                .as_mut()
                .expect("Meta document must be present when setting cursor");
            self.meta_doc_data = Some(save_automerge_doc(meta_automerge_doc));
        }
        if change_result.user_changed {
            let user_automerge_doc = self
                .user_automerge_doc
                .as_mut()
                .expect("User document must be present when setting cursor");
            self.user_doc_data = Some(save_automerge_doc(user_automerge_doc));
        }
    }
}
