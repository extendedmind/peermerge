use std::fmt::Debug;

use uuid::Uuid;

use crate::{
    automerge::{save_automerge_doc, AutomergeDoc, DocsChangeResult, read_document_type_and_header, read_peer_header},
    feed::{FeedDiscoveryKey, FeedPublicKey},
    DocUrlInfo, DocumentId, DocumentInfo, NameDescription, PeerId, document::DocumentWriteSettings,
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
    pub(crate) document_writer_settings: DocumentWriteSettings,
}
impl PeermergeState {
    pub(crate) fn new(
        default_peer_header: &NameDescription,
        document_ids: Vec<DocumentId>,
        document_writer_settings: DocumentWriteSettings

    ) -> Self {
        let peer_id: PeerId = *Uuid::new_v4().as_bytes();
        Self::new_with_version(
            PEERMERGE_VERSION,
            peer_id,
            default_peer_header.clone(),
            document_ids,
            document_writer_settings
        )
    }

    pub(crate) fn new_with_version(
        version: u8,
        peer_id: PeerId,
        default_peer_header: NameDescription,
        document_ids: Vec<DocumentId>,
        document_writer_settings: DocumentWriteSettings
    ) -> Self {
        Self {
            version,
            peer_id,
            default_peer_header,
            document_ids,
            document_writer_settings
        }
    }
}

/// Stores serialized information about a single document.
#[derive(Debug)]
pub(crate) struct DocumentState {
    pub(crate) version: u8,
    /// Document id. Derived from doc_discovery_key.
    pub(crate) document_id: DocumentId,
    /// Is the document encrypted. If None it is unknown and proxy must be true.
    pub(crate) encrypted: Option<bool>,
    /// Is this a proxy document.
    pub(crate) proxy: bool,
    /// State for all of the feeds that are not the doc feed.
    pub(crate) feeds_state: DocumentFeedsState,
    /// Content of the document. None for proxy.
    pub(crate) content: Option<DocumentContent>,
}
impl DocumentState {
    pub(crate) fn new(
        proxy: bool,
        encrypted: Option<bool>,
        feeds_state: DocumentFeedsState,
        content: Option<DocumentContent>,
    ) -> Self {
        Self::new_with_version(
            PEERMERGE_VERSION,
            proxy,
            encrypted,
            feeds_state,
            content,
        )
    }

    pub(crate) fn new_with_version(
        version: u8,
        proxy: bool,
        encrypted: Option<bool>,
        feeds_state: DocumentFeedsState,
        content: Option<DocumentContent>,
    ) -> Self {
        let document_id = document_id_from_discovery_key(&feeds_state.doc_discovery_key);
        Self {
            version,
            document_id,
            encrypted,
            proxy,
            feeds_state,
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
        encode_doc_url(&self.feeds_state.doc_public_key, false, &None, &None)
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
                &self.feeds_state.doc_public_key,
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
                self.feeds_state.doc_public_key,
                self.feeds_state.doc_discovery_key,
                self.document_id,
            )
        } else {
            DocUrlInfo::new(
                self.version,
                false, // TODO: child documents
                crate::FeedType::Hypercore,
                self.feeds_state.doc_public_key,
                self.feeds_state.doc_discovery_key,
                self.document_id,
                self.encrypted.unwrap(),
            )
        }
    }

    pub(crate) fn document_type_and_header(&self) -> Option<(String, Option<NameDescription>)> {
        if let Some(content) = &self.content {
            if let Some(meta_automerge_doc) = &content.meta_automerge_doc {
                read_document_type_and_header(meta_automerge_doc)
            } else {
                None
            }
        } else {
            None
        }
    }

    pub(crate) fn peer_ids(&self) -> Vec<PeerId> {
        let mut peer_ids: Vec<PeerId> = self.feeds_state.other_feeds.iter().map(|other_feed| other_feed.peer_id).collect();
        if let Some(write_feed) = &self.feeds_state.write_feed {
            peer_ids.push(write_feed.peer_id);
        }
        peer_ids.dedup();
        peer_ids
    }

    pub(crate) fn peer_header(&self, peer_id: &PeerId) -> Option<NameDescription> {
        if let Some(content) = &self.content {
            if let Some(meta_automerge_doc) = &content.meta_automerge_doc {
                read_peer_header(meta_automerge_doc, peer_id)
            } else {
                None
            }
        } else {
            None
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct DocumentFeedsState {
    /// Doc feed's public key
    pub(crate) doc_public_key: FeedPublicKey,
    /// Doc feed's discovery key. Derived from doc_public_key.
    pub(crate) doc_discovery_key: FeedDiscoveryKey,
    /// Id and public key of personal writeable feed. None if proxy.
    pub(crate) write_feed: Option<DocumentFeedInfo>,
    /// Id and public key of peers' feeds and also our replaced write
    /// feeds.
    pub(crate) other_feeds: Vec<DocumentFeedInfo>,
}

impl DocumentFeedsState {
    pub(crate) fn new(doc_public_key: FeedPublicKey) -> Self {
        Self::new_from_data(doc_public_key, None, vec![])
    }

    pub(crate) fn new_writer(
        doc_public_key: FeedPublicKey,
        peer_id: &PeerId, write_public_key: &FeedPublicKey) -> Self {
        Self::new_from_data(
            doc_public_key,
            Some(DocumentFeedInfo::new(*peer_id, *write_public_key, None)),
            vec![],
        )
    }

    pub(crate) fn new_from_data(
        doc_public_key: FeedPublicKey,
        mut write_feed: Option<DocumentFeedInfo>,
        mut other_feeds: Vec<DocumentFeedInfo>,
    ) -> Self {
        let doc_discovery_key = discovery_key_from_public_key(&doc_public_key);
        // For state, populate the discovery keys
        if let Some(write_feed) = write_feed.as_mut() {
            write_feed.populate_discovery_key();
        }
        for other_feed in other_feeds.iter_mut() {
            other_feed.populate_discovery_key();
        }
        Self {
            doc_public_key,
            doc_discovery_key,
            write_feed,
            other_feeds,
        }
    }

    pub(crate) fn compare_broadcasted_feeds(
        &self,
        remote_write_feed: Option<DocumentFeedInfo>,
        remote_other_feeds: Vec<DocumentFeedInfo>,
    ) -> (bool, Vec<DocumentFeedInfo>) {
        let stored_feeds_found: bool = {
            let stored_write_feed_found = if let Some(stored_write_feed) = &self.write_feed {
                remote_other_feeds.contains(stored_write_feed)
            } else {
                true
            };
            stored_write_feed_found
                && self.other_feeds.iter().all(|stored_feed| {
                    let remote_write_feed_matches =
                        if let Some(remote_write_feed) = &remote_write_feed {
                            stored_feed == remote_write_feed
                        } else {
                            false
                        };
                    remote_write_feed_matches || remote_other_feeds.contains(stored_feed)
                })
        };
        let mut new_remote_feeds: Vec<DocumentFeedInfo> =
            if let Some(remote_write_feed) = remote_write_feed {
                if !self.other_feeds.iter().any(|feed| {
                    feed.peer_id == remote_write_feed.peer_id
                        && feed.public_key == remote_write_feed.public_key
                }) {
                    vec![remote_write_feed]
                } else {
                    vec![]
                }
            } else {
                vec![]
            };
        remote_other_feeds.into_iter().for_each(|remote_other_feed| {
            let writable_matches: bool = if let Some(write_feed) = &self.write_feed {
                write_feed.peer_id == remote_other_feed.peer_id
                    && write_feed.public_key == remote_other_feed.public_key
            } else {
                false
            };

            if !writable_matches
                && !self.other_feeds.iter().any(|stored_feed| {
                    stored_feed.peer_id == remote_other_feed.peer_id
                        && stored_feed.public_key == remote_other_feed.public_key
                        // If the replaced feed is exactly the same, or then if
                        // we have newer info, then this is not a new feed.
                        && (stored_feed.replaced_by_public_key
                            == remote_other_feed.replaced_by_public_key || 
                            (stored_feed.replaced_by_public_key.is_some() 
                             && remote_other_feed.replaced_by_public_key.is_none()))
                })
            {
                new_remote_feeds.push(remote_other_feed);
            }
        });

        (stored_feeds_found, new_remote_feeds)
    }

    /// Merge incoming feeds into existing feeds
    pub(crate) fn merge_new_feeds(
        &mut self,
        new_feeds: &[DocumentFeedInfo],
    ) -> (bool, Vec<DocumentFeedInfo>, Vec<DocumentFeedInfo>) {
        // First make sure that some other thread didn't already
        // merge some of these, in which case there are less changed
        // feeds than new feeds.
        let changed_feeds: Vec<DocumentFeedInfo> = new_feeds
            .iter()
            .filter(|incoming_feed| {
                !self
                    .other_feeds
                    .iter()
                    .any(|stored_feed| stored_feed == *incoming_feed)
            })
            .cloned()
            .collect();
        let feeds_to_create: Vec<DocumentFeedInfo> = changed_feeds
            .iter()
            .filter(|changed_feed| changed_feed.replaced_by_public_key.is_none())
            .cloned()
            .collect();

        let (changed, replaced_feeds): (bool, Vec<DocumentFeedInfo>) = {
            if changed_feeds.is_empty() {
                (false, vec![])
            } else {
                // Find out if there are values currently which
                // differ only in that replaced_by_public_key has
                // been set.
                let to_be_mutated_feeds: Vec<&mut DocumentFeedInfo> = self
                    .other_feeds
                    .iter_mut()
                    .filter(|feed| {
                        changed_feeds.iter().any(|changed_feed| {
                            changed_feed.peer_id == feed.peer_id
                                && changed_feed.public_key == feed.public_key
                                && feed.replaced_by_public_key
                                    != changed_feed.replaced_by_public_key
                                && changed_feed.replaced_by_public_key.is_some()
                        })
                    })
                    .collect();
                if to_be_mutated_feeds.is_empty() {
                    // Just append the new feeds to the end, they are all new
                    let to_add_feeds: Vec<DocumentFeedInfo> = changed_feeds
                        .iter()
                        .map(|feed| {
                            let mut add_feed = feed.clone();
                            add_feed.populate_discovery_key();
                            add_feed
                        })
                        .collect();
                    self.other_feeds.extend(to_add_feeds);
                    (true, vec![])
                } else {
                    // Get the feeds that were replaced
                    let replaced_feeds: Vec<DocumentFeedInfo> = to_be_mutated_feeds
                        .into_iter()
                        .map(|to_be_mutated_feed| {
                            let replacement = changed_feeds
                                .iter()
                                .find(|new_feed| {
                                    new_feed.peer_id == to_be_mutated_feed.peer_id
                                        && new_feed.public_key == to_be_mutated_feed.public_key
                                })
                                .unwrap()
                                .clone();
                            to_be_mutated_feed.replaced_by_public_key =
                                replacement.replaced_by_public_key;
                            replacement
                        })
                        .collect();
                    // The rest can just be pushed in
                    let to_add_feeds: Vec<DocumentFeedInfo> = changed_feeds
                        .iter()
                        .filter(|changed_feed| !replaced_feeds.contains(changed_feed))
                        .map(|feed| {
                            let mut add_feed = feed.clone();
                            add_feed.populate_discovery_key();
                            add_feed
                        })
                        .collect();
                    self.other_feeds.extend(to_add_feeds);

                    (true, replaced_feeds)
                }
            }
        };
        (changed, replaced_feeds, feeds_to_create)
    }

    /// Replace the write feed's public key, returns the replaced_feeds and feeds_to_create.
    #[allow(dead_code)] // TODO: Remote when implemented
    pub(crate) fn replace_write_public_key(
        &mut self,
        new_write_public_key: FeedPublicKey,
    ) -> (Vec<DocumentFeedInfo>, Vec<DocumentFeedInfo>) {
        let mut replaced_write_feed = self.write_feed.clone().unwrap();
        let mut new_write_feed =
            DocumentFeedInfo::new(replaced_write_feed.peer_id, new_write_public_key, None);
        new_write_feed.populate_discovery_key();
        self.write_feed = Some(new_write_feed.clone());
        replaced_write_feed.replaced_by_public_key = Some(new_write_public_key);
        self.other_feeds.push(replaced_write_feed.clone());

        // Return values without discovery keys
        replaced_write_feed.discovery_key = None;
        new_write_feed.discovery_key = None;
        (vec![replaced_write_feed], vec![new_write_feed])
    }

    /// Set result received from merge_incoming_peers or replace_write_public_key into this feeds state
    pub(crate) fn set_replaced_feeds_and_feeds_to_create(
        &mut self,
        replaced_feeds: Vec<DocumentFeedInfo>,
        mut feeds_to_create: Vec<DocumentFeedInfo>,
    ) {
        if let Some(write_feed) = self.write_feed.as_mut() {
            if let Some(replaced_write_feed) = replaced_feeds.iter().find(|replaced_feed| {
                replaced_feed.peer_id == write_feed.peer_id
                    && replaced_feed.public_key == write_feed.public_key
            }) {
                let mut old_write_feed = write_feed.clone();
                old_write_feed.replaced_by_public_key = replaced_write_feed.replaced_by_public_key;

                // The write feed needs to be replaced, the new one needs to be in feeds_to_create
                let mut write_feed_index: Option<usize> = None;
                for (i, feed_to_create) in feeds_to_create.iter().enumerate() {
                    if Some(feed_to_create.public_key) == replaced_write_feed.replaced_by_public_key
                    {
                        write_feed_index = Some(i);
                    }
                }
                if let Some(write_feed_index) = write_feed_index {
                    *write_feed = feeds_to_create
                        .drain(write_feed_index..write_feed_index + 1)
                        .collect::<Vec<DocumentFeedInfo>>()
                        .into_iter()
                        .next()
                        .unwrap();
                    write_feed.populate_discovery_key();
                } else {
                    panic!("Invalid write feed change parameters, new write feed missing");
                }

                // Put the old write feed to the other_feeds
                self.other_feeds.push(old_write_feed);
            }
        }

        self.other_feeds.iter_mut().for_each(|stored_feed| {
            if let Some(replaced_feed) = replaced_feeds.iter().find(|replaced_feed| {
                replaced_feed.public_key == stored_feed.public_key
                    && replaced_feed.peer_id == stored_feed.peer_id
                    && stored_feed.replaced_by_public_key.is_none()
                    && replaced_feed.replaced_by_public_key.is_some()
            }) {
                stored_feed.replaced_by_public_key = replaced_feed.replaced_by_public_key;
            }
        });
        feeds_to_create.into_iter().for_each(|mut feed_to_create| {
            feed_to_create.populate_discovery_key();
            self.other_feeds.push(feed_to_create)
        });
    }

    pub(crate) fn peer_id(&self, discovery_key: &FeedDiscoveryKey) -> PeerId {
        let peer = self
            .other_feeds
            .iter()
            .find(|peer| &peer.discovery_key.unwrap() == discovery_key);
        if let Some(peer) = peer {
            return peer.peer_id;
        } else if let Some(write_peer) = &self.write_feed {
            if &write_peer.discovery_key.unwrap() == discovery_key {
                return write_peer.peer_id;
            }
        }
        panic!("We should always have a peer id for every discovery key")
    }
}

#[derive(Clone, Debug, PartialOrd, Eq, Ord)]
pub(crate) struct DocumentFeedInfo {
    /// Id of the peer.
    pub(crate) peer_id: PeerId,
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

impl PartialEq for DocumentFeedInfo {
    #[inline]
    fn eq(&self, other: &DocumentFeedInfo) -> bool {
        self.peer_id == other.peer_id
            && self.public_key == other.public_key
            && self.replaced_by_public_key == other.replaced_by_public_key
    }
}

impl DocumentFeedInfo {
    pub(crate) fn new(
        id: PeerId,
        public_key: [u8; 32],
        replaced_by_public_key: Option<FeedPublicKey>,
    ) -> Self {
        Self {
            peer_id: id,
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
    /// Peer id, stored redundanty to be able to use for actor id
    /// when loading document.
    pub(crate) peer_id: PeerId,
    /// Cursors of feeds which have been read to get the below fields.
    /// Can also be empty when meta_doc_data is read from a doc URL.
    pub(crate) cursors: Vec<DocumentCursor>,
    /// Data blob containing meta_automerge_doc.
    pub(crate) meta_doc_data: Vec<u8>,
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
    pub(crate) fn new(
        peer_id: PeerId,
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
            peer_id,
            cursors,
            meta_doc_data,
            user_doc_data,
            meta_automerge_doc: Some(meta_automerge_doc),
            user_automerge_doc,
        }
    }

    pub(crate) fn is_bootsrapped(&self) -> bool {
        self.user_doc_data.is_some()
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

    pub(crate) fn meta_automerge_doc_mut(&mut self) -> Option<&mut AutomergeDoc> {
        if let Some(meta_automerge_doc) = self.meta_automerge_doc.as_mut() {
            Some(meta_automerge_doc)
        } else {
            None
        }
    }

    pub(crate) fn docs_mut(&mut self) -> Option<(&mut AutomergeDoc, &mut AutomergeDoc)> {
        if let Some(meta_automerge_doc) = self.meta_automerge_doc.as_mut() {
            self.user_automerge_doc
                .as_mut()
                .map(|user_automerge_doc| (meta_automerge_doc, user_automerge_doc))
        } else {
            None
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
            self.meta_doc_data = save_automerge_doc(meta_automerge_doc);
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

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn feeds_state_write_all_new_remote() -> anyhow::Result<()> {
        let doc_public_key: [u8; 32] = [100; 32];
        let my_id: [u8; 16] = [0; 16];
        let my_write_feed: DocumentFeedInfo = DocumentFeedInfo::new(my_id, [0; 32], None);
        let peer_1_id: [u8; 16] = [1; 16];
        let mut peer_1_write: DocumentFeedInfo = DocumentFeedInfo::new(peer_1_id, [1; 32], None);
        let peer_2_id: [u8; 16] = [2; 16];
        let peer_2: DocumentFeedInfo = DocumentFeedInfo::new(peer_2_id, [2; 32], None);
        let peer_3_id: [u8; 16] = [3; 16];
        let peer_3: DocumentFeedInfo = DocumentFeedInfo::new(peer_3_id, [3; 32], None);

        let mut fs = DocumentFeedsState::new_from_data(doc_public_key, Some(my_write_feed.clone()), vec![]);
        let mut fs_copy = fs.clone();
        assert_eq!(fs, fs_copy);

        // Remote sends new peers and doesn't know about our write feed
        let (stored_feeds_found, new_remote_feeds) = fs.compare_broadcasted_feeds(
            Some(peer_1_write.clone()),
            vec![peer_2.clone(), peer_3.clone()],
        );
        assert!(!stored_feeds_found);
        assert_eq!(
            new_remote_feeds,
            vec![peer_1_write.clone(), peer_2.clone(), peer_3.clone()]
        );

        let (changed, replaced_feeds, feeds_to_create) = fs.merge_new_feeds(&new_remote_feeds);
        assert!(changed);
        assert!(replaced_feeds.is_empty());
        assert_eq!(
            feeds_to_create,
            vec![peer_1_write.clone(), peer_2.clone(), peer_3.clone()]
        );
        fs_copy.set_replaced_feeds_and_feeds_to_create(replaced_feeds, feeds_to_create);
        assert_eq!(fs, fs_copy);

        // Remote sends our feed as well
        let (stored_feeds_found, new_remote_feeds) = fs.compare_broadcasted_feeds(
            Some(peer_1_write.clone()),
            vec![peer_3.clone(), peer_2.clone(), my_write_feed.clone()],
        );
        assert!(stored_feeds_found);
        assert!(new_remote_feeds.is_empty());

        // We replace our write feed
        let my_new_write_feed_pk: [u8; 32] = [4; 32];
        let my_new_write_feed =
            DocumentFeedInfo::new(my_write_feed.peer_id, my_new_write_feed_pk, None);
        let my_replaced_write_feed = DocumentFeedInfo::new(
                my_write_feed.peer_id,
                my_write_feed.public_key,
                Some(my_new_write_feed_pk)
            );
        let (replaced_feeds, feeds_to_create) = fs.replace_write_public_key(my_new_write_feed_pk);
        assert_eq!(fs.write_feed, Some(my_new_write_feed.clone()));
        assert_eq!(
            replaced_feeds,
            vec![my_replaced_write_feed.clone()]
        );
        assert_eq!(feeds_to_create, vec![my_new_write_feed.clone()]);
        fs_copy.set_replaced_feeds_and_feeds_to_create(replaced_feeds, feeds_to_create);
        assert_eq!(fs, fs_copy);

        // The peer replaces their write key too
        let new_peer_1_write_pk: [u8; 32] = [5; 32];
        let new_peer_1_write: DocumentFeedInfo =
            DocumentFeedInfo::new(peer_1_id, new_peer_1_write_pk, None);
        peer_1_write.replaced_by_public_key = Some(new_peer_1_write_pk);
        let (stored_feeds_found, new_remote_feeds) = fs.compare_broadcasted_feeds(
            Some(new_peer_1_write.clone()),
            vec![
                peer_3.clone(),
                peer_2.clone(),
                my_write_feed,
                peer_1_write.clone(),
            ],
        );
        assert!(!stored_feeds_found);
        assert_eq!(new_remote_feeds, vec![new_peer_1_write.clone(), peer_1_write.clone()]);
        let (changed, replaced_feeds, feeds_to_create) = fs.merge_new_feeds(&new_remote_feeds);
        assert!(changed);
        assert_eq!(replaced_feeds, vec![peer_1_write.clone()]);
        assert_eq!(
            feeds_to_create,
            vec![new_peer_1_write.clone()]
        );
        fs_copy.set_replaced_feeds_and_feeds_to_create(replaced_feeds, feeds_to_create);
        assert_eq!(fs, fs_copy);

        // Remote sends our changed feed as well
        let (stored_feeds_found, new_remote_feeds) = fs.compare_broadcasted_feeds(
            Some(new_peer_1_write),
            vec![
                peer_3,
                peer_2,
                my_new_write_feed,
                my_replaced_write_feed,
                peer_1_write,
            ],
        );
        assert!(stored_feeds_found);
        assert!(new_remote_feeds.is_empty());
        Ok(())
    }
}
