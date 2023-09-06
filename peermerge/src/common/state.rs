use std::{
    collections::HashMap,
    fmt::Debug,
    hash::{Hash, Hasher},
};

use hypercore_protocol::hypercore::{sign, SigningKey, VerifyingKey};
use tracing::warn;
use uuid::Uuid;

use crate::{
    crdt::{
        read_document_type_and_header, read_peer_header, save_automerge_doc, AutomergeDoc,
        DocsChangeResult,
    },
    document::DocumentSettings,
    feeds::{FeedDiscoveryKey, FeedPublicKey},
    AccessType, DocumentId, DocumentInfo, DynamicDocumentInfo, NameDescription, PeerId,
    PeermergeError, StaticDocumentInfo,
};

use super::{
    cipher::{encode_doc_url, verify_data_signature, DocUrlAppendix},
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
    pub(crate) document_settings: DocumentSettings,
}
impl PeermergeState {
    pub(crate) fn new(
        default_peer_header: &NameDescription,
        document_ids: Vec<DocumentId>,
        document_settings: DocumentSettings,
    ) -> Self {
        let peer_id: PeerId = *Uuid::new_v4().as_bytes();
        Self::new_with_version(
            PEERMERGE_VERSION,
            peer_id,
            default_peer_header.clone(),
            document_ids,
            document_settings,
        )
    }

    pub(crate) fn new_with_version(
        version: u8,
        peer_id: PeerId,
        default_peer_header: NameDescription,
        document_ids: Vec<DocumentId>,
        document_settings: DocumentSettings,
    ) -> Self {
        Self {
            version,
            peer_id,
            default_peer_header,
            document_ids,
            document_settings,
        }
    }
}

/// Stores serialized information about a single document.
#[derive(Debug)]
pub(crate) struct DocumentState {
    pub(crate) version: u8,
    /// Document id. Derived from doc_discovery_key.
    pub(crate) document_id: DocumentId,
    /// Document signature verifying key in bytes.
    pub(crate) doc_signature_verifying_key: [u8; 32],
    /// Is the document encrypted. If None it is unknown and proxy must be true.
    pub(crate) encrypted: Option<bool>,
    /// Access to the document.
    pub(crate) access_type: AccessType,
    /// State for all of the feeds involved in this document
    pub(crate) feeds_state: DocumentFeedsState,
    /// Content of the document. None for proxy.
    pub(crate) content: Option<DocumentContent>,
    /// Child documents of this document
    pub(crate) child_documents: Vec<ChildDocumentInfo>,
}
impl DocumentState {
    pub(crate) fn new(
        access_type: AccessType,
        doc_signature_verifying_key: [u8; 32],
        encrypted: Option<bool>,
        feeds_state: DocumentFeedsState,
        content: Option<DocumentContent>,
    ) -> Self {
        Self::new_with_version(
            PEERMERGE_VERSION,
            access_type,
            doc_signature_verifying_key,
            encrypted,
            feeds_state,
            content,
            vec![],
        )
    }

    pub(crate) fn new_with_version(
        version: u8,
        access_type: AccessType,
        doc_signature_verifying_key: [u8; 32],
        encrypted: Option<bool>,
        feeds_state: DocumentFeedsState,
        content: Option<DocumentContent>,
        child_documents: Vec<ChildDocumentInfo>,
    ) -> Self {
        let document_id = document_id_from_discovery_key(&feeds_state.doc_discovery_key);
        Self {
            version,
            access_type,
            doc_signature_verifying_key,
            document_id,
            encrypted,
            feeds_state,
            content,
            child_documents,
        }
    }

    pub(crate) fn info(&mut self) -> DocumentInfo {
        let doc_url_info = self.doc_url_info();
        if let Some((document_type, document_header)) = self.document_type_and_header() {
            DocumentInfo {
                encrypted: self.encrypted.clone(),
                access_type: self.access_type,
                static_info: doc_url_info,
                dynamic_info: Some(DynamicDocumentInfo {
                    document_type,
                    document_header,
                }),
                parent_document_id: None, // TODO: Support for document hierarchies
            }
        } else {
            DocumentInfo {
                encrypted: self.encrypted.clone(),
                access_type: self.access_type,
                static_info: doc_url_info,
                dynamic_info: None,
                parent_document_id: None, // TODO: Support for document hierarchies
            }
        }
    }

    pub(crate) fn doc_url(
        &mut self,
        initial_meta_doc_data: Vec<u8>,
        doc_signature_signing_key: &SigningKey,
        encryption_key: &Option<Vec<u8>>,
    ) -> String {
        if let Some((document_type, document_header)) = self.document_type_and_header() {
            encode_doc_url(
                &self.feeds_state.doc_public_key,
                doc_signature_signing_key,
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

    pub(crate) fn doc_url_info(&self) -> StaticDocumentInfo {
        StaticDocumentInfo::new(
            self.version,
            crate::FeedType::Hypercore,
            false, // TODO: Child documents
            self.feeds_state.doc_public_key,
            self.feeds_state.doc_discovery_key,
            self.document_id,
            VerifyingKey::from_bytes(&self.doc_signature_verifying_key).expect(
                "It should never be possible an invalid verifying key is stored to the state",
            ),
        )
    }

    pub(crate) fn document_type_and_header(&mut self) -> Option<(String, Option<NameDescription>)> {
        if let Some(content) = self.content.as_mut() {
            if let Some(meta_automerge_doc) = &content.meta_automerge_doc {
                let stored_type_and_header = read_document_type_and_header(meta_automerge_doc);
                if stored_type_and_header.is_some() {
                    if content.temporary_document_type.is_some() {
                        // Remove temporary values when dynamic values are found
                        content.temporary_document_type = None;
                        content.temporary_document_header = None;
                    }
                    stored_type_and_header
                } else {
                    // Return temporarily stored values
                    Some((
                        content
                            .temporary_document_type
                            .clone()
                            .expect("Temporary document type should always have a value when meta document doesn't"),
                        content.temporary_document_header.clone(),
                    ))
                }
            } else {
                panic!("Meta automerge doc should always be initialized");
            }
        } else {
            None
        }
    }

    pub(crate) fn peer_ids(&self) -> Vec<PeerId> {
        let mut peer_ids: Vec<PeerId> = self
            .feeds_state
            .other_feeds
            .keys()
            .into_iter()
            .cloned()
            .collect();
        if let Some(write_feed) = &self.feeds_state.write_feed {
            if !peer_ids.contains(&write_feed.peer_id) {
                peer_ids.push(write_feed.peer_id);
            }
        }
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

#[derive(Clone, Debug)]
pub(crate) struct ChildDocumentInfo {
    /// Doc feed's public key
    pub(crate) doc_public_key: FeedPublicKey,
    /// Doc signature verifying key.
    pub(crate) doc_signature_verifying_key: VerifyingKey,
    /// Signature of doc_public_key and doc_signature_verifying_key signed
    /// with the doc signature key of the parent document. This is needed
    /// to prevent rogue proxies or read-only peers from announcing bogus
    /// documents with the broadcast message.
    pub(crate) signature: Vec<u8>,
    /// Whether or not the document creation is pending metadoc to
    /// contain the encryption and/or signing key.
    pub(crate) creation_pending: bool,
}

impl PartialEq for ChildDocumentInfo {
    #[inline]
    fn eq(&self, other: &ChildDocumentInfo) -> bool {
        self.doc_public_key == other.doc_public_key
            && self.doc_signature_verifying_key == other.doc_signature_verifying_key
            && self.signature == other.signature
    }
}

impl Hash for ChildDocumentInfo {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.doc_public_key.hash(state);
        self.doc_signature_verifying_key.hash(state);
        self.signature.hash(state);
    }
}

impl ChildDocumentInfo {
    pub(crate) fn new_from_data(
        doc_public_key: FeedPublicKey,
        doc_signature_verifying_key: [u8; 32],
        signature: Vec<u8>,
        creation_pending: bool,
    ) -> Self {
        Self {
            doc_public_key,
            doc_signature_verifying_key: VerifyingKey::from_bytes(&doc_signature_verifying_key)
                .expect(
                    "It should never be possible an invalid verifying key is stored to the state",
                ),
            signature,
            creation_pending,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct DocumentFeedsState {
    /// Local peer's id
    pub(crate) peer_id: PeerId,
    /// Doc feed's public key
    pub(crate) doc_public_key: FeedPublicKey,
    /// Doc feed's discovery key. Derived from doc_public_key.
    pub(crate) doc_discovery_key: FeedDiscoveryKey,
    /// Whether or not the doc feed has been verified.
    pub(crate) doc_feed_verified: bool,
    /// Id and public key of personal writeable feed. None if proxy.
    pub(crate) write_feed: Option<DocumentFeedInfo>,
    /// Id and public key of peers' feeds and also our old replaced
    /// write feeds.
    pub(crate) other_feeds: HashMap<PeerId, Vec<DocumentFeedInfo>>,
}

#[derive(Clone, Debug)]
pub(crate) struct CompareBroadcastedFeedsResult {
    pub(crate) stored_active_feeds_found: bool,
    pub(crate) new_feeds: Vec<DocumentFeedInfo>,
    pub(crate) inactive_feeds_to_rebroadcast: Vec<DocumentFeedInfo>,
    pub(crate) wait_for_rebroadcast: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct ChangeDocumentFeedsStateResult {
    pub(crate) changed: bool,
    pub(crate) replaced_feeds: Vec<DocumentFeedInfo>,
    pub(crate) feeds_to_create: Vec<DocumentFeedInfo>,
}

impl ChangeDocumentFeedsStateResult {
    pub(crate) fn unchanged() -> Self {
        Self {
            changed: false,
            replaced_feeds: vec![],
            feeds_to_create: vec![],
        }
    }
}

impl DocumentFeedsState {
    pub(crate) fn new(
        peer_id: PeerId,
        doc_public_key: FeedPublicKey,
        doc_feed_verified: bool,
    ) -> Self {
        Self::new_from_data(peer_id, doc_public_key, doc_feed_verified, None, vec![])
    }

    pub(crate) fn new_writer(
        peer_id: PeerId,
        doc_public_key: FeedPublicKey,
        doc_feed_verified: bool,
        write_public_key: &FeedPublicKey,
        doc_signature_signing_key: &SigningKey,
    ) -> Self {
        Self::new_from_data(
            peer_id,
            doc_public_key,
            doc_feed_verified,
            Some(DocumentFeedInfo::new(
                peer_id,
                *write_public_key,
                None,
                doc_signature_signing_key,
            )),
            vec![],
        )
    }

    pub(crate) fn new_from_data(
        peer_id: PeerId,
        doc_public_key: FeedPublicKey,
        doc_feed_verified: bool,
        mut write_feed: Option<DocumentFeedInfo>,
        stored_other_feeds: Vec<DocumentFeedInfo>,
    ) -> Self {
        let doc_discovery_key = discovery_key_from_public_key(&doc_public_key);
        // For state, populate the discovery keys
        if let Some(write_feed) = write_feed.as_mut() {
            write_feed.verified = true;
            write_feed.populate_discovery_key();
        }
        let mut other_feeds: HashMap<PeerId, Vec<DocumentFeedInfo>> = HashMap::new();
        for mut stored_other_feed in stored_other_feeds.into_iter() {
            stored_other_feed.populate_discovery_key();
            if let Some(existing_feeds) = other_feeds.get_mut(&stored_other_feed.peer_id).as_mut() {
                existing_feeds.push(stored_other_feed);
            } else {
                other_feeds.insert(stored_other_feed.peer_id, vec![stored_other_feed]);
            }
        }
        Self {
            peer_id,
            doc_public_key,
            doc_discovery_key,
            doc_feed_verified,
            write_feed,
            other_feeds,
        }
    }

    /// Gets all active, verified feeds
    pub(crate) fn active_peer_feeds(
        &self,
        tolerate_unverified: bool,
    ) -> Vec<(usize, DocumentFeedInfo)> {
        let mut active_feeds: Vec<(usize, DocumentFeedInfo)> = vec![];
        for (peer_id, feeds) in &self.other_feeds {
            if peer_id != &self.peer_id {
                let replaced_public_keys: Vec<FeedPublicKey> = feeds
                    .iter()
                    .filter_map(|feed| {
                        if !tolerate_unverified && !feed.verified {
                            None
                        } else {
                            feed.replaced_public_key
                        }
                    })
                    .collect();
                for (index, feed) in feeds.iter().enumerate().rev() {
                    if (tolerate_unverified || feed.verified)
                        && !replaced_public_keys.contains(&feed.public_key)
                    {
                        active_feeds.push((index, feed.clone()));
                        // There can't be more than one, we expect to break
                        // immediately as the feeds are stored in reverse
                        // chronological order.
                        break;
                    }
                }
            }
        }
        active_feeds
    }

    /// Gets all removable feeds
    pub(crate) fn removable_feeds(&self) -> Vec<DocumentFeedInfo> {
        let mut removable_feeds: Vec<DocumentFeedInfo> = vec![];
        let active_peer_feeds = self.active_peer_feeds(false);
        for feeds in self.other_feeds.values() {
            for feed in feeds.iter().rev() {
                if !active_peer_feeds
                    .iter()
                    .map(|(_, feed)| feed)
                    .collect::<Vec<&DocumentFeedInfo>>()
                    .contains(&feed)
                    && !feed.removed
                {
                    removable_feeds.push(feed.clone());
                }
            }
        }
        removable_feeds
    }

    pub(crate) fn replaced_public_key_exists(
        &self,
        peer_id: &PeerId,
        replaced_public_key: &Option<FeedPublicKey>,
        remote_inactive_feeds: &Option<Vec<DocumentFeedInfo>>,
    ) -> bool {
        if let Some(replaced_public_key) = replaced_public_key {
            if let Some(stored_feeds) = self.other_feeds.get(peer_id) {
                if stored_feeds
                    .iter()
                    .any(|feed| &feed.public_key == replaced_public_key)
                {
                    true
                } else if let Some(remote_inactive_feeds) = remote_inactive_feeds {
                    remote_inactive_feeds
                        .iter()
                        .any(|feed| &feed.public_key == replaced_public_key)
                } else {
                    false
                }
            } else {
                false
            }
        } else {
            true
        }
    }

    pub(crate) fn compare_broadcasted_feeds(
        &self,
        mut remote_write_feed: Option<DocumentFeedInfo>,
        mut remote_active_feeds: Vec<DocumentFeedInfo>,
        remote_inactive_feeds: Option<Vec<DocumentFeedInfo>>,
    ) -> Result<CompareBroadcastedFeedsResult, PeermergeError> {
        let mut stored_active_peer_feeds = self.active_peer_feeds(true);
        let mut wait_for_rebroadcast = false;

        // First trim the remote_write_feed and remote_active_feeds and to not have any
        // of the active feeds we sent out.
        let stored_active_feeds_found: bool = {
            let local_write_feed_found = if let Some(stored_write_feed) = &self.write_feed {
                if let Some(index) = remote_active_feeds
                    .iter()
                    .position(|feed| feed == stored_write_feed)
                {
                    remote_active_feeds
                        .drain(index..index + 1)
                        .into_iter()
                        .next()
                        .is_some()
                } else {
                    false
                }
            } else {
                true
            };

            let remote_write_feed_stored_active_feeds_index =
                if let Some(remote_write_feed) = &remote_write_feed {
                    stored_active_peer_feeds
                        .iter()
                        .position(|(_, stored_active_feed)| stored_active_feed == remote_write_feed)
                } else {
                    None
                };
            if let Some(index) = remote_write_feed_stored_active_feeds_index {
                stored_active_peer_feeds.drain(index..index + 1);
                remote_write_feed.take();
            }

            let remote_active_feeds_original_len = remote_active_feeds.len();
            let mut drain_count = 0;
            for i in 0..remote_active_feeds_original_len {
                let remote_active_feed_index = i - drain_count;
                if remote_active_feed_index >= remote_active_feeds.len() {
                    break;
                }
                let remote_active_feed = &remote_active_feeds[remote_active_feed_index];
                if let Some(stored_active_feeds_index) = stored_active_peer_feeds
                    .iter()
                    .position(|(_, feed)| feed == remote_active_feed)
                {
                    remote_active_feeds
                        .drain(remote_active_feed_index..remote_active_feed_index + 1);
                    stored_active_peer_feeds
                        .drain(stored_active_feeds_index..stored_active_feeds_index + 1);
                    drain_count += 1;
                }
            }
            stored_active_peer_feeds.is_empty() && local_write_feed_found
        };

        let mut new_remote_feeds_map: HashMap<PeerId, Vec<DocumentFeedInfo>> = HashMap::new();
        if let Some(remote_inactive_feeds) = &remote_inactive_feeds {
            for remote_inactive_feed in remote_inactive_feeds {
                if let Some(stored_feeds) = self.other_feeds.get(&remote_inactive_feed.peer_id) {
                    if !stored_feeds.contains(remote_inactive_feed) {
                        let new_feed = remote_inactive_feed.clone();
                        if let Some(new_remote_feeds) =
                            new_remote_feeds_map.get_mut(&new_feed.peer_id)
                        {
                            if let Some(previous_index) = new_remote_feeds.iter().position(|feed| {
                                Some(feed.public_key) == new_feed.replaced_public_key
                            }) {
                                new_remote_feeds
                                    .splice(previous_index + 1..previous_index + 1, [new_feed]);
                            } else {
                                new_remote_feeds.push(new_feed);
                            }
                        } else {
                            new_remote_feeds_map.insert(new_feed.peer_id, vec![new_feed]);
                        }
                    }
                }
            }
        }
        if let Some(remote_write_feed) = remote_write_feed {
            // We didn't have this in our active feeds
            if self.replaced_public_key_exists(
                &remote_write_feed.peer_id,
                &remote_write_feed.replaced_public_key,
                &remote_inactive_feeds,
            ) {
                if let Some(new_remote_feeds) =
                    new_remote_feeds_map.get_mut(&remote_write_feed.peer_id)
                {
                    if let Some(previous_index) = new_remote_feeds.iter().position(|feed| {
                        Some(feed.public_key) == remote_write_feed.replaced_public_key
                    }) {
                        new_remote_feeds
                            .splice(previous_index + 1..previous_index + 1, [remote_write_feed]);
                    } else {
                        new_remote_feeds.push(remote_write_feed);
                    }
                } else {
                    new_remote_feeds_map.insert(remote_write_feed.peer_id, vec![remote_write_feed]);
                }
            } else {
                wait_for_rebroadcast = true;
            }
        };
        let mut inactive_feeds_to_rebroadcast: Vec<DocumentFeedInfo> = vec![];
        for remote_active_feed in remote_active_feeds {
            let mut is_new_feed = false;
            if let Some(stored_feeds) = self.other_feeds.get(&remote_active_feed.peer_id) {
                if let Some((remote_feed_stored_feeds_index, stored_feed_with_remote_pk)) =
                    stored_feeds
                        .iter()
                        .enumerate()
                        .rev()
                        .find(|(_, stored_feed)| {
                            stored_feed.public_key == remote_active_feed.public_key
                        })
                {
                    // Verify if the remote feed info is bogus because we have
                    if stored_feed_with_remote_pk.verified
                        && stored_feed_with_remote_pk.replaced_public_key
                            != remote_active_feed.replaced_public_key
                    {
                        return Err(PeermergeError::SecurityIssue {
                                context: format!(
                                    "Peer broadcasted feed with invalid replaced public key, \
                                        stored {stored_feed_with_remote_pk:?}, received {remote_active_feed:?}",
                                ),
                            });
                    }

                    if let Some((stored_active_feeds_index, (stored_feeds_index, _))) =
                        stored_active_peer_feeds
                            .iter()
                            .enumerate()
                            .find(|(_, (_, feed))| feed.peer_id == remote_active_feed.peer_id)
                    {
                        // If our index is larger than the stored index, it is our task
                        // to notify the other side about all of the replacements that
                        // came after this.
                        if remote_feed_stored_feeds_index < *stored_feeds_index {
                            for i in remote_feed_stored_feeds_index + 1..*stored_feeds_index {
                                if let Some(stored_inactive_feed) = stored_feeds.get(i) {
                                    if stored_inactive_feed.verified {
                                        inactive_feeds_to_rebroadcast
                                            .push(stored_inactive_feed.clone());
                                    } else {
                                        // Need to break on the first unverified or otherwise
                                        // the info has gaps, which is useless for the remote
                                        break;
                                    }
                                }
                            }
                            // Remove this peer from stored_active_feeds
                            stored_active_peer_feeds
                                .drain(stored_active_feeds_index..stored_active_feeds_index + 1);
                        }
                    }
                } else {
                    // We don't have knowledge of this public key at all within this peer's feeds
                    is_new_feed = true;
                }
            } else {
                // We don't have knowledge of this peer at all
                is_new_feed = true;
            }
            if is_new_feed {
                if self.replaced_public_key_exists(
                    &remote_active_feed.peer_id,
                    &remote_active_feed.replaced_public_key,
                    &remote_inactive_feeds,
                ) {
                    if let Some(new_remote_feeds) =
                        new_remote_feeds_map.get_mut(&remote_active_feed.peer_id)
                    {
                        if let Some(previous_index) = new_remote_feeds.iter().position(|feed| {
                            Some(feed.public_key) == remote_active_feed.replaced_public_key
                        }) {
                            new_remote_feeds.splice(
                                previous_index + 1..previous_index + 1,
                                [remote_active_feed],
                            );
                        } else {
                            new_remote_feeds.push(remote_active_feed);
                        }
                    } else {
                        new_remote_feeds_map
                            .insert(remote_active_feed.peer_id, vec![remote_active_feed]);
                    }
                } else {
                    wait_for_rebroadcast = true;
                }
            }
        }

        // Any remaining entries in stored_active_feeds are feeds remote new
        // nothing about. Send out in the re-broadcast all of the replaced
        // keys.
        for (_, stored_active_feed) in stored_active_peer_feeds {
            if stored_active_feed.replaced_public_key.is_some() {
                let stored_feeds = self.other_feeds.get(&stored_active_feed.peer_id).unwrap();
                let stored_active_feed_index = stored_feeds
                    .iter()
                    .position(|feed| &stored_active_feed == feed)
                    .unwrap();
                for i in 0..stored_active_feed_index {
                    let stored_inactive_feed = &stored_feeds[i];
                    if stored_inactive_feed.verified {
                        inactive_feeds_to_rebroadcast.push(stored_inactive_feed.clone());
                    } else {
                        panic!("Unverified feed before active feed");
                    }
                }
            }
        }

        // Make sure the map is in order of peer and also replace chain
        let mut new_feeds: Vec<DocumentFeedInfo> = vec![];
        for feeds in new_remote_feeds_map.into_values() {
            let mut previous_public_key: Option<FeedPublicKey> = None;
            for mut feed in feeds {
                if previous_public_key.is_some() && feed.replaced_public_key != previous_public_key
                {
                    // There is a gap, we need to abort
                    warn!("Remote broadcasted feed {feed:?} that didn't replace the previous public key: {previous_public_key:?}");
                    break;
                } else {
                    previous_public_key = Some(feed.public_key);
                    // Reset all feeds sent out
                    feed.verified = false;
                    feed.removed = false;
                    new_feeds.push(feed);
                }
            }
        }

        Ok(CompareBroadcastedFeedsResult {
            stored_active_feeds_found,
            new_feeds,
            inactive_feeds_to_rebroadcast,
            wait_for_rebroadcast,
        })
    }

    /// Merge incoming feeds into existing feeds
    pub(crate) fn merge_new_feeds(
        &mut self,
        new_feeds: &[DocumentFeedInfo],
    ) -> Result<ChangeDocumentFeedsStateResult, PeermergeError> {
        // First make sure that some other thread didn't already
        // merge some of these, in which case there are less changed
        // feeds than new feeds.
        let changed_feeds: Vec<DocumentFeedInfo> = new_feeds
            .iter()
            .filter(|incoming_feed| {
                if let Some(stored_feeds) = self.other_feeds.get(&incoming_feed.peer_id) {
                    !stored_feeds
                        .iter()
                        .any(|stored_feed| stored_feed == *incoming_feed)
                } else {
                    true
                }
            })
            .cloned()
            .collect();
        if changed_feeds.is_empty() {
            // Nothing changed, return empty result
            Ok(ChangeDocumentFeedsStateResult::unchanged())
        } else {
            // Recreate map created in compare_broadcasted_feeds
            let mut changed_feeds_map: HashMap<PeerId, Vec<DocumentFeedInfo>> = HashMap::new();
            for changed_feed in changed_feeds {
                if let Some(changed_feeds) = changed_feeds_map.get_mut(&changed_feed.peer_id) {
                    changed_feeds.push(changed_feed);
                } else {
                    changed_feeds_map.insert(changed_feed.peer_id, vec![changed_feed]);
                }
            }

            // Go throgh the values
            let mut replaced_feeds: Vec<DocumentFeedInfo> = vec![];
            let mut feeds_to_create: Vec<DocumentFeedInfo> = vec![];
            for (peer_id, feeds) in changed_feeds_map {
                let len = feeds.len();
                for (i, feed) in feeds.into_iter().enumerate() {
                    let mut feed_to_store = feed.clone();
                    feed_to_store.populate_discovery_key();
                    feed_to_store.verified = false;
                    feed_to_store.removed = false;
                    if let Some(stored_feeds) = self.other_feeds.get_mut(&feed.peer_id) {
                        let previous_feed_index = stored_feeds.len() - 1;
                        let previous_feed = stored_feeds.get_mut(previous_feed_index).unwrap();
                        let previous_public_key = previous_feed.public_key;
                        if feed.replaced_public_key != Some(previous_public_key) {
                            return Err(PeermergeError::InvalidOperation {
                                context: format!("Invalid new feed {feed:?}, does not replace {previous_public_key:?}")
                            });
                        }
                        if i == 0 {
                            replaced_feeds.push(previous_feed.clone());
                        }
                        stored_feeds.push(feed_to_store);
                    } else {
                        if feed.replaced_public_key.is_some() {
                            return Err(PeermergeError::InvalidOperation {
                                context: format!(
                                    "Invalid first feed {feed:?} for peer_id {peer_id:?}"
                                ),
                            });
                        }
                        self.other_feeds.insert(feed.peer_id, vec![feed_to_store]);
                    }
                    if i == len - 1 {
                        // The last one is the active one that should be created
                        feeds_to_create.push(feed);
                    } else {
                        // The remote gave us a more feeds for this peer, say they
                        // are replaced, even though we don't actually have them
                        // at all in the storage.
                        replaced_feeds.push(feed);
                    }
                }
            }
            Ok(ChangeDocumentFeedsStateResult {
                changed: true,
                replaced_feeds,
                feeds_to_create,
            })
        }
    }

    /// Set result received from merge_incoming_peers or replace_write_public_key into this feeds state
    pub(crate) fn set_replaced_feeds_and_feeds_to_create(
        &mut self,
        mut replaced_feeds: Vec<DocumentFeedInfo>,
        mut feeds_to_create: Vec<DocumentFeedInfo>,
    ) {
        if let Some(write_feed) = self.write_feed.as_mut() {
            if let Some(old_write_feed_index) = replaced_feeds
                .iter()
                .position(|replaced_feed| replaced_feed == write_feed)
            {
                let old_write_feed = replaced_feeds
                    .drain(old_write_feed_index..old_write_feed_index + 1)
                    .collect::<Vec<DocumentFeedInfo>>()
                    .into_iter()
                    .next()
                    .unwrap();

                // The write feed needs to be replaced, the new one needs to be in feeds_to_create
                let mut write_feed_index: Option<usize> = None;
                for (i, feed_to_create) in feeds_to_create.iter().enumerate() {
                    if feed_to_create.replaced_public_key == Some(old_write_feed.public_key) {
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
                    write_feed.verified = true;
                    write_feed.removed = false;
                } else {
                    panic!("Invalid write feed change parameters, new write feed missing");
                }

                // Put the old write feed to other_feeds
                if let Some(other_feeds) = self.other_feeds.get_mut(&old_write_feed.peer_id) {
                    other_feeds.push(old_write_feed);
                } else {
                    self.other_feeds
                        .insert(old_write_feed.peer_id, vec![old_write_feed]);
                }
            }
        }

        for mut replaced_feed in replaced_feeds {
            replaced_feed.populate_discovery_key();
            replaced_feed.verified = false;
            replaced_feed.removed = false;
            if let Some(other_feeds) = self.other_feeds.get_mut(&replaced_feed.peer_id) {
                // Only insert if the replaced feed doesn't already exist
                if !other_feeds.contains(&replaced_feed) {
                    other_feeds.push(replaced_feed);
                }
            } else {
                self.other_feeds
                    .insert(replaced_feed.peer_id, vec![replaced_feed]);
            }
        }

        for mut feed_to_create in feeds_to_create {
            feed_to_create.populate_discovery_key();
            if let Some(other_feeds) = self.other_feeds.get_mut(&feed_to_create.peer_id) {
                other_feeds.push(feed_to_create);
            } else {
                self.other_feeds
                    .insert(feed_to_create.peer_id, vec![feed_to_create]);
            }
        }
    }

    /// Replace the write feed's public key, returns the replaced_feeds and feeds_to_create.
    #[allow(dead_code)] // TODO: Remote when implemented
    pub(crate) fn replace_write_public_key(
        &mut self,
        new_write_public_key: FeedPublicKey,
        doc_signature_signing_key: &SigningKey,
    ) -> ChangeDocumentFeedsStateResult {
        let mut replaced_write_feed = self.write_feed.clone().unwrap();
        let mut new_write_feed = DocumentFeedInfo::new(
            replaced_write_feed.peer_id,
            new_write_public_key,
            Some(replaced_write_feed.public_key),
            doc_signature_signing_key,
        );
        new_write_feed.populate_discovery_key();
        new_write_feed.verified = true;
        self.write_feed = Some(new_write_feed.clone());

        if let Some(other_feeds) = self.other_feeds.get_mut(&replaced_write_feed.peer_id) {
            other_feeds.push(replaced_write_feed.clone());
        } else {
            self.other_feeds.insert(
                replaced_write_feed.peer_id,
                vec![replaced_write_feed.clone()],
            );
        }

        // Return values without discovery keys
        replaced_write_feed.discovery_key = None;
        new_write_feed.discovery_key = None;
        ChangeDocumentFeedsStateResult {
            changed: true,
            replaced_feeds: vec![replaced_write_feed],
            feeds_to_create: vec![new_write_feed],
        }
    }

    pub(crate) fn verify_feed(
        &mut self,
        discovery_key: &FeedDiscoveryKey,
        peer_id: &Option<PeerId>,
    ) -> bool {
        let mut changed = false;
        if let Some(peer_id) = peer_id {
            if let Some(stored_feeds) = self.other_feeds.get_mut(peer_id) {
                let stored_feed = stored_feeds
                    .iter_mut()
                    .find(|feed| feed.discovery_key.as_ref().unwrap() == discovery_key)
                    .expect("Could not find feed with discovery key from other feeds");
                if !stored_feed.verified {
                    stored_feed.verified = true;
                    changed = true;
                }
            }
        } else {
            if discovery_key != &self.doc_discovery_key {
                panic!("Got verify without peer but not to doc feed");
            }

            if !self.doc_feed_verified {
                self.doc_feed_verified = true;
                changed = true;
            }
        }
        changed
    }

    pub(crate) fn peer_id(&self, discovery_key: &FeedDiscoveryKey) -> PeerId {
        for (peer_id, feeds) in &self.other_feeds {
            for feed in feeds {
                if &feed.discovery_key.unwrap() == discovery_key {
                    return *peer_id;
                }
            }
        }
        if let Some(write_peer) = &self.write_feed {
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
    /// Key that the above public_key replaced for the same
    /// id. Needed when write feed is rotated for efficiency,
    /// but needs to be stored/sent to make sure stale peers will
    /// always find the latest feed.
    pub(crate) replaced_public_key: Option<FeedPublicKey>,
    /// Signature of peer_id, public_key and replaced_public_key signed
    /// with the doc signature key of the document. This is needed
    /// to prevent rogue proxies or read-only peers from announcing bogus
    /// feeds with the broadcast message.
    pub(crate) signature: Vec<u8>,
    /// If the signature in the first entry of the feed has been
    /// verified to contain valid information.
    pub(crate) verified: bool,
    /// If the feed has been removed from storage.
    pub(crate) removed: bool,
    /// Transient discovery key of the peer's write feed, can be saved
    /// to speed up searching based on it.
    pub(crate) discovery_key: Option<FeedDiscoveryKey>,
}

impl PartialEq for DocumentFeedInfo {
    #[inline]
    fn eq(&self, other: &DocumentFeedInfo) -> bool {
        self.peer_id == other.peer_id
            && self.public_key == other.public_key
            && self.replaced_public_key == other.replaced_public_key
            && self.signature == other.signature
    }
}

impl Hash for DocumentFeedInfo {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.peer_id.hash(state);
        self.public_key.hash(state);
        self.replaced_public_key.hash(state);
        self.signature.hash(state);
    }
}

impl DocumentFeedInfo {
    pub(crate) fn new(
        id: PeerId,
        public_key: [u8; 32],
        replaced_public_key: Option<FeedPublicKey>,
        doc_signature_signing_key: &SigningKey,
    ) -> Self {
        let mut buffer = id.to_vec();
        buffer.extend(&public_key);
        if let Some(key) = &replaced_public_key {
            buffer.extend(key);
        }
        let signature = sign(doc_signature_signing_key, &buffer).to_bytes().to_vec();
        Self {
            peer_id: id,
            public_key,
            replaced_public_key,
            signature,
            verified: false,
            removed: false,
            discovery_key: None,
        }
    }

    pub(crate) fn populate_discovery_key(&mut self) {
        if self.discovery_key.is_none() {
            self.discovery_key = Some(discovery_key_from_public_key(&self.public_key));
        }
    }

    pub(crate) fn verify(
        &self,
        doc_signature_verifying_key: &VerifyingKey,
    ) -> Result<(), PeermergeError> {
        let mut buffer = self.peer_id.to_vec();
        buffer.extend(&self.public_key);
        if let Some(key) = &self.replaced_public_key {
            buffer.extend(key);
        }
        buffer.extend(&self.signature);
        verify_data_signature(&buffer, doc_signature_verifying_key)
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

    /// Temporary storage of the document type that was stored to
    /// document URL. Stored only up until meta_automerge_doc has a
    /// value after which it is set to None.
    pub(crate) temporary_document_type: Option<String>,
    /// Temporary storage of the document header that was stored to
    /// document URL. Stored only up until meta_automerge_doc has a
    /// value after which it is set to None.
    pub(crate) temporary_document_header: Option<NameDescription>,
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
        temporary_document_type: Option<String>,
        temporary_document_header: Option<NameDescription>,
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
            temporary_document_type,
            temporary_document_header,
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

    use crate::common::keys::generate_keys;

    use super::*;

    fn eq_ignore_order<T>(a: &[T], b: &[T]) -> bool
    where
        T: Eq + Hash,
    {
        fn count<T>(items: &[T]) -> HashMap<&T, usize>
        where
            T: Eq + Hash,
        {
            let mut cnt = HashMap::new();
            for i in items {
                *cnt.entry(i).or_insert(0) += 1
            }
            cnt
        }

        count(a) == count(b)
    }

    #[test]
    fn feeds_state_write_all_new_remote() -> anyhow::Result<()> {
        let (signing_key, _) = generate_keys();
        let doc_public_key: [u8; 32] = [100; 32];
        let my_id: [u8; 16] = [0; 16];
        let my_write_feed: DocumentFeedInfo =
            DocumentFeedInfo::new(my_id, [0; 32], None, &signing_key);
        let peer_1_id: [u8; 16] = [1; 16];
        let peer_1_write: DocumentFeedInfo =
            DocumentFeedInfo::new(peer_1_id, [1; 32], None, &signing_key);
        let peer_2_id: [u8; 16] = [2; 16];
        let peer_2: DocumentFeedInfo =
            DocumentFeedInfo::new(peer_2_id, [2; 32], None, &signing_key);
        let peer_3_id: [u8; 16] = [3; 16];
        let peer_3: DocumentFeedInfo =
            DocumentFeedInfo::new(peer_3_id, [3; 32], None, &signing_key);

        let mut fs = DocumentFeedsState::new_from_data(
            my_id,
            doc_public_key,
            true,
            Some(my_write_feed.clone()),
            vec![],
        );
        let mut fs_copy = fs.clone();
        assert_eq!(fs, fs_copy);

        // Remote sends new peers and doesn't know about our write feed
        let compare_result = fs
            .compare_broadcasted_feeds(
                Some(peer_1_write.clone()),
                vec![peer_2.clone(), peer_3.clone()],
                None,
            )
            .unwrap();
        assert!(!compare_result.stored_active_feeds_found);
        assert!(eq_ignore_order(
            &compare_result.new_feeds,
            &vec![peer_1_write.clone(), peer_2.clone(), peer_3.clone()]
        ));
        assert!(!compare_result.wait_for_rebroadcast);
        assert!(compare_result.inactive_feeds_to_rebroadcast.is_empty());
        let merge_result = fs.merge_new_feeds(&compare_result.new_feeds).unwrap();
        assert!(merge_result.changed);
        assert!(merge_result.replaced_feeds.is_empty());
        assert!(eq_ignore_order(
            &merge_result.feeds_to_create,
            &vec![peer_1_write.clone(), peer_2.clone(), peer_3.clone()]
        ));
        fs_copy.set_replaced_feeds_and_feeds_to_create(
            merge_result.replaced_feeds,
            merge_result.feeds_to_create,
        );
        assert_eq!(fs, fs_copy);

        // Remote sends our feed as well
        let compare_result = fs
            .compare_broadcasted_feeds(
                Some(peer_1_write.clone()),
                vec![peer_3.clone(), peer_2.clone(), my_write_feed.clone()],
                None,
            )
            .unwrap();
        assert!(compare_result.stored_active_feeds_found);
        assert!(compare_result.new_feeds.is_empty());
        assert!(!compare_result.wait_for_rebroadcast);
        assert!(compare_result.inactive_feeds_to_rebroadcast.is_empty());

        // We replace our write feed
        let my_new_write_feed_pk: [u8; 32] = [4; 32];
        let my_new_write_feed = DocumentFeedInfo::new(
            my_write_feed.peer_id,
            my_new_write_feed_pk,
            Some(my_write_feed.public_key),
            &signing_key,
        );
        let replace_result = fs.replace_write_public_key(my_new_write_feed_pk, &signing_key);
        assert_eq!(fs.write_feed, Some(my_new_write_feed.clone()));
        assert_eq!(replace_result.replaced_feeds, vec![my_write_feed.clone()]);
        assert_eq!(
            replace_result.feeds_to_create,
            vec![my_new_write_feed.clone()]
        );
        fs_copy.set_replaced_feeds_and_feeds_to_create(
            replace_result.replaced_feeds,
            replace_result.feeds_to_create,
        );
        assert_eq!(fs, fs_copy);

        // The peer replaces their write key too
        let new_peer_1_write_pk: [u8; 32] = [5; 32];
        let new_peer_1_write: DocumentFeedInfo = DocumentFeedInfo::new(
            peer_1_id,
            new_peer_1_write_pk,
            Some(peer_1_write.public_key),
            &signing_key,
        );
        let compare_result = fs
            .compare_broadcasted_feeds(
                Some(new_peer_1_write.clone()),
                vec![
                    peer_3.clone(),
                    peer_2.clone(),
                    my_write_feed,
                    peer_1_write.clone(),
                ],
                None,
            )
            .unwrap();
        assert!(!compare_result.stored_active_feeds_found);
        assert_eq!(compare_result.new_feeds, vec![new_peer_1_write.clone()]);
        assert!(!compare_result.wait_for_rebroadcast);
        assert!(compare_result.inactive_feeds_to_rebroadcast.is_empty());

        let merge_result = fs.merge_new_feeds(&compare_result.new_feeds).unwrap();
        assert!(merge_result.changed);
        assert_eq!(merge_result.replaced_feeds, vec![peer_1_write.clone()]);
        assert_eq!(merge_result.feeds_to_create, vec![new_peer_1_write.clone()]);
        fs_copy.set_replaced_feeds_and_feeds_to_create(
            merge_result.replaced_feeds,
            merge_result.feeds_to_create,
        );
        assert_eq!(fs, fs_copy);

        // Remote sends our changed feed as well
        let compare_result = fs
            .compare_broadcasted_feeds(
                Some(new_peer_1_write),
                vec![peer_3, peer_2, my_new_write_feed, peer_1_write],
                None,
            )
            .unwrap();
        assert!(compare_result.stored_active_feeds_found);
        assert!(compare_result.new_feeds.is_empty());
        assert!(!compare_result.wait_for_rebroadcast);
        assert!(compare_result.inactive_feeds_to_rebroadcast.is_empty());

        Ok(())
    }
}
