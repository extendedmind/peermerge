use automerge::transaction::Transaction;
use automerge::{AutomergeError, ObjId, Patch};
use compact_encoding::EncodingError;
use dashmap::DashMap;
use futures::channel::mpsc::UnboundedSender;
use hypercore_protocol::hypercore::PartialKeypair;
#[cfg(not(target_arch = "wasm32"))]
use random_access_disk::RandomAccessDisk;
use random_access_memory::RandomAccessMemory;
use random_access_storage::RandomAccess;
use std::collections::HashMap;
use std::sync::Arc;
use std::{fmt::Debug, path::PathBuf};
use tracing::{debug, enabled, instrument, warn, Level};

use crate::automerge::{
    apply_entries_autocommit, apply_unapplied_entries_autocommit,
    bootstrap_automerge_user_doc_from_entries, init_automerge_doc_from_data, init_automerge_docs,
    init_first_peer, init_peer, transact_autocommit, transact_mut_autocommit,
    ApplyEntriesFeedChange, AutomergeDoc, DocsChangeResult, UnappliedEntries,
};
use crate::common::cipher::{decode_doc_url, encode_document_id, encode_proxy_doc_url};
use crate::common::encoding::serialize_entry;
use crate::common::entry::{EntryContent, ShrunkEntries};
use crate::common::keys::{
    discovery_key_from_public_key, document_id_from_discovery_key, generate_keys, Keypair,
};
use crate::common::state::{DocumentFeedInfo, DocumentFeedsState, DocumentState};
use crate::common::utils::{Mutex, YieldNow};
use crate::common::{DocumentInfo, StateEventContent::*};
use crate::feed::FeedDiscoveryKey;
#[cfg(not(target_arch = "wasm32"))]
use crate::feed::{create_new_read_disk_feed, create_new_write_disk_feed, open_disk_feed};
#[cfg(not(target_arch = "wasm32"))]
use crate::FeedDiskPersistence;
use crate::{
    common::{state::DocumentContent, storage::DocStateWrapper},
    feed::{
        create_new_read_memory_feed, create_new_write_memory_feed, get_feed,
        get_feed_discovery_keys, Feed,
    },
    FeedMemoryPersistence, FeedPersistence, StateEvent,
};
use crate::{
    DocumentId, DocumentSharingInfo, NameDescription, PeerId, PeermergeError, StateEventContent,
};

/// Document represents a single Automerge doc shared via feeds.
#[derive(derivative::Derivative)]
#[derivative(Clone(bound = ""))]
#[derive(Debug)]
pub(crate) struct Document<T, U>
where
    T: RandomAccess + Debug + Send,
    U: FeedPersistence,
{
    /// Map of the feeds of this document
    feeds: Arc<DashMap<FeedDiscoveryKey, Arc<Mutex<Feed<U>>>>>,
    /// The state of the document
    document_state: Arc<Mutex<DocStateWrapper<T>>>,
    /// Locally stored prefix path, empty for memory.
    prefix: PathBuf,
    /// This peer's id.
    peer_id: PeerId,
    /// If this document is a proxy
    proxy: bool,
    /// The doc discovery key
    doc_discovery_key: FeedDiscoveryKey,
    /// The document id, derived from doc_discovery_key.
    id: DocumentId,
    /// Settings for writing. None for proxy.
    write_settings: Option<DocumentWriteSettings>,
    /// The write discovery key, if any
    write_discovery_key: Option<FeedDiscoveryKey>,
    /// Whether or not this document is encrypted.
    encrypted: bool,
    /// If encrypted is true and not a proxy, the encryption key to use to decrypt feed entries.
    encryption_key: Option<Vec<u8>>,
    /// Log context for the entry points of Document. Will be read from meta data
    /// during creation or set to empty if not possible.
    log_context: String,
}

#[derive(Debug, Clone)]
pub(crate) struct DocumentWriteSettings {
    /// Maximum entry data field size in bytes. None for proxy.
    pub(crate) max_entry_data_size_bytes: usize,
    /// Maximum write feed length. None for proxy.
    pub(crate) max_write_feed_length: u64,
}

impl<T, U> Document<T, U>
where
    T: RandomAccess + Debug + Send + 'static,
    U: FeedPersistence,
{
    pub(crate) fn id(&self) -> DocumentId {
        self.id
    }

    pub(crate) fn doc_discovery_key(&self) -> FeedDiscoveryKey {
        self.doc_discovery_key
    }

    pub(crate) async fn info(&self) -> DocumentInfo {
        let document_state_wrapper = self.document_state.lock().await;
        document_state_wrapper.state().info()
    }

    pub(crate) async fn peer_header(&self, peer_id: &PeerId) -> Option<NameDescription> {
        let document_state_wrapper = self.document_state.lock().await;
        document_state_wrapper.state().peer_header(peer_id)
    }

    pub(crate) async fn doc_feed(&self) -> Arc<Mutex<Feed<U>>> {
        get_feed(&self.feeds, &self.doc_discovery_key)
            .await
            .unwrap()
    }

    pub(crate) async fn peer_feeds(&self) -> Vec<Arc<Mutex<Feed<U>>>> {
        let mut leaf_feeds = vec![];
        for feed_discovery_key in get_feed_discovery_keys(&self.feeds).await {
            if feed_discovery_key != self.doc_discovery_key {
                leaf_feeds.push(get_feed(&self.feeds, &feed_discovery_key).await.unwrap());
            }
        }
        leaf_feeds
    }

    pub(crate) async fn peer_feed(
        &self,
        discovery_key: &FeedDiscoveryKey,
    ) -> Option<Arc<Mutex<Feed<U>>>> {
        if discovery_key == &self.doc_discovery_key {
            return None;
        }
        get_feed(&self.feeds, discovery_key).await
    }

    pub(crate) async fn peer_ids(&self) -> Vec<PeerId> {
        let document_state_wrapper = self.document_state.lock().await;
        document_state_wrapper.state().peer_ids()
    }

    pub(crate) async fn feeds_state(&self) -> DocumentFeedsState {
        let state = self.document_state.lock().await;
        let state = state.state();
        state.feeds_state.clone()
    }

    pub(crate) async fn peer_id_from_discovery_key(
        &self,
        discovery_key: &FeedDiscoveryKey,
    ) -> PeerId {
        let state = self.document_state.lock().await;
        let state = state.state();
        state.feeds_state.peer_id(discovery_key)
    }

    #[instrument(skip_all, fields(ctx = self.log_context))]
    pub(crate) async fn watch(&mut self, ids: Option<Vec<ObjId>>) {
        if self.proxy {
            panic!("Can not watch on a proxy");
        }
        let mut document_state = self.document_state.lock().await;
        document_state.watch(ids);
    }

    #[instrument(skip_all, fields(ctx = self.log_context))]
    pub(crate) async fn transact<F, O>(&self, cb: F) -> Result<O, PeermergeError>
    where
        F: FnOnce(&AutomergeDoc) -> Result<O, AutomergeError>,
    {
        if self.proxy {
            panic!("Can not read on a proxy");
        }
        let result = {
            let document_state = self.document_state.lock().await;
            let result = if let Some(doc) = document_state.user_automerge_doc() {
                transact_autocommit(doc, cb).unwrap()
            } else {
                unimplemented!(
                    "TODO: No proper error code for trying to read before a document is synced"
                );
            };
            result
        };
        Ok(result)
    }

    #[instrument(skip_all, fields(ctx = self.log_context))]
    pub(crate) async fn transact_mut<F, O>(
        &mut self,
        cb: F,
        change_id: Option<Vec<u8>>,
        state_event_sender: &mut Arc<Mutex<Option<UnboundedSender<StateEvent>>>>,
    ) -> Result<O, PeermergeError>
    where
        F: FnOnce(&mut AutomergeDoc) -> Result<O, AutomergeError>,
    {
        if self.proxy {
            panic!("Can not transact on a proxy");
        }
        let (result, patches) = {
            let mut document_state = self.document_state.lock().await;
            let (entries, result, patches) =
                if let Some(doc) = document_state.user_automerge_doc_mut() {
                    let (entries, result) = transact_mut_autocommit(false, doc, cb).unwrap();
                    let patches = if !entries.is_empty() {
                        doc.diff_incremental()
                    } else {
                        vec![]
                    };
                    (entries, result, patches)
                } else {
                    unimplemented!(
                    "TODO: No proper error code for trying to change before a document is synced"
                );
                };
            if !entries.is_empty() {
                let write_discovery_key = document_state.write_discovery_key();
                let length = {
                    let write_feed = get_feed(&self.feeds, &write_discovery_key).await.unwrap();
                    let mut write_feed = write_feed.lock().await;

                    let entry_data_batch: Vec<Vec<u8>> = entries
                        .into_iter()
                        .map(|entry| serialize_entry(&entry))
                        .collect::<Result<Vec<Vec<u8>>, EncodingError>>()?;
                    write_feed.append_batch(entry_data_batch).await?
                };
                document_state
                    .set_cursor_and_save_data(
                        &write_discovery_key,
                        length,
                        DocsChangeResult {
                            meta_changed: false,
                            user_changed: true,
                        },
                    )
                    .await;
            }
            (result, patches)
        };
        if !patches.is_empty() {
            let mut state_event_sender = state_event_sender.lock().await;
            if let Some(sender) = state_event_sender.as_mut() {
                sender
                    .unbounded_send(StateEvent::new(
                        self.doc_discovery_key,
                        DocumentChanged { change_id, patches },
                    ))
                    .unwrap();
            }
        }
        Ok(result)
    }

    #[instrument(skip_all, fields(ctx = self.log_context))]
    pub(crate) async fn reserve_object<O: AsRef<ObjId>>(
        &mut self,
        obj: O,
    ) -> Result<(), PeermergeError> {
        if self.proxy {
            panic!("Can not reserve object on a proxy");
        }
        let mut document_state = self.document_state.lock().await;
        document_state.reserve_object(obj);
        Ok(())
    }

    #[instrument(skip_all, fields(ctx = self.log_context))]
    pub(crate) async fn unreserve_object<O: AsRef<ObjId>>(
        &mut self,
        obj: O,
    ) -> Result<Vec<StateEvent>, PeermergeError> {
        if self.proxy {
            panic!("Can not unreserve object on a proxy");
        }
        let mut document_state = self.document_state.lock().await;
        document_state.unreserve_object(obj);

        // Now we need to re-consolidate
        if let Some((content, feeds_state, unapplied_entries)) =
            document_state.content_feeds_state_and_unapplied_entries_mut()
        {
            if let Some(meta_automerge_doc) = content.meta_automerge_doc.as_mut() {
                if let Some(user_automerge_doc) = content.user_automerge_doc.as_mut() {
                    let result = apply_unapplied_entries_autocommit(
                        meta_automerge_doc,
                        user_automerge_doc,
                        unapplied_entries,
                    )?;
                    let (patches, peer_syncs) = update_content_from_edit_result(
                        result,
                        &self.doc_discovery_key,
                        content,
                        feeds_state,
                    )
                    .await?;
                    document_state.persist_content().await;
                    let state_events = self.state_events_from_update_content_result(
                        &document_state,
                        None,
                        patches,
                        peer_syncs,
                    );
                    return Ok(state_events);
                }
            }
        }

        Ok(vec![])
    }

    #[instrument(skip_all, fields(ctx = self.log_context))]
    pub(crate) async fn close(&mut self) -> Result<(), PeermergeError> {
        let root_feed = get_feed(&self.feeds, &self.doc_discovery_key)
            .await
            .unwrap();
        let mut root_feed = root_feed.lock().await;
        root_feed.notify_closed().await?;
        Ok(())
    }

    #[instrument(skip_all, fields(ctx = self.log_context))]
    pub(crate) async fn sharing_info(&self) -> Result<DocumentSharingInfo, PeermergeError> {
        if !self.proxy {
            let doc_feed = get_feed(&self.feeds, &self.doc_discovery_key)
                .await
                .unwrap();
            let mut doc_feed = doc_feed.lock().await;
            let init_doc_entry = &doc_feed.entries(0, 1).await?.entries[0];
            let meta_doc_data = match &init_doc_entry.content {
                EntryContent::InitDoc { meta_doc_data, .. } => meta_doc_data,
                _ => panic!("Invalid doc feed"),
            };

            let document_state = self.document_state.lock().await;
            let doc_url = document_state
                .state()
                .doc_url(meta_doc_data.to_vec(), &self.encryption_key);

            Ok(DocumentSharingInfo {
                proxy: false,
                proxy_doc_url: encode_proxy_doc_url(&doc_url),
                doc_url: Some(doc_url),
            })
        } else {
            let document_state = self.document_state.lock().await;
            let proxy_doc_url = document_state.state().proxy_doc_url();
            Ok(DocumentSharingInfo {
                proxy: true,
                proxy_doc_url,
                doc_url: None,
            })
        }
    }

    #[instrument(skip_all, fields(ctx = self.log_context))]
    pub(crate) fn encryption_key(&self) -> Option<Vec<u8>> {
        if self.proxy {
            panic!("A proxy does not store the encryption key");
        }
        self.encryption_key.clone()
    }

    #[instrument(skip_all, fields(ctx = self.log_context))]
    pub(crate) async fn write_key_pair(&self) -> PartialKeypair {
        if self.proxy {
            panic!("A proxy does not have a write key pair");
        }
        let document_state = self.document_state.lock().await;
        let write_discovery_key = document_state.write_discovery_key();
        let write_feed_wrapper = get_feed(&self.feeds, &write_discovery_key).await.unwrap();
        let write_feed = write_feed_wrapper.lock().await;
        write_feed.key_pair().await
    }

    #[instrument(level = "debug", skip_all, fields(ctx = self.log_context))]
    pub(crate) async fn take_patches(&mut self) -> Vec<Patch> {
        let mut document_state = self.document_state.lock().await;
        if let Some(doc) = document_state.user_automerge_doc_mut() {
            doc.diff_incremental()
        } else {
            vec![]
        }
    }

    #[instrument(level = "debug", skip_all, fields(ctx = self.log_context))]
    pub(crate) async fn process_remote_feed_synced(
        &self,
        peer_id: Option<PeerId>,
        discovery_key: FeedDiscoveryKey,
        synced_contiguous_length: u64,
    ) -> Vec<StateEvent> {
        if let Some(peer_id) = peer_id {
            vec![StateEvent::new(
                self.id(),
                RemotePeerSynced {
                    peer_id,
                    discovery_key,
                    contiguous_length: synced_contiguous_length,
                },
            )]
        } else {
            // Peer id is not available for the doc feed only, don't send that
            // forward
            vec![]
        }
    }

    #[instrument(level = "debug", skip_all, fields(ctx = self.log_context))]
    pub(crate) async fn process_feed_synced(
        &mut self,
        peer_id: Option<PeerId>,
        discovery_key: FeedDiscoveryKey,
        synced_contiguous_length: u64,
    ) -> Vec<StateEvent> {
        debug!(
            "Processing peer synced, is_root={}",
            discovery_key == self.doc_discovery_key
        );
        if self.proxy {
            if let Some(peer_id) = peer_id {
                // Just notify a peer sync forward
                return vec![StateEvent::new(
                    self.id(),
                    PeerSynced {
                        peer_id,
                        discovery_key,
                        contiguous_length: synced_contiguous_length,
                    },
                )];
            } else {
                // Peer id is not available for the doc feed only, don't send that
                // forward
                return vec![];
            }
        }
        let state_events: Vec<StateEvent> = {
            // Sync doc state exclusively...
            let mut document_state = self.document_state.lock().await;
            let (document_initialized, patches, peer_syncs) =
                if let Some((content, feeds_state, unapplied_entries)) =
                    document_state.content_feeds_state_and_unapplied_entries_mut()
                {
                    if content.is_bootsrapped() {
                        debug!("Document has bootstrapped content, updating it");
                        let (patches, peer_syncs) = self
                            .update_synced_content(
                                &discovery_key,
                                synced_contiguous_length,
                                content,
                                feeds_state,
                                unapplied_entries,
                            )
                            .await
                            .unwrap();
                        document_state.persist_content().await;
                        (None, patches, peer_syncs)
                    } else {
                        debug!("Bootstrapping document content from entries");
                        if let Some(peer_syncs) = self
                            .bootstrap_content(
                                &discovery_key,
                                synced_contiguous_length,
                                content,
                                feeds_state,
                                unapplied_entries,
                            )
                            .await
                            .unwrap()
                        {
                            debug!("Document created, saving and returning DocumentInitialized");
                            document_state.persist_content().await;
                            (
                                Some(DocumentInitialized {
                                    new_document: true,
                                    parent_document_id: None, // TODO: Parent document id
                                }),
                                vec![],
                                peer_syncs,
                            )
                        } else {
                            debug!("Document could not be created, need more peers");
                            // Could not create content from this peer's data, needs more peers
                            (None, vec![], vec![])
                        }
                    }
                } else {
                    panic!("Content needs to exist for non-proxy documents");
                };

            self.state_events_from_update_content_result(
                &document_state,
                document_initialized,
                patches,
                peer_syncs,
            )
            // ..doc state sync ready, release lock
        };

        // Finally, notify about the new sync so that other protocols can get synced as
        // well. The message reaches the same feed that sent this, but is disregarded.
        {
            let feed = get_feed(&self.feeds, &discovery_key).await.unwrap();
            let mut feed = feed.lock().await;
            feed.notify_feed_synced(synced_contiguous_length)
                .await
                .unwrap();
        }
        state_events
    }

    #[instrument(level = "debug", skip_all, fields(ctx = self.log_context))]
    async fn notify_feeds_changed(
        &mut self,
        replaced_feeds: Vec<DocumentFeedInfo>,
        feeds_to_create: Vec<DocumentFeedInfo>,
    ) {
        // Send message to doc feed that new feeds have been created to get all open protocols to
        // open channels to it.
        let doc_feed = get_feed(&self.feeds, &self.doc_discovery_key)
            .await
            .unwrap();
        let mut doc_feed = doc_feed.lock().await;
        doc_feed
            .notify_feeds_changed(self.doc_discovery_key, replaced_feeds, feeds_to_create)
            .await
            .unwrap();
    }

    async fn bootstrap_content(
        &self,
        synced_discovery_key: &[u8; 32],
        synced_contiguous_length: u64,
        content: &mut DocumentContent,
        feeds_state: &mut DocumentFeedsState,
        unapplied_entries: &mut UnappliedEntries,
    ) -> Result<Option<Vec<(PeerId, FeedDiscoveryKey, u64)>>, PeermergeError> {
        // The document starts from the doc feed, so get that first
        if synced_discovery_key == &self.doc_discovery_key {
            let entries = {
                let doc_feed = get_feed(&self.feeds, &self.doc_discovery_key)
                    .await
                    .unwrap();
                let mut doc_feed = doc_feed.lock().await;
                doc_feed.entries(0, synced_contiguous_length).await?
            };

            // Bootstrap user document from the feed
            let (mut bootstrap_result, feed_change_result) = {
                let meta_automerge_doc = content.meta_automerge_doc_mut().unwrap();
                let result = bootstrap_automerge_user_doc_from_entries(
                    meta_automerge_doc,
                    &self.peer_id,
                    synced_discovery_key,
                    synced_contiguous_length,
                    entries,
                    unapplied_entries,
                )?;
                // Empty meta patches
                meta_automerge_doc.update_diff_cursor();
                result
            };

            let cursor_changes: Vec<(FeedDiscoveryKey, u64)> = feed_change_result
                .iter()
                .map(|(discovery_key, feed_change)| (*discovery_key, feed_change.length))
                .collect();
            content.set_cursors_and_save_data(
                cursor_changes,
                DocsChangeResult {
                    meta_changed: false,
                    user_changed: false,
                },
            );

            // Empty patches queue, documents were just initialized, so they can be safely ignored.
            bootstrap_result.user_automerge_doc.update_diff_cursor();

            // Set values to content
            content.meta_doc_data = bootstrap_result.meta_doc_data;
            content.user_doc_data = Some(bootstrap_result.user_doc_data);
            content.user_automerge_doc = Some(bootstrap_result.user_automerge_doc);

            let peer_syncs: Vec<(PeerId, FeedDiscoveryKey, u64)> = feed_change_result
                .iter()
                // The root feed is not a peer.
                .filter(|(discovery_key, _)| *discovery_key != &self.doc_discovery_key)
                .map(|(discovery_key, feed_change)| {
                    let peer_id = feeds_state.peer_id(discovery_key);
                    (peer_id, *discovery_key, feed_change.length)
                })
                .collect();
            Ok(Some(peer_syncs))
        } else {
            // Got first some other peer's data, need to store it to unapplied changes
            let feed = get_feed(&self.feeds, synced_discovery_key).await.unwrap();
            let mut feed = feed.lock().await;
            let current_length = unapplied_entries.current_length(synced_discovery_key);
            let shrunk_entries = feed
                .entries(current_length, synced_contiguous_length - current_length)
                .await?;
            let mut new_length = current_length + shrunk_entries.shrunk_count + 1;
            for entry in shrunk_entries.entries.into_iter() {
                unapplied_entries.add(synced_discovery_key, new_length, entry, vec![]);
                new_length += 1;
            }
            Ok(None)
        }
    }

    async fn update_synced_content(
        &self,
        synced_discovery_key: &[u8; 32],
        synced_contiguous_length: u64,
        content: &mut DocumentContent,
        feeds_state: &mut DocumentFeedsState,
        unapplied_entries: &mut UnappliedEntries,
    ) -> Result<(Vec<Patch>, Vec<(PeerId, FeedDiscoveryKey, u64)>), PeermergeError> {
        let (_, entries) = get_new_entries(
            synced_discovery_key,
            Some(synced_contiguous_length),
            content,
            &self.feeds,
        )
        .await?;

        update_content_with_entries(
            entries,
            synced_discovery_key,
            synced_contiguous_length,
            &self.doc_discovery_key,
            content,
            feeds_state,
            unapplied_entries,
        )
        .await
    }

    fn state_events_from_update_content_result(
        &self,
        document_state: &DocStateWrapper<T>,
        document_initialized: Option<StateEventContent>,
        mut patches: Vec<Patch>,
        peer_syncs: Vec<(PeerId, FeedDiscoveryKey, u64)>,
    ) -> Vec<StateEvent> {
        // Filter out unwatched patches
        document_state.filter_watched_patches(&mut patches);

        let mut state_events: Vec<StateEvent> = vec![];
        let peer_synced_state_events: Vec<StateEvent> = peer_syncs
            .iter()
            .map(|sync| {
                StateEvent::new(
                    self.id(),
                    PeerSynced {
                        peer_id: sync.0,
                        discovery_key: sync.1,
                        contiguous_length: sync.2,
                    },
                )
            })
            .collect();
        state_events.extend(peer_synced_state_events);
        if let Some(event) = document_initialized {
            state_events.push(StateEvent::new(self.id(), event));
        }
        if !patches.is_empty() {
            state_events.push(StateEvent::new(
                self.id(),
                DocumentChanged {
                    change_id: None,
                    patches,
                },
            ));
        }
        state_events
    }
}

//////////////////////////////////////////////////////
//
// Memory

impl Document<RandomAccessMemory, FeedMemoryPersistence> {
    pub(crate) async fn create_new_memory<F, O>(
        peer_id: PeerId,
        peer_header: &NameDescription,
        document_type: &str,
        document_header: Option<NameDescription>,
        encrypted: bool,
        write_settings: DocumentWriteSettings,
        init_cb: F,
    ) -> Result<(Self, O), PeermergeError>
    where
        F: FnOnce(&mut Transaction) -> Result<O, AutomergeError>,
    {
        let (prepare_result, init_result) = prepare_create(
            &peer_id,
            peer_header,
            document_type,
            &document_header,
            encrypted,
            init_cb,
        )
        .await?;

        // Create the doc memory feed
        let (_doc_feed_length, doc_feed, doc_encryption_key) = create_new_write_memory_feed(
            prepare_result.doc_key_pair,
            Some(prepare_result.doc_feed_init_data),
            encrypted,
            &None,
        )
        .await;

        // Create a write memory feed
        let (_write_feed_length, write_feed, _) = create_new_write_memory_feed(
            prepare_result.write_key_pair,
            Some(prepare_result.write_feed_init_data),
            encrypted,
            &doc_encryption_key,
        )
        .await;

        Ok((
            Self::new_memory(
                peer_id,
                (prepare_result.doc_discovery_key, doc_feed),
                Some((prepare_result.write_discovery_key, write_feed)),
                prepare_result.state,
                encrypted,
                doc_encryption_key,
                Some(write_settings),
            )
            .await,
            init_result,
        ))
    }

    pub(crate) async fn attach_writer_memory(
        peer_id: PeerId,
        peer_header: &NameDescription,
        doc_url: &str,
        encryption_key: &Option<Vec<u8>>,
        write_settings: DocumentWriteSettings,
    ) -> Result<Self, PeermergeError> {
        Self::do_attach_writer_memory(
            peer_id,
            peer_header,
            doc_url,
            encryption_key,
            write_settings,
            None,
        )
        .await
    }

    pub(crate) async fn reattach_writer_memory(
        peer_id: PeerId,
        write_key_pair: Keypair,
        peer_name: &str,
        doc_url: &str,
        encryption_key: &Option<Vec<u8>>,
        write_settings: DocumentWriteSettings,
    ) -> Result<Self, PeermergeError> {
        Self::do_attach_writer_memory(
            peer_id,
            &NameDescription::new(peer_name),
            doc_url,
            encryption_key,
            write_settings,
            Some(write_key_pair),
        )
        .await
    }

    pub(crate) async fn attach_proxy_memory(peer_id: PeerId, doc_url: &str) -> Self {
        let proxy = true;
        let doc_url = encode_proxy_doc_url(doc_url);
        let encrypted = false;

        // Process keys from doc URL
        let decoded_doc_url = decode_doc_url(&doc_url, &None);
        let doc_discovery_key = decoded_doc_url.doc_url_info.doc_discovery_key;

        // Create the doc feed
        let (_, doc_feed) = create_new_read_memory_feed(
            &decoded_doc_url.doc_url_info.doc_public_key,
            proxy,
            encrypted,
            &None,
        )
        .await;

        // Initialize document state
        let state = DocumentState::new(
            proxy,
            None,
            DocumentFeedsState::new(decoded_doc_url.doc_url_info.doc_public_key),
            None,
        );

        Self::new_memory(
            peer_id,
            (doc_discovery_key, doc_feed),
            None,
            state,
            encrypted,
            None,
            None,
        )
        .await
    }

    #[instrument(level = "debug", skip_all, fields(ctx = self.log_context))]
    pub(crate) async fn process_new_feeds_broadcasted_memory(
        &mut self,
        new_remote_feeds: Vec<DocumentFeedInfo>,
    ) -> bool {
        let (changed, replaced_feeds, feeds_to_create) = {
            let mut document_state = self.document_state.lock().await;
            document_state
                .merge_new_remote_feeds(&new_remote_feeds)
                .await
        };
        if changed {
            {
                // Create and insert all new feeds
                self.create_and_insert_read_memory_feeds(feeds_to_create.clone())
                    .await;
            }
            {
                self.notify_feeds_changed(replaced_feeds, feeds_to_create)
                    .await;
            }
        }
        changed
    }

    async fn do_attach_writer_memory(
        peer_id: PeerId,
        peer_header: &NameDescription,
        doc_url: &str,
        encryption_key: &Option<Vec<u8>>,
        write_settings: DocumentWriteSettings,
        mut write_key_pair: Option<Keypair>,
    ) -> Result<Self, PeermergeError> {
        let proxy = false;

        // Process keys from doc URL
        let decoded_doc_url = decode_doc_url(doc_url, encryption_key);
        let doc_discovery_key = decoded_doc_url.doc_url_info.doc_discovery_key;
        let encrypted = if let Some(encrypted) = decoded_doc_url.doc_url_info.encrypted {
            if encrypted && encryption_key.is_none() {
                panic!("Can not attach a peer to an encrypted document without an encryption key");
            }
            encrypted
        } else {
            panic!("Given doc url can only be used for proxying");
        };
        let meta_doc_data = decoded_doc_url
            .doc_url_appendix
            .expect("Writer needs to have an appendix")
            .meta_doc_data;

        // Create the root feed
        let (_, root_feed) = create_new_read_memory_feed(
            &decoded_doc_url.doc_url_info.doc_public_key,
            proxy,
            encrypted,
            encryption_key,
        )
        .await;

        // (Re)create the write feed
        let (
            write_public_key,
            write_discovery_key,
            write_feed,
            write_feed_init_data_len,
            meta_automerge_doc,
        ) = if let Some(write_key_pair) = write_key_pair.take() {
            let write_public_key = *write_key_pair.public.as_bytes();
            let write_discovery_key = discovery_key_from_public_key(&write_public_key);
            let meta_automerge_doc = init_automerge_doc_from_data(&peer_id, &meta_doc_data);
            let (_, write_feed, _) =
                create_new_write_memory_feed(write_key_pair, None, encrypted, encryption_key).await;
            (
                write_public_key,
                write_discovery_key,
                write_feed,
                0,
                meta_automerge_doc,
            )
        } else {
            let (write_key_pair, write_discovery_key) = generate_keys();
            let write_public_key = *write_key_pair.public.as_bytes();

            // Init the meta document from the URL
            let mut meta_automerge_doc = init_automerge_doc_from_data(&peer_id, &meta_doc_data);
            let init_peer_entries = init_peer(
                &mut meta_automerge_doc,
                None,
                &peer_id,
                &Some(peer_header.clone()),
            )?;
            let write_feed_init_data: Vec<Vec<u8>> = init_peer_entries
                .into_iter()
                .map(|entry| serialize_entry(&entry).unwrap())
                .collect();
            let write_feed_init_data_len = write_feed_init_data.len();
            let (_, write_feed, _) = create_new_write_memory_feed(
                write_key_pair,
                Some(write_feed_init_data),
                encrypted,
                encryption_key,
            )
            .await;
            (
                write_public_key,
                write_discovery_key,
                write_feed,
                write_feed_init_data_len,
                meta_automerge_doc,
            )
        };

        // Initialize document state
        let content = DocumentContent::new(
            peer_id,
            &decoded_doc_url.doc_url_info.doc_discovery_key,
            0,
            &write_discovery_key,
            write_feed_init_data_len,
            meta_doc_data,
            None,
            meta_automerge_doc,
            None,
        );
        let state = DocumentState::new(
            false,
            Some(encrypted),
            DocumentFeedsState::new_writer(
                decoded_doc_url.doc_url_info.doc_public_key,
                &peer_id,
                &write_public_key,
            ),
            Some(content),
        );

        Ok(Self::new_memory(
            peer_id,
            (doc_discovery_key, root_feed),
            Some((write_discovery_key, write_feed)),
            state,
            encrypted,
            encryption_key.clone(),
            Some(write_settings),
        )
        .await)
    }

    async fn new_memory(
        peer_id: PeerId,
        doc_feed: ([u8; 32], Feed<FeedMemoryPersistence>),
        write_feed: Option<([u8; 32], Feed<FeedMemoryPersistence>)>,
        state: DocumentState,
        encrypted: bool,
        encryption_key: Option<Vec<u8>>,
        write_settings: Option<DocumentWriteSettings>,
    ) -> Self {
        let id = state.document_id;
        let feeds: DashMap<[u8; 32], Arc<Mutex<Feed<FeedMemoryPersistence>>>> = DashMap::new();
        let (doc_discovery_key, doc_feed) = doc_feed;
        feeds.insert(doc_discovery_key, Arc::new(Mutex::new(doc_feed)));

        let write_discovery_key = if let Some((write_discovery_key, write_feed)) = write_feed {
            feeds.insert(write_discovery_key, Arc::new(Mutex::new(write_feed)));
            Some(write_discovery_key)
        } else {
            None
        };
        let proxy = state.proxy;
        let log_context = log_context(&state);
        let document_state = DocStateWrapper::new_memory(state).await;
        Self {
            feeds: Arc::new(feeds),
            document_state: Arc::new(Mutex::new(document_state)),
            peer_id,
            prefix: PathBuf::new(),
            proxy,
            doc_discovery_key,
            id,
            write_discovery_key,
            encrypted,
            encryption_key,
            log_context,
            write_settings,
        }
    }

    async fn create_and_insert_read_memory_feeds(&mut self, feed_infos: Vec<DocumentFeedInfo>) {
        for feed_info in feed_infos {
            let discovery_key = discovery_key_from_public_key(&feed_info.public_key);
            // Make sure to insert only once even if two protocols notice the same new
            // feed at the same time using the entry API.

            // There is a deadlock possibility with entry(), so we need to loop and yield
            let mut entry_found = false;
            while !entry_found {
                if let Some(entry) = self.feeds.try_entry(discovery_key) {
                    match entry {
                        dashmap::mapref::entry::Entry::Occupied(_) => {
                            debug!("Concurrent creating of feeds noticed, continuing.");
                        }
                        dashmap::mapref::entry::Entry::Vacant(vacant) => {
                            let (_, feed) = create_new_read_memory_feed(
                                &feed_info.public_key,
                                self.proxy,
                                self.encryption_key.is_some(),
                                &self.encryption_key,
                            )
                            .await;
                            vacant.insert(Arc::new(Mutex::new(feed)));
                        }
                    }
                    entry_found = true;
                } else {
                    debug!("Concurrent access to feeds noticed, yielding and retrying.");
                    YieldNow(false).await;
                }
            }
        }
    }
}

//////////////////////////////////////////////////////
//
// Disk

#[cfg(not(target_arch = "wasm32"))]
impl Document<RandomAccessDisk, FeedDiskPersistence> {
    pub(crate) async fn create_new_disk<F, O>(
        peer_id: PeerId,
        default_peer_header: &NameDescription,
        document_type: &str,
        document_header: Option<NameDescription>,
        encrypted: bool,
        write_settings: DocumentWriteSettings,
        init_cb: F,
        data_root_dir: &PathBuf,
    ) -> Result<(Self, O), PeermergeError>
    where
        F: FnOnce(&mut Transaction) -> Result<O, AutomergeError>,
    {
        let (prepare_result, init_result) = prepare_create(
            &peer_id,
            default_peer_header,
            document_type,
            &document_header,
            encrypted,
            init_cb,
        )
        .await?;
        let postfix = encode_document_id(&prepare_result.document_id);
        let data_root_dir = data_root_dir.join(postfix);

        // Create the root disk feed
        let (_root_feed_length, root_feed, root_encryption_key) = create_new_write_disk_feed(
            &data_root_dir,
            prepare_result.doc_key_pair,
            &prepare_result.doc_discovery_key,
            prepare_result.doc_feed_init_data,
            encrypted,
            &None,
        )
        .await;

        // Create a write disk feed
        let (_write_feed_length, write_feed, _) = create_new_write_disk_feed(
            &data_root_dir,
            prepare_result.write_key_pair,
            &prepare_result.write_discovery_key,
            prepare_result.write_feed_init_data,
            encrypted,
            &root_encryption_key,
        )
        .await;

        Ok((
            Self::new_disk(
                peer_id,
                (prepare_result.doc_discovery_key, root_feed),
                Some((prepare_result.write_discovery_key, write_feed)),
                prepare_result.state,
                encrypted,
                root_encryption_key,
                &data_root_dir,
                Some(write_settings),
            )
            .await,
            init_result,
        ))
    }

    pub(crate) async fn attach_writer_disk(
        peer_id: PeerId,
        peer_header: &NameDescription,
        doc_url: &str,
        encryption_key: &Option<Vec<u8>>,
        data_root_dir: &PathBuf,
        write_settings: DocumentWriteSettings,
    ) -> Result<Self, PeermergeError> {
        let proxy = false;

        // Process keys from doc URL
        let decoded_doc_url = decode_doc_url(doc_url, encryption_key);
        let doc_discovery_key = decoded_doc_url.doc_url_info.doc_discovery_key;
        let encrypted = if let Some(encrypted) = decoded_doc_url.doc_url_info.encrypted {
            if encrypted && encryption_key.is_none() {
                panic!("Can not attach a peer to an encrypted document without an encryption key");
            }
            encrypted
        } else {
            panic!("Given doc url can only be used for proxying");
        };
        let meta_doc_data = decoded_doc_url
            .doc_url_appendix
            .expect("Writer needs to have an appendix")
            .meta_doc_data;

        // Create the root feed
        let postfix = encode_document_id(&decoded_doc_url.doc_url_info.document_id);
        let data_root_dir = data_root_dir.join(postfix);
        let (_, root_feed) = create_new_read_disk_feed(
            &data_root_dir,
            &decoded_doc_url.doc_url_info.doc_public_key,
            &decoded_doc_url.doc_url_info.doc_discovery_key,
            proxy,
            encrypted,
            encryption_key,
        )
        .await;

        // Create the write feed keys
        let (write_key_pair, write_discovery_key) = generate_keys();
        let write_public_key = *write_key_pair.public.as_bytes();

        // Init the meta document from the URL
        let mut meta_automerge_doc = init_automerge_doc_from_data(&peer_id, &meta_doc_data);
        let init_peer_entries = init_peer(
            &mut meta_automerge_doc,
            None,
            &peer_id,
            &Some(peer_header.clone()),
        )?;
        let write_feed_init_data: Vec<Vec<u8>> = init_peer_entries
            .into_iter()
            .map(|entry| serialize_entry(&entry).unwrap())
            .collect();
        let write_feed_init_data_len = write_feed_init_data.len();

        // Create the write feed
        let (_, write_feed, _) = create_new_write_disk_feed(
            &data_root_dir,
            write_key_pair,
            &write_discovery_key,
            write_feed_init_data,
            encrypted,
            encryption_key,
        )
        .await;

        // Initialize document state
        let content = DocumentContent::new(
            peer_id,
            &decoded_doc_url.doc_url_info.doc_discovery_key,
            0,
            &write_discovery_key,
            write_feed_init_data_len,
            meta_doc_data,
            None,
            meta_automerge_doc,
            None,
        );
        let state = DocumentState::new(
            proxy,
            Some(encrypted),
            DocumentFeedsState::new_writer(
                decoded_doc_url.doc_url_info.doc_public_key,
                &peer_id,
                &write_public_key,
            ),
            Some(content),
        );

        Ok(Self::new_disk(
            peer_id,
            (doc_discovery_key, root_feed),
            Some((write_discovery_key, write_feed)),
            state,
            encrypted,
            encryption_key.clone(),
            &data_root_dir,
            Some(write_settings),
        )
        .await)
    }

    pub(crate) async fn attach_proxy_disk(
        peer_id: PeerId,
        doc_url: &str,
        data_root_dir: &PathBuf,
    ) -> Self {
        let proxy = true;
        let doc_url = encode_proxy_doc_url(doc_url);
        let encrypted = false;

        // Process keys from doc URL
        let decoded_doc_url = decode_doc_url(&doc_url, &None);
        let document_id = decoded_doc_url.doc_url_info.document_id;
        let doc_discovery_key = decoded_doc_url.doc_url_info.doc_discovery_key;

        // Create the doc feed
        let postfix = encode_document_id(&document_id);
        let data_root_dir = data_root_dir.join(postfix);
        let (_, doc_feed) = create_new_read_disk_feed(
            &data_root_dir,
            &decoded_doc_url.doc_url_info.doc_public_key,
            &document_id,
            proxy,
            encrypted,
            &None,
        )
        .await;

        // Initialize document state
        let state = DocumentState::new(
            proxy,
            None,
            DocumentFeedsState::new(decoded_doc_url.doc_url_info.doc_public_key),
            None,
        );

        Self::new_disk(
            peer_id,
            (doc_discovery_key, doc_feed),
            None,
            state,
            encrypted,
            None,
            &data_root_dir,
            None,
        )
        .await
    }

    pub(crate) async fn info_disk(data_root_dir: &PathBuf) -> Result<DocumentInfo, PeermergeError> {
        let mut document_state_wrapper = DocStateWrapper::open_disk(data_root_dir).await?;
        if let Some((content, _, _)) =
            document_state_wrapper.content_feeds_state_and_unapplied_entries_mut()
        {
            let meta_automerge_doc =
                init_automerge_doc_from_data(&content.peer_id, &content.meta_doc_data);
            content.meta_automerge_doc = Some(meta_automerge_doc);
        }
        Ok(document_state_wrapper.state().info())
    }

    pub(crate) async fn open_disk(
        peer_id: PeerId,
        encryption_key: &Option<Vec<u8>>,
        data_root_dir: &PathBuf,
        write_settings: DocumentWriteSettings,
    ) -> Result<Self, PeermergeError> {
        let mut document_state_wrapper = DocStateWrapper::open_disk(data_root_dir).await?;
        let state = document_state_wrapper.state();
        let id = state.document_id;
        let proxy = state.proxy;
        let encrypted = if let Some(encrypted) = state.encrypted {
            if encrypted && encryption_key.is_none() {
                panic!("Can not open and encrypted document without an encryption key");
            }
            encrypted
        } else {
            if !proxy {
                panic!("Stored document is not a proxy but encryption status is not known");
            }
            false
        };
        let doc_discovery_key = state.feeds_state.doc_discovery_key;
        let log_context = log_context(state);

        // Open root feed
        let feeds: DashMap<[u8; 32], Arc<Mutex<Feed<FeedDiskPersistence>>>> = DashMap::new();
        let (_, root_feed) = open_disk_feed(
            data_root_dir,
            &doc_discovery_key,
            proxy,
            encrypted,
            encryption_key,
        )
        .await;
        feeds.insert(doc_discovery_key, Arc::new(Mutex::new(root_feed)));

        // Open all peer feeds
        for peer in &state.feeds_state.other_feeds {
            if peer.replaced_by_public_key.is_none() {
                let discovery_key = discovery_key_from_public_key(&peer.public_key);
                let (_, peer_feed) = open_disk_feed(
                    data_root_dir,
                    &discovery_key,
                    proxy,
                    encrypted,
                    encryption_key,
                )
                .await;
                feeds.insert(discovery_key, Arc::new(Mutex::new(peer_feed)));
            }
        }

        // Open write feed, if any
        let (feeds, write_discovery_key) =
            if let Some(write_peer) = state.feeds_state.write_feed.clone() {
                let write_discovery_key = discovery_key_from_public_key(&write_peer.public_key);
                debug!(
                    "open_disk: peers={}, writable, proxy={proxy}, encrypted={encrypted}",
                    feeds.len() - 1
                );
                if write_peer.public_key != state.feeds_state.doc_public_key {
                    let (_, write_feed) = open_disk_feed(
                        data_root_dir,
                        &write_discovery_key,
                        proxy,
                        encrypted,
                        encryption_key,
                    )
                    .await;
                    feeds.insert(write_discovery_key, Arc::new(Mutex::new(write_feed)));
                }

                let feeds = Arc::new(feeds);

                // Initialize doc, fill unapplied changes and possibly save state if it had been left
                // unsaved
                if let Some((content, feeds_state, unapplied_entries)) =
                    document_state_wrapper.content_feeds_state_and_unapplied_entries_mut()
                {
                    let meta_automerge_doc =
                        init_automerge_doc_from_data(&write_peer.peer_id, &content.meta_doc_data);
                    content.meta_automerge_doc = Some(meta_automerge_doc);
                    if let Some(user_doc_data) = &content.user_doc_data {
                        let user_automerge_doc =
                            init_automerge_doc_from_data(&write_peer.peer_id, user_doc_data);
                        content.user_automerge_doc = Some(user_automerge_doc);
                    }
                    let changed = update_content(
                        content,
                        feeds_state,
                        &doc_discovery_key,
                        &feeds,
                        unapplied_entries,
                    )
                    .await
                    .unwrap();
                    debug!("open_disk: initialized document from data, changed={changed}");
                    if changed {
                        document_state_wrapper.persist_content().await;
                    }
                } else {
                    debug!("open_disk: document not created yet",);
                }
                (feeds, Some(write_discovery_key))
            } else {
                debug!(
                    "open_disk: peers={}, not writable, proxy={proxy}, encrypted={encrypted}",
                    feeds.len() - 1
                );
                (Arc::new(feeds), None)
            };

        // Create Document
        Ok(Self {
            feeds,
            document_state: Arc::new(Mutex::new(document_state_wrapper)),
            peer_id,
            prefix: data_root_dir.clone(),
            doc_discovery_key,
            id,
            write_discovery_key,
            proxy,
            encrypted,
            encryption_key: encryption_key.clone(),
            log_context,
            write_settings: Some(write_settings),
        })
    }

    #[instrument(level = "debug", skip_all, fields(ctx = self.log_context))]
    pub(crate) async fn process_new_feeds_broadcasted_disk(
        &mut self,
        new_remote_feeds: Vec<DocumentFeedInfo>,
    ) -> bool {
        let (changed, replaced_feeds, feeds_to_create) = {
            let mut document_state = self.document_state.lock().await;
            document_state
                .merge_new_remote_feeds(&new_remote_feeds)
                .await
        };
        if changed {
            {
                // Create and insert all new feeds
                self.create_and_insert_read_disk_feeds(feeds_to_create.clone())
                    .await;
            }
            {
                self.notify_feeds_changed(replaced_feeds, feeds_to_create)
                    .await;
            }
        }
        changed
    }

    async fn new_disk(
        peer_id: PeerId,
        root_feed: ([u8; 32], Feed<FeedDiskPersistence>),
        write_feed: Option<([u8; 32], Feed<FeedDiskPersistence>)>,
        state: DocumentState,
        encrypted: bool,
        encryption_key: Option<Vec<u8>>,
        data_root_dir: &PathBuf,
        write_settings: Option<DocumentWriteSettings>,
    ) -> Self {
        let id = state.document_id;
        let feeds: DashMap<[u8; 32], Arc<Mutex<Feed<FeedDiskPersistence>>>> = DashMap::new();
        let (doc_discovery_key, doc_feed) = root_feed;
        feeds.insert(doc_discovery_key, Arc::new(Mutex::new(doc_feed)));
        let write_discovery_key = if let Some((write_discovery_key, write_feed)) = write_feed {
            feeds.insert(write_discovery_key, Arc::new(Mutex::new(write_feed)));
            Some(write_discovery_key)
        } else {
            None
        };
        let proxy = state.proxy;
        let log_context = log_context(&state);
        let document_state = DocStateWrapper::new_disk(state, data_root_dir).await;

        Self {
            feeds: Arc::new(feeds),
            document_state: Arc::new(Mutex::new(document_state)),
            peer_id,
            prefix: data_root_dir.clone(),
            proxy,
            doc_discovery_key,
            id,
            write_settings,
            write_discovery_key,
            encrypted,
            encryption_key,
            log_context,
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn create_and_insert_read_disk_feeds(&mut self, feed_infos: Vec<DocumentFeedInfo>) {
        for feed_info in feed_infos {
            let discovery_key = discovery_key_from_public_key(&feed_info.public_key);
            // Make sure to insert only once even if two protocols notice the same new
            // feed at the same time using the entry API.

            // There is a deadlock possibility with entry(), so we need to loop and yield
            let mut entry_found = false;
            while !entry_found {
                if let Some(entry) = self.feeds.try_entry(discovery_key) {
                    match entry {
                        dashmap::mapref::entry::Entry::Occupied(_) => {
                            debug!("Concurrent creating of feeds noticed, continuing.");
                        }
                        dashmap::mapref::entry::Entry::Vacant(vacant) => {
                            let (_, feed) = create_new_read_disk_feed(
                                &self.prefix,
                                &feed_info.public_key,
                                &discovery_key,
                                self.proxy,
                                self.encryption_key.is_some(),
                                &self.encryption_key,
                            )
                            .await;
                            vacant.insert(Arc::new(Mutex::new(feed)));
                        }
                    }
                    entry_found = true;
                } else {
                    debug!("Concurrent access to feeds noticed, yielding and retrying.");
                    YieldNow(false).await;
                }
            }
        }
    }
}

//////////////////////////////////////////////////////
//
// Utilities

pub(crate) async fn get_document_by_discovery_key<T, U>(
    documents: &Arc<DashMap<DocumentId, Document<T, U>>>,
    discovery_key: &FeedDiscoveryKey,
) -> Option<Document<T, U>>
where
    T: RandomAccess + Debug + Send + 'static,
    U: FeedPersistence,
{
    get_document(documents, &document_id_from_discovery_key(discovery_key)).await
}

pub(crate) async fn get_document<T, U>(
    documents: &Arc<DashMap<DocumentId, Document<T, U>>>,
    document_id: &DocumentId,
) -> Option<Document<T, U>>
where
    T: RandomAccess + Debug + Send + 'static,
    U: FeedPersistence,
{
    loop {
        match documents.try_get(document_id) {
            dashmap::try_result::TryResult::Absent => {
                return None;
            }
            dashmap::try_result::TryResult::Locked => {
                YieldNow(false).await;
            }
            dashmap::try_result::TryResult::Present(value) => {
                // TODO: Cloning the Document is not terribly expensive, but it's not free either.
                // This can probably use a lot of optimization, especially with non-mutable
                // documents.
                return Some(value.clone());
            }
        }
    }
}

pub(crate) async fn get_document_ids<T, U>(
    documents: &Arc<DashMap<DocumentId, Document<T, U>>>,
) -> Vec<DocumentId>
where
    T: RandomAccess + Debug + Send + 'static,
    U: FeedPersistence,
{
    // I believe xacrimon/dashmap/issues/151 needs to be resolved for this to guarantee
    // to not deadlock.
    documents.iter().map(|multi| *multi.key()).collect()
}

struct PrepareCreateResult {
    document_id: DocumentId,
    doc_key_pair: Keypair,
    doc_discovery_key: FeedDiscoveryKey,
    write_key_pair: Keypair,
    write_discovery_key: [u8; 32],
    doc_feed_init_data: Vec<Vec<u8>>,
    write_feed_init_data: Vec<Vec<u8>>,
    state: DocumentState,
}

async fn prepare_create<F, O>(
    peer_id: &PeerId,
    peer_header: &NameDescription,
    document_type: &str,
    document_header: &Option<NameDescription>,
    encrypted: bool,
    init_cb: F,
) -> Result<(PrepareCreateResult, O), PeermergeError>
where
    F: FnOnce(&mut Transaction) -> Result<O, AutomergeError>,
{
    // Generate a doc feed key pair, its discovery key and the public key string
    let (doc_key_pair, doc_discovery_key) = generate_keys();
    let doc_public_key = *doc_key_pair.public.as_bytes();
    let document_id = document_id_from_discovery_key(&doc_discovery_key);

    // Generate a writeable feed key pair, its discovery key and the public key string
    let (write_key_pair, write_discovery_key) = generate_keys();
    let write_public_key = *write_key_pair.public.as_bytes();

    // Initialize the documents
    let (mut create_result, init_result, doc_feed_init_entries) =
        init_automerge_docs(document_id, peer_id, false, init_cb).unwrap();
    let doc_feed_init_data: Vec<Vec<u8>> = doc_feed_init_entries
        .into_iter()
        .map(|entry| serialize_entry(&entry).unwrap())
        .collect();

    // Initialize the first peer
    let write_feed_init_entries = init_first_peer(
        &mut create_result.meta_automerge_doc,
        peer_id,
        peer_header,
        document_type,
        document_header,
    )?;
    let write_feed_init_data: Vec<Vec<u8>> = write_feed_init_entries
        .into_iter()
        .map(|entry| serialize_entry(&entry).unwrap())
        .collect();

    // Initialize document state
    let content = DocumentContent::new(
        *peer_id,
        &doc_discovery_key,
        doc_feed_init_data.len(),
        &write_discovery_key,
        write_feed_init_data.len(),
        create_result.meta_doc_data,
        Some(create_result.user_doc_data),
        create_result.meta_automerge_doc,
        Some(create_result.user_automerge_doc),
    );
    let state = DocumentState::new(
        false,
        Some(encrypted),
        DocumentFeedsState::new_writer(doc_public_key, peer_id, &write_public_key),
        Some(content),
    );

    Ok((
        PrepareCreateResult {
            document_id,
            doc_key_pair,
            doc_discovery_key,
            write_key_pair,
            write_discovery_key,
            doc_feed_init_data,
            write_feed_init_data,
            state,
        },
        init_result,
    ))
}

async fn update_content<T>(
    content: &mut DocumentContent,
    feeds_state: &mut DocumentFeedsState,
    doc_discovery_key: &[u8; 32],
    feeds: &Arc<DashMap<[u8; 32], Arc<Mutex<Feed<T>>>>>,
    unapplied_entries: &mut UnappliedEntries,
) -> Result<bool, PeermergeError>
where
    T: RandomAccess + Debug + Send + 'static,
{
    let mut changed = false;
    for discovery_key in get_feed_discovery_keys(feeds).await {
        let (contiguous_length, shrunk_entries) =
            get_new_entries(&discovery_key, None, content, &feeds).await?;

        let result = update_content_with_entries(
            shrunk_entries,
            &discovery_key,
            contiguous_length,
            doc_discovery_key,
            content,
            feeds_state,
            unapplied_entries,
        )
        .await?;
        if !result.0.is_empty() {
            changed = true;
        }
    }

    Ok(changed)
}

async fn get_new_entries<T>(
    discovery_key: &[u8; 32],
    known_contiguous_length: Option<u64>,
    content: &DocumentContent,
    feeds: &Arc<DashMap<[u8; 32], Arc<Mutex<Feed<T>>>>>,
) -> Result<(u64, ShrunkEntries), PeermergeError>
where
    T: RandomAccess + Debug + Send + 'static,
{
    let feed = get_feed(feeds, discovery_key).await.unwrap();
    let mut feed = feed.lock().await;
    let contiguous_length = if let Some(known_contiguous_length) = known_contiguous_length {
        known_contiguous_length
    } else {
        feed.contiguous_length().await
    };
    let shrunk_entries = feed
        .entries(content.cursor_length(discovery_key), contiguous_length)
        .await?;
    Ok((contiguous_length, shrunk_entries))
}

async fn update_content_with_entries(
    shrunk_entries: ShrunkEntries,
    synced_discovery_key: &[u8; 32],
    synced_contiguous_length: u64,
    doc_discovery_key: &[u8; 32],
    content: &mut DocumentContent,
    feeds_state: &mut DocumentFeedsState,
    unapplied_entries: &mut UnappliedEntries,
) -> Result<(Vec<Patch>, Vec<(PeerId, FeedDiscoveryKey, u64)>), PeermergeError> {
    let (meta_automerge_doc, user_automerge_doc) = content.docs_mut().unwrap();
    let result = apply_entries_autocommit(
        meta_automerge_doc,
        user_automerge_doc,
        synced_discovery_key,
        synced_contiguous_length,
        shrunk_entries,
        unapplied_entries,
    )?;
    update_content_from_edit_result(result, doc_discovery_key, content, feeds_state).await
}

async fn update_content_from_edit_result(
    result: HashMap<[u8; 32], ApplyEntriesFeedChange>,
    doc_discovery_key: &[u8; 32],
    content: &mut DocumentContent,
    feeds_state: &mut DocumentFeedsState,
) -> Result<(Vec<Patch>, Vec<(PeerId, FeedDiscoveryKey, u64)>), PeermergeError> {
    let (user_patches, cursor_changes, peer_syncs, change_result) = {
        let (meta_automerge_doc, user_automerge_doc) = content.docs_mut().unwrap();
        let cursor_changes: Vec<([u8; 32], u64)> = result
            .iter()
            .map(|(discovery_key, feed_change)| (*discovery_key, feed_change.length))
            .collect();
        let peer_syncs: Vec<(PeerId, FeedDiscoveryKey, u64)> = result
            .iter()
            .filter(|(discovery_key, _)| *discovery_key != doc_discovery_key)
            .map(|(discovery_key, feed_change)| {
                let peer_id = feeds_state.peer_id(discovery_key);
                (peer_id, *discovery_key, feed_change.length)
            })
            .collect();

        let (user_patches, change_result) = if !peer_syncs.is_empty() {
            let user_patches = user_automerge_doc.diff_incremental();
            let meta_patches = meta_automerge_doc.diff_incremental();
            let change_result = DocsChangeResult {
                meta_changed: !meta_patches.is_empty(),
                user_changed: !user_patches.is_empty(),
            };
            (user_patches, change_result)
        } else {
            (
                vec![],
                DocsChangeResult {
                    meta_changed: false,
                    user_changed: false,
                },
            )
        };
        (user_patches, cursor_changes, peer_syncs, change_result)
    };
    content.set_cursors_and_save_data(cursor_changes, change_result);
    Ok((user_patches, peer_syncs))
}

fn log_context(state: &DocumentState) -> String {
    if enabled!(Level::DEBUG) {
        if let Some((document_type, document_header)) = state.document_type_and_header() {
            let postfix: String = if let Some(document_header) = document_header {
                format!("|{}", document_header.name)
            } else {
                "".to_string()
            };
            format!("{document_type}{postfix}")
        } else {
            "".to_string()
        }
    } else {
        "".to_string()
    }
}
