use automerge::transaction::Transaction;
use automerge::{AutomergeError, ObjId, Patch};
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
    apply_entries_autocommit, apply_unapplied_entries_autocommit, init_automerge_doc_from_data,
    init_automerge_docs, init_automerge_docs_from_entries, init_first_peer, init_peer,
    transact_autocommit, transact_mut_autocommit, ApplyEntriesFeedChange, AutomergeDoc,
    DocsChangeResult, UnappliedEntries,
};
use crate::common::cipher::{decode_doc_url, encode_document_id, encode_proxy_doc_url};
use crate::common::encoding::serialize_entry;
use crate::common::entry::EntryContent;
use crate::common::keys::{
    discovery_key_from_public_key, document_id_from_discovery_key, generate_keys, Keypair,
};
use crate::common::state::{DocumentPeer, DocumentPeersState, DocumentState};
use crate::common::utils::{Mutex, YieldNow};
use crate::common::{DocumentInfo, StateEventContent::*};
use crate::feed::FeedDiscoveryKey;
#[cfg(not(target_arch = "wasm32"))]
use crate::feed::{create_new_read_disk_feed, create_new_write_disk_feed, open_disk_feed};
#[cfg(not(target_arch = "wasm32"))]
use crate::FeedDiskPersistence;
use crate::{
    common::{
        entry::Entry,
        state::{DocumentContent, DocumentCursor},
        storage::DocStateWrapper,
    },
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

impl<T, U> Document<T, U>
where
    T: RandomAccess + Debug + Send + 'static,
    U: FeedPersistence,
{
    pub(crate) fn id(&self) -> DocumentId {
        self.doc_discovery_key
    }

    pub(crate) async fn info(&self) -> DocumentInfo {
        let document_state_wrapper = self.document_state.lock().await;
        document_state_wrapper.state().info()
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

    pub(crate) async fn feed_discovery_keys(&self) -> Vec<FeedDiscoveryKey> {
        get_feed_discovery_keys(&self.feeds).await
    }

    pub(crate) async fn peers_state(&self) -> DocumentPeersState {
        let state = self.document_state.lock().await;
        let state = state.state();
        state.peers_state.clone()
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
            let (entry, result, patches) =
                if let Some(doc) = document_state.user_automerge_doc_mut() {
                    let (entry, result) = transact_mut_autocommit(false, doc, cb).unwrap();
                    let patches = if entry.is_some() {
                        doc.diff_incremental()
                    } else {
                        vec![]
                    };
                    (entry, result, patches)
                } else {
                    unimplemented!(
                    "TODO: No proper error code for trying to change before a document is synced"
                );
                };
            if let Some(entry) = entry {
                let write_discovery_key = document_state.write_discovery_key();
                let length = {
                    let write_feed = get_feed(&self.feeds, &write_discovery_key).await.unwrap();
                    let mut write_feed = write_feed.lock().await;
                    write_feed.append(&serialize_entry(&entry)?).await?
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
                        DocumentChanged((change_id, patches)),
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
        if let Some((content, unapplied_entries)) =
            document_state.content_and_unapplied_entries_mut()
        {
            if let Some(meta_automerge_doc) = content.meta_automerge_doc.as_mut() {
                if let Some(user_automerge_doc) = content.user_automerge_doc.as_mut() {
                    let result = apply_unapplied_entries_autocommit(
                        meta_automerge_doc,
                        user_automerge_doc,
                        unapplied_entries,
                    )?;
                    let (patches, peer_syncs) =
                        update_content_from_edit_result(result, &self.doc_discovery_key, content)
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
            let init_doc_entry = &doc_feed.entries(0, 1).await?.0[0];
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
    pub(crate) async fn process_peer_synced(
        &mut self,
        discovery_key: [u8; 32],
        synced_contiguous_length: u64,
    ) -> Vec<StateEvent> {
        debug!(
            "Processing peer synced, is_root={}",
            discovery_key == self.doc_discovery_key
        );
        if self.proxy {
            if discovery_key != self.doc_discovery_key {
                // Just notify a peer sync forward
                return vec![StateEvent::new(
                    self.id(),
                    PeerSynced((None, discovery_key, synced_contiguous_length)),
                )];
            } else {
                return vec![];
            }
        }
        let state_events: Vec<StateEvent> = {
            // Sync doc state exclusively...
            let mut document_state = self.document_state.lock().await;
            let (document_initialized, patches, peer_syncs) =
                if let Some((content, unapplied_entries)) =
                    document_state.content_and_unapplied_entries_mut()
                {
                    debug!("Document has content, updating it");
                    let (patches, peer_syncs) = self
                        .update_synced_content(
                            &discovery_key,
                            synced_contiguous_length,
                            content,
                            unapplied_entries,
                        )
                        .await
                        .unwrap();
                    document_state.persist_content().await;
                    (None, patches, peer_syncs)
                } else {
                    let write_discovery_key = document_state.write_discovery_key();
                    let unapplied_entries = document_state.unappliend_entries_mut();
                    if let Some((content, peer_syncs)) = self
                        .create_content(
                            &discovery_key,
                            synced_contiguous_length,
                            &write_discovery_key,
                            unapplied_entries,
                        )
                        .await
                        .unwrap()
                    {
                        debug!("Document created, saving and returning DocumentInitialized");
                        document_state.set_content(content).await;
                        (
                            Some(DocumentInitialized(true, None)), // TODO: Parent document id
                            vec![],
                            peer_syncs,
                        )
                    } else {
                        debug!("Document could not be created, need more peers");
                        // Could not create content from this peer's data, needs more peers
                        (None, vec![], vec![])
                    }
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
            feed.notify_peer_synced(synced_contiguous_length)
                .await
                .unwrap();
        }
        state_events
    }

    #[instrument(level = "debug", skip_all, fields(ctx = self.log_context))]
    async fn notify_peers_changed(
        &mut self,
        incoming_peers: Vec<DocumentPeer>,
        replaced_peers: Vec<DocumentPeer>,
        new_peers: Vec<DocumentPeer>,
    ) {
        // Send message to doc feed that new peers have been created to get all open protocols to
        // open channels to it.
        let doc_feed = get_feed(&self.feeds, &self.doc_discovery_key)
            .await
            .unwrap();
        let mut doc_feed = doc_feed.lock().await;
        doc_feed
            .notify_peers_changed(
                self.doc_discovery_key,
                incoming_peers,
                replaced_peers,
                new_peers,
            )
            .await
            .unwrap();
    }

    async fn create_content(
        &self,
        synced_discovery_key: &[u8; 32],
        synced_contiguous_length: u64,
        write_discovery_key: &[u8; 32],
        unapplied_entries: &mut UnappliedEntries,
    ) -> Result<Option<(DocumentContent, Vec<([u8; 32], u64)>)>, PeermergeError> {
        // The document starts from the doc feed, so get that first
        if synced_discovery_key == &self.doc_discovery_key {
            let (init_entries, init_entries_original_offset) = {
                let doc_feed = get_feed(&self.feeds, &self.doc_discovery_key)
                    .await
                    .unwrap();
                let mut doc_feed = doc_feed.lock().await;
                doc_feed.entries(0, synced_contiguous_length).await?
            };

            // Create DocContent from the feed
            let (mut init_docs_result, feed_change_result) = init_automerge_docs_from_entries(
                &self.peer_id,
                write_discovery_key,
                synced_discovery_key,
                synced_contiguous_length,
                init_entries,
                init_entries_original_offset,
                unapplied_entries,
            )?;
            let cursors: Vec<DocumentCursor> = feed_change_result
                .iter()
                .map(|(discovery_key, feed_change)| {
                    DocumentCursor::new(*discovery_key, feed_change.length)
                })
                .collect();

            // Empty patches queue, documents were just initialized, so they can be safely ignored.
            init_docs_result.user_automerge_doc.update_diff_cursor();
            init_docs_result.meta_automerge_doc.update_diff_cursor();

            let peer_syncs: Vec<([u8; 32], u64)> = feed_change_result
                .iter()
                // The root feed is not a peer.
                .filter(|(discovery_key, _)| *discovery_key != &self.doc_discovery_key)
                .map(|(discovery_key, feed_change)| (*discovery_key, feed_change.length))
                .collect();
            Ok(Some((
                DocumentContent::new(
                    cursors,
                    init_docs_result.meta_doc_data,
                    init_docs_result.user_doc_data,
                    init_docs_result.meta_automerge_doc,
                    init_docs_result.user_automerge_doc,
                ),
                peer_syncs,
            )))
        } else {
            // Got first some other peer's data, need to store it to unapplied changes
            let feed = get_feed(&self.feeds, synced_discovery_key).await.unwrap();
            let mut feed = feed.lock().await;
            let current_length = unapplied_entries.current_length(synced_discovery_key);
            let (entries, original_offset) = feed
                .entries(current_length, synced_contiguous_length - current_length)
                .await?;
            let mut new_length = current_length + original_offset + 1;
            for entry in entries {
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
        unapplied_entries: &mut UnappliedEntries,
    ) -> Result<(Vec<Patch>, Vec<([u8; 32], u64)>), PeermergeError> {
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
            unapplied_entries,
        )
        .await
    }

    fn state_events_from_update_content_result(
        &self,
        document_state: &DocStateWrapper<T>,
        document_initialized: Option<StateEventContent>,
        mut patches: Vec<Patch>,
        peer_syncs: Vec<([u8; 32], u64)>,
    ) -> Vec<StateEvent> {
        // Filter out unwatched patches
        document_state.filter_watched_patches(&mut patches);

        let mut state_events: Vec<StateEvent> = vec![];
        let peer_synced_state_events: Vec<StateEvent> = peer_syncs
            .iter()
            .map(|sync| {
                // FIXME: PeerSynced should have PeerId
                let name = None;
                StateEvent::new(self.id(), PeerSynced((name, sync.0, sync.1)))
            })
            .collect();
        state_events.extend(peer_synced_state_events);
        if let Some(event) = document_initialized {
            state_events.push(StateEvent::new(self.id(), event));
        }
        if !patches.is_empty() {
            state_events.push(StateEvent::new(self.id(), DocumentChanged((None, patches))));
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
    ) -> Result<Self, PeermergeError> {
        Self::do_attach_writer_memory(peer_id, peer_header, doc_url, encryption_key, None).await
    }

    pub(crate) async fn reattach_writer_memory(
        peer_id: PeerId,
        write_key_pair: Keypair,
        peer_name: &str,
        doc_url: &str,
        encryption_key: &Option<Vec<u8>>,
    ) -> Result<Self, PeermergeError> {
        Self::do_attach_writer_memory(
            peer_id,
            &NameDescription::new(peer_name),
            doc_url,
            encryption_key,
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
        let document_id = decoded_doc_url.doc_url_info.document_id;

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
            decoded_doc_url.doc_url_info.doc_public_key,
            None,
            DocumentPeersState::new(),
            Some(DocumentContent::new_proxy(
                &decoded_doc_url.doc_url_info.doc_discovery_key,
            )),
        );

        Self::new_memory(
            peer_id,
            (document_id, doc_feed),
            None,
            state,
            encrypted,
            None,
        )
        .await
    }

    #[instrument(level = "debug", skip_all, fields(ctx = self.log_context))]
    pub(crate) async fn process_new_peers_broadcasted_memory(
        &mut self,
        incoming_peers: Vec<DocumentPeer>,
    ) -> bool {
        let (changed, replaced_peers, peers_to_create) = {
            let mut document_state = self.document_state.lock().await;
            document_state.process_incoming_peers(&incoming_peers).await
        };
        if changed {
            {
                // Create and insert all new feeds
                self.create_and_insert_read_memory_feeds(peers_to_create.clone())
                    .await;
            }
            {
                self.notify_peers_changed(incoming_peers, replaced_peers, peers_to_create)
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
        mut write_key_pair: Option<Keypair>,
    ) -> Result<Self, PeermergeError> {
        let proxy = false;

        // Process keys from doc URL
        let decoded_doc_url = decode_doc_url(doc_url, encryption_key);
        let document_id = decoded_doc_url.doc_url_info.document_id;
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
            let meta_automerge_doc =
                init_automerge_doc_from_data(&peer_id, &write_discovery_key, &meta_doc_data);
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
            let mut meta_automerge_doc =
                init_automerge_doc_from_data(&peer_id, &write_discovery_key, &meta_doc_data);
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
        let content = DocumentContent::new_writer(
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
            decoded_doc_url.doc_url_info.doc_public_key,
            Some(encrypted),
            DocumentPeersState::new_writer(&peer_id, &write_public_key),
            Some(content),
        );

        Ok(Self::new_memory(
            peer_id,
            (document_id, root_feed),
            Some((write_discovery_key, write_feed)),
            state,
            encrypted,
            encryption_key.clone(),
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
    ) -> Self {
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
            write_discovery_key,
            encrypted,
            encryption_key,
            log_context,
        }
    }

    async fn create_and_insert_read_memory_feeds(&mut self, peers: Vec<DocumentPeer>) {
        for peer in peers {
            let discovery_key = discovery_key_from_public_key(&peer.public_key);
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
                                &peer.public_key,
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
    ) -> Result<Self, PeermergeError> {
        let proxy = false;

        // Process keys from doc URL
        let decoded_doc_url = decode_doc_url(doc_url, encryption_key);
        let document_id = decoded_doc_url.doc_url_info.document_id;
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
        let postfix = encode_document_id(&document_id);
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
        let mut meta_automerge_doc =
            init_automerge_doc_from_data(&peer_id, &write_discovery_key, &meta_doc_data);
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
        let content = DocumentContent::new_writer(
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
            decoded_doc_url.doc_url_info.doc_public_key,
            Some(encrypted),
            DocumentPeersState::new_writer(&peer_id, &write_public_key),
            Some(content),
        );

        Ok(Self::new_disk(
            peer_id,
            (document_id, root_feed),
            Some((write_discovery_key, write_feed)),
            state,
            encrypted,
            encryption_key.clone(),
            &data_root_dir,
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
            decoded_doc_url.doc_url_info.doc_public_key,
            None,
            DocumentPeersState::new(),
            Some(DocumentContent::new_proxy(
                &decoded_doc_url.doc_url_info.doc_discovery_key,
            )),
        );

        Self::new_disk(
            peer_id,
            (document_id, doc_feed),
            None,
            state,
            encrypted,
            None,
            &data_root_dir,
        )
        .await
    }

    pub(crate) async fn info_disk(data_root_dir: &PathBuf) -> Result<DocumentInfo, PeermergeError> {
        let document_state_wrapper = DocStateWrapper::open_disk(data_root_dir).await?;
        Ok(document_state_wrapper.state().info())
    }

    pub(crate) async fn open_disk(
        peer_id: PeerId,
        encryption_key: &Option<Vec<u8>>,
        data_root_dir: &PathBuf,
    ) -> Result<Self, PeermergeError> {
        let mut document_state_wrapper = DocStateWrapper::open_disk(data_root_dir).await?;
        let state = document_state_wrapper.state();
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
        let doc_discovery_key = state.doc_discovery_key;
        let log_context = log_context(&state);

        // Open root feed
        let feeds: DashMap<[u8; 32], Arc<Mutex<Feed<FeedDiskPersistence>>>> = DashMap::new();
        let (_, root_feed) = open_disk_feed(
            data_root_dir,
            &state.doc_discovery_key,
            proxy,
            encrypted,
            encryption_key,
        )
        .await;
        feeds.insert(state.doc_discovery_key, Arc::new(Mutex::new(root_feed)));

        // Open all peer feeds
        for peer in &state.peers_state.peers {
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
            if let Some(write_peer) = state.peers_state.write_peer.clone() {
                let write_discovery_key = discovery_key_from_public_key(&write_peer.public_key);
                debug!(
                    "open_disk: peers={}, writable, proxy={proxy}, encrypted={encrypted}",
                    feeds.len() - 1
                );
                if write_peer.public_key != state.doc_public_key {
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
                if let Some((content, unapplied_entries)) =
                    document_state_wrapper.content_and_unapplied_entries_mut()
                {
                    if let Some(meta_doc_data) = &content.meta_doc_data {
                        let meta_automerge_doc = init_automerge_doc_from_data(
                            &write_peer.id,
                            &write_discovery_key,
                            meta_doc_data,
                        );
                        content.meta_automerge_doc = Some(meta_automerge_doc);
                    }
                    if let Some(user_doc_data) = &content.user_doc_data {
                        let user_automerge_doc = init_automerge_doc_from_data(
                            &write_peer.id,
                            &write_discovery_key,
                            user_doc_data,
                        );
                        content.user_automerge_doc = Some(user_automerge_doc);
                    }

                    let changed =
                        update_content(content, &doc_discovery_key, &feeds, unapplied_entries)
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
            write_discovery_key,
            proxy,
            encrypted,
            encryption_key: encryption_key.clone(),
            log_context,
        })
    }

    #[instrument(level = "debug", skip_all, fields(ctx = self.log_context))]
    pub(crate) async fn process_new_peers_broadcasted_disk(
        &mut self,
        incoming_peers: Vec<DocumentPeer>,
    ) -> bool {
        let (changed, replaced_peers, peers_to_create) = {
            let mut document_state = self.document_state.lock().await;
            document_state.process_incoming_peers(&incoming_peers).await
        };
        if changed {
            {
                // Create and insert all new feeds
                self.create_and_insert_read_disk_feeds(peers_to_create.clone())
                    .await;
            }
            {
                self.notify_peers_changed(incoming_peers, replaced_peers, peers_to_create)
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
    ) -> Self {
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
            write_discovery_key,
            encrypted,
            encryption_key,
            log_context,
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn create_and_insert_read_disk_feeds(&mut self, peers: Vec<DocumentPeer>) {
        for peer in peers {
            let discovery_key = discovery_key_from_public_key(&peer.public_key);
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
                                &peer.public_key,
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
        init_automerge_docs(document_id, peer_id, &doc_discovery_key, false, init_cb).unwrap();
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
    let content = DocumentContent::new_writer(
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
        doc_public_key,
        Some(encrypted),
        DocumentPeersState::new_writer(peer_id, &write_public_key),
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
    doc_discovery_key: &[u8; 32],
    feeds: &Arc<DashMap<[u8; 32], Arc<Mutex<Feed<T>>>>>,
    unapplied_entries: &mut UnappliedEntries,
) -> Result<bool, PeermergeError>
where
    T: RandomAccess + Debug + Send + 'static,
{
    let mut changed = false;
    for discovery_key in get_feed_discovery_keys(feeds).await {
        let (contiguous_length, entries) =
            get_new_entries(&discovery_key, None, content, &feeds).await?;

        let result = update_content_with_entries(
            entries,
            &discovery_key,
            contiguous_length,
            doc_discovery_key,
            content,
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
) -> Result<(u64, Vec<Entry>), PeermergeError>
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
    let (entries, _) = feed
        .entries(content.cursor_length(discovery_key), contiguous_length)
        .await?;
    Ok((contiguous_length, entries))
}

async fn update_content_with_entries(
    entries: Vec<Entry>,
    synced_discovery_key: &[u8; 32],
    synced_contiguous_length: u64,
    doc_discovery_key: &[u8; 32],
    content: &mut DocumentContent,
    unapplied_entries: &mut UnappliedEntries,
) -> Result<(Vec<Patch>, Vec<([u8; 32], u64)>), PeermergeError> {
    let meta_automerge_doc = content.meta_automerge_doc.as_mut().unwrap();
    let user_automerge_doc = content.user_automerge_doc.as_mut().unwrap();
    let result = apply_entries_autocommit(
        meta_automerge_doc,
        user_automerge_doc,
        synced_discovery_key,
        synced_contiguous_length,
        entries,
        unapplied_entries,
    )?;
    update_content_from_edit_result(result, doc_discovery_key, content).await
}

async fn update_content_from_edit_result(
    result: HashMap<[u8; 32], ApplyEntriesFeedChange>,
    doc_discovery_key: &[u8; 32],
    content: &mut DocumentContent,
) -> Result<(Vec<Patch>, Vec<([u8; 32], u64)>), PeermergeError> {
    let (user_patches, cursor_changes, peer_syncs, change_result) = {
        let meta_automerge_doc = content.meta_automerge_doc.as_mut().unwrap();
        let user_automerge_doc = content.user_automerge_doc.as_mut().unwrap();
        let cursor_changes: Vec<([u8; 32], u64)> = result
            .iter()
            .map(|(discovery_key, feed_change)| (*discovery_key, feed_change.length))
            .collect();
        let peer_syncs: Vec<([u8; 32], u64)> = result
            .iter()
            .filter(|(discovery_key, _)| *discovery_key != doc_discovery_key)
            .map(|(discovery_key, feed_change)| (*discovery_key, feed_change.length))
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
