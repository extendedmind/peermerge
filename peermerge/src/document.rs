use automerge::{ObjId, ObjType, Patch, Prop, ReadDoc, ScalarValue, Value};
use dashmap::DashMap;
use futures::channel::mpsc::UnboundedSender;
#[cfg(not(target_arch = "wasm32"))]
use random_access_disk::RandomAccessDisk;
use random_access_memory::RandomAccessMemory;
use random_access_storage::RandomAccess;
use std::sync::Arc;
use std::{fmt::Debug, path::PathBuf};
use tracing::{debug, instrument, warn};

#[cfg(feature = "async-std")]
use async_std::{sync::Mutex, task::yield_now};
#[cfg(feature = "tokio")]
use tokio::{sync::Mutex, task::yield_now};

use crate::automerge::{
    apply_entries_autocommit, init_automerge_doc_from_data, init_automerge_doc_from_entries,
    put_scalar_autocommit, splice_text, AutomergeDoc, UnappliedEntries,
};
use crate::common::cipher::{
    decode_doc_url, encode_doc_url, encode_document_id, encode_proxy_doc_url, DecodedDocUrl,
};
use crate::common::encoding::serialize_entry;
use crate::common::keys::{discovery_key_from_public_key, generate_keys, Keypair};
use crate::common::state::DocumentState;
use crate::common::{DocumentInfo, StateEventContent::*};
#[cfg(not(target_arch = "wasm32"))]
use crate::feed::{create_new_read_disk_feed, create_new_write_disk_feed, open_disk_feed};
#[cfg(not(target_arch = "wasm32"))]
use crate::FeedDiskPersistence;
use crate::{
    automerge::{init_automerge_doc_with_root_scalars, put_object_autocommit},
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
use crate::{DocumentId, NameDescription};

/// Document represents a single Automerge doc shared via feeds.
#[derive(derivative::Derivative)]
#[derivative(Clone(bound = ""))]
#[derive(Debug)]
pub(crate) struct Document<T, U>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send,
    U: FeedPersistence,
{
    /// Map of the feeds of this document
    feeds: Arc<DashMap<[u8; 32], Arc<Mutex<Feed<U>>>>>,
    /// The state of the document
    document_state: Arc<Mutex<DocStateWrapper<T>>>,
    /// Sender to use to communicate local changes to document
    state_event_sender: Arc<Mutex<Option<UnboundedSender<StateEvent>>>>,
    /// Locally stored prefix path, empty for memory.
    prefix: PathBuf,
    /// This peer's name, needed for creating an actor for the automerge document.
    peer_name: String,
    /// Document's name, needed for debugging.
    document_name: String,
    /// If this document is a proxy
    proxy: bool,
    /// The root discovery key
    root_discovery_key: [u8; 32],
    /// Document URL, possibly encrypted.
    doc_url: String,
    /// Whether or not this document is encrypted.
    encrypted: bool,
    /// If encrypted is true and not a proxy, the encryption key to use to decrypt feed entries.
    encryption_key: Option<Vec<u8>>,
}

impl<T, U> Document<T, U>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send + 'static,
    U: FeedPersistence,
{
    pub(crate) fn id(&self) -> DocumentId {
        self.root_discovery_key.clone()
    }

    pub(crate) async fn root_feed(&self) -> Arc<Mutex<Feed<U>>> {
        get_feed(&self.feeds, &self.root_discovery_key)
            .await
            .unwrap()
    }

    pub(crate) async fn leaf_feeds(&self) -> Vec<Arc<Mutex<Feed<U>>>> {
        let mut leaf_feeds = vec![];
        for feed_discovery_key in get_feed_discovery_keys(&self.feeds).await {
            if feed_discovery_key != self.root_discovery_key {
                leaf_feeds.push(get_feed(&self.feeds, &feed_discovery_key).await.unwrap());
            }
        }
        leaf_feeds
    }

    pub(crate) async fn leaf_feed(&self, discovery_key: &[u8; 32]) -> Option<Arc<Mutex<Feed<U>>>> {
        if discovery_key == &self.root_discovery_key {
            return None;
        }
        get_feed(&self.feeds, &discovery_key).await
    }

    pub(crate) async fn public_keys(&self) -> (Option<[u8; 32]>, Vec<[u8; 32]>) {
        let state = self.document_state.lock().await;
        let state = state.state();
        let peer_public_keys: Vec<[u8; 32]> = state
            .peers
            .iter()
            .map(|peer| peer.public_key.clone())
            .collect();
        (state.write_public_key, peer_public_keys)
    }

    #[instrument(skip_all, fields(doc_name = self.document_name))]
    pub(crate) async fn watch(&mut self, ids: Vec<ObjId>) {
        if self.proxy {
            panic!("Can not watch on a proxy");
        }
        let mut document_state = self.document_state.lock().await;
        document_state.watch(ids);
    }

    #[instrument(skip_all, fields(doc_name = self.document_name))]
    pub(crate) async fn get_id<O: AsRef<ObjId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
    ) -> anyhow::Result<Option<ObjId>> {
        if self.proxy {
            panic!("Can not get id on a proxy");
        }
        let document_state = &self.document_state;
        let result = {
            let document_state = document_state.lock().await;
            if let Some(doc) = document_state.automerge_doc() {
                match doc.get(obj, prop) {
                    Ok(result) => {
                        if let Some(result) = result {
                            Some(result.1)
                        } else {
                            None
                        }
                    }
                    Err(_err) => {
                        // TODO: Some errors should probably be errors
                        None
                    }
                }
            } else {
                unimplemented!("TODO: No proper error code for trying to get id from doc before a document is synced");
            }
        };
        Ok(result)
    }

    #[instrument(skip_all, fields(doc_name = self.document_name))]
    pub(crate) async fn get_scalar<O: AsRef<ObjId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
    ) -> anyhow::Result<Option<ScalarValue>> {
        if self.proxy {
            panic!("Can not get id on a proxy");
        }
        let document_state = &self.document_state;
        let result = {
            let document_state = document_state.lock().await;
            if let Some(doc) = document_state.automerge_doc() {
                match doc.get(obj, prop) {
                    Ok(result) => {
                        if let Some(result) = result {
                            if let Some(value) = result.0.to_scalar() {
                                Some(value.clone())
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    }
                    Err(_err) => {
                        // TODO: Some errors should probably be errors
                        None
                    }
                }
            } else {
                unimplemented!("TODO: No proper error code for trying to get id from doc before a document is synced");
            }
        };
        Ok(result)
    }

    #[instrument(skip_all, fields(doc_name = self.document_name))]
    pub(crate) async fn get<O: AsRef<ObjId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
    ) -> anyhow::Result<Option<(Value, ObjId)>> {
        if self.proxy {
            panic!("Can not get document values on a proxy");
        }
        let document_state = &self.document_state;
        let result = {
            let document_state = document_state.lock().await;
            if let Some(doc) = document_state.automerge_doc() {
                match doc.get(obj, prop) {
                    Ok(result) => {
                        if let Some(result) = result {
                            let value = result.0.to_owned();
                            let id = result.1.to_owned();
                            Some((value, id))
                        } else {
                            None
                        }
                    }
                    Err(_err) => {
                        // TODO: Some errors should probably be errors
                        None
                    }
                }
            } else {
                unimplemented!("TODO: No proper error code for trying to get from doc before a document is synced");
            }
        };
        Ok(result)
    }

    #[instrument(skip_all, fields(doc_name = self.document_name))]
    pub(crate) async fn realize_text<O: AsRef<ObjId>>(
        &self,
        obj: O,
    ) -> anyhow::Result<Option<String>> {
        if self.proxy {
            panic!("Can not realize text on a proxy");
        }
        let document_state = &self.document_state;
        let result = {
            let document_state = document_state.lock().await;
            if let Some(doc) = document_state.automerge_doc() {
                let length = doc.length(obj.as_ref().clone());
                let mut chars = Vec::with_capacity(length);
                for i in 0..length {
                    match doc.get(obj.as_ref().clone(), i) {
                        Ok(result) => {
                            if let Some(result) = result {
                                let scalar = result.0.to_scalar().unwrap();
                                match scalar {
                                    ScalarValue::Str(character) => {
                                        chars.push(character.to_string());
                                    }
                                    _ => {
                                        panic!("Not a char")
                                    }
                                }
                            }
                        }
                        Err(_err) => {
                            panic!("Not a char")
                        }
                    };
                }
                let string: String = chars.into_iter().collect();
                Some(string)
            } else {
                unimplemented!("TODO: No proper error code for trying to get from doc before a document is synced");
            }
        };
        Ok(result)
    }

    #[instrument(skip_all, fields(doc_name = self.document_name))]
    pub(crate) async fn put_object<O: AsRef<ObjId>, P: Into<Prop>>(
        &mut self,
        obj: O,
        prop: P,
        object: ObjType,
    ) -> anyhow::Result<ObjId> {
        if self.proxy {
            panic!("Can not put object on a proxy");
        }
        let id = {
            let mut document_state = self.document_state.lock().await;
            let (entry, id) = if let Some(doc) = document_state.automerge_doc_mut() {
                put_object_autocommit(doc, obj, prop, object).unwrap()
            } else {
                unimplemented!(
                    "TODO: No proper error code for trying to change before a document is synced"
                );
            };

            let write_discovery_key = document_state.write_discovery_key();
            let length = {
                let write_feed = get_feed(&self.feeds, &write_discovery_key).await.unwrap();
                let mut write_feed = write_feed.lock().await;
                write_feed.append(&serialize_entry(&entry)).await?
            };
            document_state
                .set_cursor(&write_discovery_key, length)
                .await;
            id
        };
        {
            self.notify_of_document_changes().await;
        }
        Ok(id)
    }

    #[instrument(skip_all, fields(doc_name = self.document_name))]
    pub(crate) async fn put_scalar<O: AsRef<ObjId>, P: Into<Prop>, V: Into<ScalarValue>>(
        &mut self,
        obj: O,
        prop: P,
        value: V,
    ) -> anyhow::Result<()> {
        if self.proxy {
            panic!("Can not put scalar on a proxy");
        }
        {
            let mut document_state = self.document_state.lock().await;
            let entry = if let Some(doc) = document_state.automerge_doc_mut() {
                put_scalar_autocommit(doc, obj, prop, value).unwrap()
            } else {
                unimplemented!(
                    "TODO: No proper error code for trying to change before a document is synced"
                );
            };

            let write_discovery_key = document_state.write_discovery_key();
            let length = {
                let write_feed = get_feed(&self.feeds, &write_discovery_key).await.unwrap();
                let mut write_feed = write_feed.lock().await;
                write_feed.append(&serialize_entry(&entry)).await?
            };
            document_state
                .set_cursor(&write_discovery_key, length)
                .await;
        };
        {
            self.notify_of_document_changes().await;
        }
        Ok(())
    }

    #[instrument(skip_all, fields(doc_name = self.document_name))]
    pub(crate) async fn splice_text<O: AsRef<ObjId>>(
        &mut self,
        obj: O,
        index: usize,
        delete: usize,
        text: &str,
    ) -> anyhow::Result<()> {
        if self.proxy {
            panic!("Can not splice text on a proxy");
        }
        {
            let mut document_state = self.document_state.lock().await;
            let entry = if let Some(doc) = document_state.automerge_doc_mut() {
                splice_text(doc, obj, index, delete, text)?
            } else {
                unimplemented!(
                "TODO: No proper error code for trying to splice text before a document is synced"
            );
            };
            let write_discovery_key = document_state.write_discovery_key();
            let length = {
                let write_feed_wrapper = get_feed(&self.feeds, &write_discovery_key).await.unwrap();
                let mut write_feed = write_feed_wrapper.lock().await;
                write_feed.append(&serialize_entry(&entry)).await?
            };
            document_state
                .set_cursor(&write_discovery_key, length)
                .await;
        }
        {
            self.notify_of_document_changes().await;
        }
        Ok(())
    }

    #[instrument(skip_all, fields(doc_name = self.document_name))]
    pub(crate) async fn cork(&mut self) {
        if self.proxy {
            panic!("Can not cork a proxy");
        }
        let document_state = self.document_state.lock().await;
        let write_discovery_key = document_state.write_discovery_key();
        let write_feed_wrapper = get_feed(&self.feeds, &write_discovery_key).await.unwrap();
        let mut write_feed = write_feed_wrapper.lock().await;
        write_feed.cork();
    }

    #[instrument(skip_all, fields(doc_name = self.document_name))]
    pub(crate) async fn uncork(&mut self) -> anyhow::Result<()> {
        if self.proxy {
            panic!("Can not uncork a proxy");
        }
        let document_state = self.document_state.lock().await;
        let write_discovery_key = document_state.write_discovery_key();
        let write_feed_wrapper = get_feed(&self.feeds, &write_discovery_key).await.unwrap();
        let mut write_feed = write_feed_wrapper.lock().await;
        write_feed.uncork().await?;
        Ok(())
    }

    #[instrument(skip_all, fields(doc_name = self.document_name))]
    pub(crate) fn doc_url(&self) -> String {
        self.doc_url.clone()
    }

    #[instrument(skip_all, fields(doc_name = self.document_name))]
    pub(crate) fn proxy_doc_url(&self) -> String {
        encode_proxy_doc_url(&self.doc_url)
    }

    #[instrument(skip_all, fields(doc_name = self.document_name))]
    pub(crate) fn encryption_key(&self) -> Option<Vec<u8>> {
        if self.proxy {
            panic!("A proxy does not store the encryption key");
        }
        self.encryption_key.clone()
    }

    #[instrument(level = "debug", skip_all, fields(doc_name = self.document_name))]
    pub(crate) async fn take_patches(&mut self) -> Vec<Patch> {
        let mut document_state = self.document_state.lock().await;
        if let Some(doc) = document_state.automerge_doc_mut() {
            doc.observer().take_patches()
        } else {
            vec![]
        }
    }

    #[instrument(level = "debug", skip_all, fields(doc_name = self.document_name))]
    pub(crate) async fn process_peer_synced(
        &mut self,
        discovery_key: [u8; 32],
        synced_contiguous_length: u64,
    ) -> Vec<StateEvent> {
        if self.proxy {
            if &discovery_key != &self.root_discovery_key {
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
            let (document_initialized, mut patches, peer_syncs) =
                if let Some((content, unapplied_entries)) =
                    document_state.content_and_unapplied_entries_mut()
                {
                    let (patches, new_peer_headers, peer_syncs) = self
                        .update_synced_content(
                            &discovery_key,
                            synced_contiguous_length,
                            content,
                            unapplied_entries,
                        )
                        .await
                        .unwrap();
                    document_state
                        .persist_content_and_new_peer_headers(new_peer_headers)
                        .await;
                    (false, patches, peer_syncs)
                } else {
                    let write_discovery_key = document_state.write_discovery_key();
                    let unapplied_entries = document_state.unappliend_entries_mut();
                    if let Some((content, new_peer_headers, peer_syncs)) = self
                        .create_content(
                            &discovery_key,
                            synced_contiguous_length,
                            &write_discovery_key,
                            unapplied_entries,
                        )
                        .await
                        .unwrap()
                    {
                        document_state
                            .set_content_and_new_peer_headers(content, new_peer_headers)
                            .await;
                        (true, vec![], peer_syncs)
                    } else {
                        // Could not create content from this peer's data, needs more peers
                        (false, vec![], vec![])
                    }
                };

            // Filter out unwatched patches
            let watched_ids = &document_state.state().watched_ids;

            patches.retain(|patch| match patch {
                Patch::Put { obj, .. } => watched_ids.contains(obj),
                Patch::Insert { obj, .. } => watched_ids.contains(obj),
                Patch::Delete { obj, .. } => watched_ids.contains(obj),
                Patch::Increment { obj, .. } => watched_ids.contains(obj),
                Patch::Expose { obj, .. } => watched_ids.contains(obj),
                Patch::Splice { obj, .. } => watched_ids.contains(obj),
            });
            let mut state_events: Vec<StateEvent> = peer_syncs
                .iter()
                .map(|sync| {
                    let name = document_state.peer_name(&sync.0).unwrap();
                    StateEvent::new(self.id(), PeerSynced((Some(name), sync.0.clone(), sync.1)))
                })
                .collect();
            if document_initialized {
                state_events.push(StateEvent::new(self.id(), DocumentInitialized()));
            }
            if patches.len() > 0 {
                state_events.push(StateEvent::new(self.id(), DocumentChanged(patches)));
            }
            state_events
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

    #[instrument(level = "debug", skip_all, fields(doc_name = self.document_name))]
    async fn notify_of_document_changes(&mut self) {
        let mut document_state = self.document_state.lock().await;
        if let Some(doc) = document_state.automerge_doc_mut() {
            let mut state_event_sender = self.state_event_sender.lock().await;
            if let Some(sender) = state_event_sender.as_mut() {
                if sender.is_closed() {
                    *state_event_sender = None;
                } else {
                    let patches = doc.observer().take_patches();
                    if patches.len() > 0 {
                        sender
                            .unbounded_send(StateEvent::new(
                                self.root_discovery_key.clone(),
                                DocumentChanged(patches),
                            ))
                            .unwrap();
                    }
                }
            }
        }
    }

    #[instrument(level = "debug", skip_all, fields(doc_name = self.document_name))]
    async fn notify_new_peers_created(&mut self, public_keys: Vec<[u8; 32]>) {
        // Send message to doc feed that new peers have been created to get all open protocols to
        // open channels to it.
        let doc_feed = get_feed(&self.feeds, &self.root_discovery_key)
            .await
            .unwrap();
        let mut doc_feed = doc_feed.lock().await;
        doc_feed
            .notify_new_peers_created(self.root_discovery_key.clone(), public_keys)
            .await
            .unwrap();
    }

    async fn create_content(
        &self,
        synced_discovery_key: &[u8; 32],
        synced_contiguous_length: u64,
        write_discovery_key: &[u8; 32],
        unapplied_entries: &mut UnappliedEntries,
    ) -> anyhow::Result<
        Option<(
            DocumentContent,
            Vec<([u8; 32], NameDescription)>,
            Vec<([u8; 32], u64)>,
        )>,
    > {
        // The document starts from the doc feed, so get that first
        if synced_discovery_key == &self.root_discovery_key {
            assert_eq!(
                synced_contiguous_length, 1,
                "There can never be more than one entry in the root feed"
            );
            let init_entry: Entry = {
                let doc_feed = get_feed(&self.feeds, &self.root_discovery_key)
                    .await
                    .unwrap();
                let mut doc_feed = doc_feed.lock().await;
                let mut entries = doc_feed.entries(0, synced_contiguous_length).await?;
                entries.swap_remove(0)
            };

            // Create DocContent from the feed
            let (mut automerge_doc, data, result) = init_automerge_doc_from_entries(
                &self.peer_name,
                write_discovery_key,
                synced_discovery_key,
                init_entry,
                unapplied_entries,
            )?;
            let cursors: Vec<DocumentCursor> = result
                .iter()
                .map(|(discovery_key, feed_change)| {
                    DocumentCursor::new(discovery_key.clone(), feed_change.length)
                })
                .collect();

            // Empty patches queue, document is just initialized, so they can be safely ignored.
            automerge_doc.observer().take_patches();

            let new_peer_headers: Vec<([u8; 32], NameDescription)> = result
                .iter()
                .filter(|(_, feed_change)| feed_change.peer_header.is_some())
                .map(|(discovery_key, feed_change)| {
                    (
                        discovery_key.clone(),
                        feed_change.peer_header.clone().unwrap(),
                    )
                })
                .collect();
            let peer_syncs: Vec<([u8; 32], u64)> = result
                .iter()
                // The root feed is not a peer.
                .filter(|(discovery_key, _)| *discovery_key != &self.root_discovery_key)
                .map(|(discovery_key, feed_change)| (discovery_key.clone(), feed_change.length))
                .collect();
            Ok(Some((
                DocumentContent::new(data, cursors, automerge_doc),
                new_peer_headers,
                peer_syncs,
            )))
        } else {
            // Got first some other peer's data, need to store it to unapplied changes
            let feed = get_feed(&self.feeds, synced_discovery_key).await.unwrap();
            let mut feed = feed.lock().await;
            let current_length = unapplied_entries.current_length(synced_discovery_key);
            let entries = feed
                .entries(current_length, synced_contiguous_length - current_length)
                .await?;
            let mut new_length = current_length + 1;
            for entry in entries {
                unapplied_entries.add(synced_discovery_key, new_length, entry);
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
    ) -> anyhow::Result<(
        Vec<Patch>,
        Vec<([u8; 32], NameDescription)>,
        Vec<([u8; 32], u64)>,
    )> {
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
            &self.root_discovery_key,
            content,
            unapplied_entries,
        )
        .await
    }
}

//////////////////////////////////////////////////////
//
// Memory

impl Document<RandomAccessMemory, FeedMemoryPersistence> {
    pub(crate) async fn create_new_memory<P: Into<Prop>, V: Into<ScalarValue>>(
        peer_header: &NameDescription,
        document_header: NameDescription,
        doc_root_scalars: Vec<(P, V)>,
        encrypted: bool,
    ) -> Self {
        let result =
            prepare_create(peer_header, &document_header, doc_root_scalars, encrypted).await;

        // Create the root memory feed
        let (root_feed_length, root_feed, root_encryption_key) = create_new_write_memory_feed(
            result.root_key_pair,
            serialize_entry(&Entry::new_init_doc(
                document_header.clone(),
                result.data.clone(),
            )),
            encrypted,
            &None,
        )
        .await;
        let doc_url = encode_doc_url(
            &result.root_public_key,
            &document_header,
            &root_encryption_key,
        );

        // Create a write memory feed
        let (write_feed_length, write_feed, _) = create_new_write_memory_feed(
            result.write_key_pair,
            serialize_entry(&Entry::new_init_peer(
                peer_header.clone(),
                result.root_discovery_key,
            )),
            encrypted,
            &root_encryption_key,
        )
        .await;

        // Augment state with content
        let content = DocumentContent::new(
            result.data,
            vec![
                DocumentCursor::new(result.root_discovery_key.clone(), root_feed_length),
                DocumentCursor::new(result.write_discovery_key.clone(), write_feed_length),
            ],
            result.automerge_doc,
        );
        let mut state = result.state;
        state.content = Some(content);

        Self::new_memory(
            (result.root_discovery_key.clone(), root_feed),
            Some((result.write_discovery_key.clone(), write_feed)),
            peer_header,
            state,
            encrypted,
            &doc_url,
            root_encryption_key,
        )
        .await
    }

    pub(crate) async fn attach_writer_memory(
        peer_header: &NameDescription,
        doc_url: &str,
        encryption_key: &Option<Vec<u8>>,
    ) -> Self {
        let proxy = false;

        // Process keys from doc URL
        let decoded_doc_url = decode_doc_url(doc_url, &encryption_key);
        let encrypted = if let Some(encrypted) = decoded_doc_url.encrypted {
            if encrypted && encryption_key.is_none() {
                panic!("Can not attach a peer to an encrypted document without an encryption key");
            }
            encrypted
        } else {
            panic!("Given doc url can only be used for proxying");
        };

        // Create the root feed
        let root_discovery_key = discovery_key_from_public_key(&decoded_doc_url.root_public_key);
        let (_, root_feed) = create_new_read_memory_feed(
            &decoded_doc_url.root_public_key,
            proxy,
            encrypted,
            encryption_key,
        )
        .await;

        // Create the write feed
        let (write_key_pair, write_discovery_key) = generate_keys();
        let write_public_key = *write_key_pair.public.as_bytes();
        let (_, write_feed, _) = create_new_write_memory_feed(
            write_key_pair,
            serialize_entry(&Entry::new_init_peer(
                peer_header.clone(),
                root_discovery_key,
            )),
            encrypted,
            &encryption_key,
        )
        .await;

        // Initialize document state
        let state =
            DocumentState::new(decoded_doc_url, proxy, vec![], Some(write_public_key), None);

        Self::new_memory(
            (root_discovery_key, root_feed),
            Some((write_discovery_key, write_feed)),
            peer_header,
            state,
            encrypted,
            doc_url,
            encryption_key.clone(),
        )
        .await
    }

    pub(crate) async fn attach_proxy_memory(peer_header: &NameDescription, doc_url: &str) -> Self {
        let proxy = true;
        let doc_url = encode_proxy_doc_url(doc_url);
        let encrypted = false;

        // Process keys from doc URL
        let decoded_doc_url = decode_doc_url(&doc_url, &None);

        // Create the root feed
        let root_discovery_key = discovery_key_from_public_key(&decoded_doc_url.root_public_key);
        let (_, root_feed) =
            create_new_read_memory_feed(&decoded_doc_url.root_public_key, proxy, encrypted, &None)
                .await;

        // Initialize document state
        let state = DocumentState::new(decoded_doc_url, proxy, vec![], None, None);

        Self::new_memory(
            (root_discovery_key, root_feed),
            None,
            peer_header,
            state,
            encrypted,
            &doc_url,
            None,
        )
        .await
    }

    #[instrument(level = "debug", skip_all, fields(doc_name = self.document_name))]
    pub(crate) async fn process_new_peers_broadcasted_memory(
        &mut self,
        public_keys: Vec<[u8; 32]>,
    ) -> bool {
        let changed = {
            let mut document_state = self.document_state.lock().await;
            document_state
                .add_peer_public_keys_to_state(public_keys.clone())
                .await
        };
        if changed {
            {
                // Create and insert all new feeds
                self.create_and_insert_read_memory_feeds(public_keys.clone())
                    .await;
            }
            {
                self.notify_new_peers_created(public_keys).await;
            }
        }
        changed
    }

    async fn new_memory(
        root_feed: ([u8; 32], Feed<FeedMemoryPersistence>),
        write_feed: Option<([u8; 32], Feed<FeedMemoryPersistence>)>,
        peer_header: &NameDescription,
        state: DocumentState,
        encrypted: bool,
        encrypted_doc_url: &str,
        encryption_key: Option<Vec<u8>>,
    ) -> Self {
        let feeds: DashMap<[u8; 32], Arc<Mutex<Feed<FeedMemoryPersistence>>>> = DashMap::new();
        let (root_discovery_key, root_feed) = root_feed;
        feeds.insert(root_discovery_key, Arc::new(Mutex::new(root_feed)));

        if let Some((write_discovery_key, write_feed)) = write_feed {
            feeds.insert(write_discovery_key, Arc::new(Mutex::new(write_feed)));
        }
        let proxy = state.proxy;
        let document_name = document_name_or_default(&state.document_header);
        let document_state = DocStateWrapper::new_memory(state).await;
        Self {
            feeds: Arc::new(feeds),
            document_state: Arc::new(Mutex::new(document_state)),
            state_event_sender: Arc::new(Mutex::new(None)),
            prefix: PathBuf::new(),
            document_name,
            proxy,
            root_discovery_key,
            doc_url: encrypted_doc_url.to_string(),
            peer_name: peer_header.name.clone(),
            encrypted,
            encryption_key,
        }
    }

    async fn create_and_insert_read_memory_feeds(&mut self, public_keys: Vec<[u8; 32]>) {
        for public_key in public_keys {
            let discovery_key = discovery_key_from_public_key(&public_key);
            // Make sure to insert only once even if two protocols notice the same new
            // feed at the same time using the entry API.

            // There is a deadlock possibility with entry(), so we need to loop and yield
            let mut entry_found = false;
            while !entry_found {
                if let Some(entry) = self.feeds.try_entry(discovery_key.clone()) {
                    match entry {
                        dashmap::mapref::entry::Entry::Occupied(_) => {
                            debug!("Concurrent creating of feeds noticed, continuing.");
                        }
                        dashmap::mapref::entry::Entry::Vacant(vacant) => {
                            let (_, feed) = create_new_read_memory_feed(
                                &public_key,
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
                    yield_now().await;
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
    pub(crate) async fn create_new_disk<P: Into<Prop>, V: Into<ScalarValue>>(
        peer_header: &NameDescription,
        document_header: NameDescription,
        doc_root_scalars: Vec<(P, V)>,
        encrypted: bool,
        data_root_dir: &PathBuf,
    ) -> Self {
        let result =
            prepare_create(peer_header, &document_header, doc_root_scalars, encrypted).await;
        let postfix = encode_document_id(&result.root_discovery_key);
        let data_root_dir = data_root_dir.join(postfix);

        // Create the root disk feed
        let (root_feed_length, root_feed, root_encryption_key) = create_new_write_disk_feed(
            &data_root_dir,
            result.root_key_pair,
            &result.root_discovery_key,
            serialize_entry(&Entry::new_init_doc(
                document_header.clone(),
                result.data.clone(),
            )),
            encrypted,
            &None,
        )
        .await;
        let doc_url = encode_doc_url(
            &result.root_public_key,
            &document_header,
            &root_encryption_key,
        );

        // Create a write disk feed
        let (write_feed_length, write_feed, _) = create_new_write_disk_feed(
            &data_root_dir,
            result.write_key_pair,
            &result.write_discovery_key,
            serialize_entry(&Entry::new_init_peer(
                peer_header.clone(),
                result.root_discovery_key,
            )),
            encrypted,
            &root_encryption_key,
        )
        .await;

        // Augment state with content
        let content = DocumentContent::new(
            result.data,
            vec![
                DocumentCursor::new(result.root_discovery_key.clone(), root_feed_length),
                DocumentCursor::new(result.write_discovery_key.clone(), write_feed_length),
            ],
            result.automerge_doc,
        );
        let mut state = result.state;
        state.content = Some(content);

        Self::new_disk(
            (result.root_discovery_key.clone(), root_feed),
            Some((result.write_discovery_key.clone(), write_feed)),
            peer_header,
            state,
            encrypted,
            &doc_url,
            root_encryption_key,
            &data_root_dir,
        )
        .await
    }

    pub(crate) async fn attach_writer_disk(
        peer_header: &NameDescription,
        doc_url: &str,
        encryption_key: &Option<Vec<u8>>,
        data_root_dir: &PathBuf,
    ) -> Self {
        let proxy = false;

        // Process keys from doc URL
        let decoded_doc_url = decode_doc_url(doc_url, &encryption_key);
        let encrypted = if let Some(encrypted) = decoded_doc_url.encrypted {
            if encrypted && encryption_key.is_none() {
                panic!("Can not attach a peer to an encrypted document without an encryption key");
            }
            encrypted
        } else {
            panic!("Given doc url can only be used for proxying");
        };

        // Create the root feed
        let root_discovery_key = discovery_key_from_public_key(&decoded_doc_url.root_public_key);
        let postfix = encode_document_id(&root_discovery_key);
        let data_root_dir = data_root_dir.join(postfix);
        let (_, root_feed) = create_new_read_disk_feed(
            &data_root_dir,
            &decoded_doc_url.root_public_key,
            &root_discovery_key,
            proxy,
            encrypted,
            encryption_key,
        )
        .await;

        // Create the write feed
        let (write_key_pair, write_discovery_key) = generate_keys();
        let write_public_key = *write_key_pair.public.as_bytes();
        let (_, write_feed, _) = create_new_write_disk_feed(
            &data_root_dir,
            write_key_pair,
            &write_discovery_key,
            serialize_entry(&Entry::new_init_peer(
                peer_header.clone(),
                root_discovery_key,
            )),
            encrypted,
            &encryption_key,
        )
        .await;

        // Initialize document state
        let state =
            DocumentState::new(decoded_doc_url, proxy, vec![], Some(write_public_key), None);

        Self::new_disk(
            (root_discovery_key, root_feed),
            Some((write_discovery_key, write_feed)),
            peer_header,
            state,
            encrypted,
            &doc_url,
            encryption_key.clone(),
            &data_root_dir,
        )
        .await
    }

    pub(crate) async fn attach_proxy_disk(
        peer_header: &NameDescription,
        doc_url: &str,
        data_root_dir: &PathBuf,
    ) -> Self {
        let proxy = true;
        let doc_url = encode_proxy_doc_url(doc_url);
        let encrypted = false;

        // Process keys from doc URL
        let decoded_doc_url = decode_doc_url(&doc_url, &None);

        // Create the root feed
        let root_discovery_key = discovery_key_from_public_key(&decoded_doc_url.root_public_key);
        let postfix = encode_document_id(&root_discovery_key);
        let data_root_dir = data_root_dir.join(postfix);
        let (_, root_feed) = create_new_read_disk_feed(
            &data_root_dir,
            &decoded_doc_url.root_public_key,
            &root_discovery_key,
            proxy,
            encrypted,
            &None,
        )
        .await;

        // Initialize document state
        let state = DocumentState::new(decoded_doc_url, proxy, vec![], None, None);

        Self::new_disk(
            (root_discovery_key, root_feed),
            None,
            peer_header,
            state,
            encrypted,
            &doc_url,
            None,
            &data_root_dir,
        )
        .await
    }

    pub(crate) async fn info_disk(data_root_dir: &PathBuf) -> DocumentInfo {
        let document_state_wrapper = DocStateWrapper::open_disk(data_root_dir).await;
        let state = document_state_wrapper.state();
        DocumentInfo {
            document_id: state.root_discovery_key,
            doc_url_info: state.doc_url_info(),
            document_header: state.document_header.clone(),
        }
    }

    pub(crate) async fn open_disk(
        peer_header: &NameDescription,
        encryption_key: &Option<Vec<u8>>,
        data_root_dir: &PathBuf,
    ) -> Self {
        let mut document_state_wrapper = DocStateWrapper::open_disk(data_root_dir).await;
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
        let root_discovery_key = state.root_discovery_key.clone();
        let doc_url = state.doc_url(encryption_key);
        let document_name = document_name_or_default(&state.document_header);

        // Open root feed
        let feeds: DashMap<[u8; 32], Arc<Mutex<Feed<FeedDiskPersistence>>>> = DashMap::new();
        let (_, root_feed) = open_disk_feed(
            &data_root_dir,
            &state.root_discovery_key,
            proxy,
            encrypted,
            encryption_key,
        )
        .await;
        feeds.insert(state.root_discovery_key, Arc::new(Mutex::new(root_feed)));

        // Open all peer feeds
        for peer in &state.peers {
            let (_, peer_feed) = open_disk_feed(
                &data_root_dir,
                &peer.discovery_key,
                proxy,
                encrypted,
                encryption_key,
            )
            .await;
            feeds.insert(peer.discovery_key, Arc::new(Mutex::new(peer_feed)));
        }

        // Open write feed, if any
        let feeds = if let Some(write_public_key) = state.write_public_key {
            let write_discovery_key = discovery_key_from_public_key(&write_public_key);
            if write_public_key != state.root_public_key {
                let (_, write_feed) = open_disk_feed(
                    &data_root_dir,
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
                let doc = init_automerge_doc_from_data(
                    &peer_header.name,
                    &write_discovery_key,
                    &content.data,
                );
                content.automerge_doc = Some(doc);

                let (changed, new_peer_headers) =
                    update_content(content, &root_discovery_key, &feeds, unapplied_entries)
                        .await
                        .unwrap();
                if changed {
                    document_state_wrapper
                        .persist_content_and_new_peer_headers(new_peer_headers)
                        .await;
                }
            }
            feeds
        } else {
            Arc::new(feeds)
        };

        // Create Document
        Self {
            feeds,
            document_state: Arc::new(Mutex::new(document_state_wrapper)),
            state_event_sender: Arc::new(Mutex::new(None)),
            prefix: data_root_dir.clone(),
            root_discovery_key,
            doc_url,
            peer_name: peer_header.name.clone(),
            document_name,
            proxy,
            encrypted,
            encryption_key: encryption_key.clone(),
        }
    }

    #[instrument(level = "debug", skip_all, fields(doc_name = self.document_name))]
    pub(crate) async fn process_new_peers_broadcasted_disk(
        &mut self,
        public_keys: Vec<[u8; 32]>,
    ) -> bool {
        let changed = {
            let mut document_state = self.document_state.lock().await;
            document_state
                .add_peer_public_keys_to_state(public_keys.clone())
                .await
        };
        if changed {
            {
                // Create and insert all new feeds
                self.create_and_insert_read_disk_feeds(public_keys.clone())
                    .await;
            }
            {
                self.notify_new_peers_created(public_keys).await;
            }
        }
        changed
    }

    async fn new_disk(
        root_feed: ([u8; 32], Feed<FeedDiskPersistence>),
        write_feed: Option<([u8; 32], Feed<FeedDiskPersistence>)>,
        peer_header: &NameDescription,
        state: DocumentState,
        encrypted: bool,
        encrypted_doc_url: &str,
        encryption_key: Option<Vec<u8>>,
        data_root_dir: &PathBuf,
    ) -> Self {
        let feeds: DashMap<[u8; 32], Arc<Mutex<Feed<FeedDiskPersistence>>>> = DashMap::new();
        let (doc_discovery_key, doc_feed) = root_feed;
        feeds.insert(doc_discovery_key, Arc::new(Mutex::new(doc_feed)));
        if let Some((write_discovery_key, write_feed)) = write_feed {
            feeds.insert(write_discovery_key, Arc::new(Mutex::new(write_feed)));
        }
        let proxy = state.proxy;
        let document_name = document_name_or_default(&state.document_header);
        let document_state = DocStateWrapper::new_disk(state, &data_root_dir).await;

        Self {
            feeds: Arc::new(feeds),
            document_state: Arc::new(Mutex::new(document_state)),
            state_event_sender: Arc::new(Mutex::new(None)),
            prefix: data_root_dir.clone(),
            document_name,
            proxy,
            root_discovery_key: doc_discovery_key,
            doc_url: encrypted_doc_url.to_string(),
            peer_name: peer_header.name.clone(),
            encrypted,
            encryption_key,
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn create_and_insert_read_disk_feeds(&mut self, public_keys: Vec<[u8; 32]>) {
        for public_key in public_keys {
            let discovery_key = discovery_key_from_public_key(&public_key);
            // Make sure to insert only once even if two protocols notice the same new
            // feed at the same time using the entry API.

            // There is a deadlock possibility with entry(), so we need to loop and yield
            let mut entry_found = false;
            while !entry_found {
                if let Some(entry) = self.feeds.try_entry(discovery_key.clone()) {
                    match entry {
                        dashmap::mapref::entry::Entry::Occupied(_) => {
                            debug!("Concurrent creating of feeds noticed, continuing.");
                        }
                        dashmap::mapref::entry::Entry::Vacant(vacant) => {
                            let (_, feed) = create_new_read_disk_feed(
                                &self.prefix,
                                &public_key,
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
                    yield_now().await;
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
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send + 'static,
    U: FeedPersistence,
{
    loop {
        match documents.try_get(document_id) {
            dashmap::try_result::TryResult::Absent => {
                return None;
            }
            dashmap::try_result::TryResult::Locked => {
                yield_now().await;
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
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send + 'static,
    U: FeedPersistence,
{
    // I believe this needs to be resolved xacrimon/dashmap/issues/151 for this to guarantee
    // to not deadlock.
    documents.iter().map(|multi| multi.key().clone()).collect()
}

struct PrepareCreateResult {
    root_key_pair: Keypair,
    root_discovery_key: [u8; 32],
    root_public_key: [u8; 32],
    write_key_pair: Keypair,
    write_discovery_key: [u8; 32],
    automerge_doc: AutomergeDoc,
    data: Vec<u8>,
    state: DocumentState,
}

async fn prepare_create<P: Into<Prop>, V: Into<ScalarValue>>(
    peer_header: &NameDescription,
    document_header: &NameDescription,
    doc_root_scalars: Vec<(P, V)>,
    encrypted: bool,
) -> PrepareCreateResult {
    // Generate a root feed key pair, its discovery key and the public key string
    let (root_key_pair, root_discovery_key) = generate_keys();
    let root_public_key = *root_key_pair.public.as_bytes();

    // Generate a writeable feed key pair, its discovery key and the public key string
    let (write_key_pair, write_discovery_key) = generate_keys();
    let write_public_key = *write_key_pair.public.as_bytes();

    // Initialize the document
    let (automerge_doc, data) = init_automerge_doc_with_root_scalars(
        &peer_header.name,
        &root_discovery_key,
        doc_root_scalars,
    );

    // Initialize document state, will be filled later with content
    let decoded_doc_url =
        DecodedDocUrl::new(root_public_key.clone(), document_header.clone(), encrypted);
    let state = DocumentState::new(decoded_doc_url, false, vec![], Some(write_public_key), None);

    PrepareCreateResult {
        root_key_pair,
        root_discovery_key,
        root_public_key,
        write_key_pair,
        write_discovery_key,
        automerge_doc,
        data,
        state,
    }
}

async fn update_content<T>(
    content: &mut DocumentContent,
    root_discovery_key: &[u8; 32],
    feeds: &Arc<DashMap<[u8; 32], Arc<Mutex<Feed<T>>>>>,
    unapplied_entries: &mut UnappliedEntries,
) -> anyhow::Result<(bool, Vec<([u8; 32], NameDescription)>)>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send + 'static,
{
    let mut new_peer_headers: Vec<([u8; 32], NameDescription)> = vec![];
    let mut changed = false;
    for discovery_key in get_feed_discovery_keys(feeds).await {
        let (contiguous_length, entries) =
            get_new_entries(&discovery_key, None, content, &feeds).await?;

        let result = update_content_with_entries(
            entries,
            &discovery_key,
            contiguous_length,
            root_discovery_key,
            content,
            unapplied_entries,
        )
        .await?;
        if !result.0.is_empty() {
            changed = true;
        }
        new_peer_headers.extend(result.1);
    }

    Ok((changed, new_peer_headers))
}

async fn get_new_entries<T>(
    discovery_key: &[u8; 32],
    known_contiguous_length: Option<u64>,
    content: &mut DocumentContent,
    feeds: &Arc<DashMap<[u8; 32], Arc<Mutex<Feed<T>>>>>,
) -> anyhow::Result<(u64, Vec<Entry>)>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send + 'static,
{
    let feed = get_feed(feeds, discovery_key).await.unwrap();
    let mut feed = feed.lock().await;
    let contiguous_length = if let Some(known_contiguous_length) = known_contiguous_length {
        known_contiguous_length
    } else {
        feed.contiguous_length().await
    };
    let entries = feed
        .entries(content.cursor_length(discovery_key), contiguous_length)
        .await?;
    Ok((contiguous_length, entries))
}

async fn update_content_with_entries(
    entries: Vec<Entry>,
    synced_discovery_key: &[u8; 32],
    synced_contiguous_length: u64,
    root_discovery_key: &[u8; 32],
    content: &mut DocumentContent,
    unapplied_entries: &mut UnappliedEntries,
) -> anyhow::Result<(
    Vec<Patch>,
    Vec<([u8; 32], NameDescription)>,
    Vec<([u8; 32], u64)>,
)> {
    let (patches, new_headers, cursor_changes, peer_syncs) = {
        let automerge_doc = content.automerge_doc.as_mut().unwrap();
        let result = apply_entries_autocommit(
            automerge_doc,
            synced_discovery_key,
            synced_contiguous_length,
            entries,
            unapplied_entries,
        )?;
        let new_peer_headers: Vec<([u8; 32], NameDescription)> = result
            .iter()
            .filter(|(_, feed_change)| feed_change.peer_header.is_some())
            .map(|(discovery_key, feed_change)| {
                (
                    discovery_key.clone(),
                    feed_change.peer_header.clone().unwrap(),
                )
            })
            .collect();

        let cursor_changes: Vec<([u8; 32], u64)> = result
            .iter()
            .map(|(discovery_key, feed_change)| (discovery_key.clone(), feed_change.length))
            .collect();
        let peer_syncs: Vec<([u8; 32], u64)> = result
            .iter()
            .filter(|(discovery_key, _)| *discovery_key != root_discovery_key)
            .map(|(discovery_key, feed_change)| (discovery_key.clone(), feed_change.length))
            .collect();

        let patches = if !peer_syncs.is_empty() {
            automerge_doc.observer().take_patches()
        } else {
            vec![]
        };
        (patches, new_peer_headers, cursor_changes, peer_syncs)
    };

    for (discovery_key, length) in cursor_changes {
        content.set_cursor(&discovery_key, length);
    }

    Ok((patches, new_headers, peer_syncs))
}

fn document_name_or_default(document_header: &Option<NameDescription>) -> String {
    document_header
        .clone()
        .map_or("unknown".to_string(), |header| header.name.clone())
}
