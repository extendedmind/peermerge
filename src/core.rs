use async_channel::{unbounded, Receiver, Sender};
use async_std::sync::{Arc, Mutex};
#[cfg(not(target_arch = "wasm32"))]
use async_std::task;
use automerge::{transaction::Transactable, ObjId, ObjType, Patch, Prop, ScalarValue, Value};
use dashmap::DashMap;
use futures_lite::{AsyncRead, AsyncWrite, StreamExt};
use hypercore_protocol::hypercore::compact_encoding::{CompactEncoding, State};
use hypercore_protocol::Protocol;
use random_access_memory::RandomAccessMemory;
use random_access_storage::RandomAccess;
use std::{fmt::Debug, path::PathBuf};
use tracing::{debug, instrument};
#[cfg(target_arch = "wasm32")]
use wasm_bindgen_futures::spawn_local;

use crate::automerge::{
    apply_changes_autocommit, init_doc_from_entries, put_scalar_autocommit, splice_text,
};
use crate::common::PeerEvent;
use crate::hypercore::{discovery_key_from_public_key, on_protocol};
use crate::{
    automerge::{init_doc_with_root_scalars, put_object_autocommit},
    common::{
        entry::Entry,
        state::{DocContent, DocCursor},
        storage::DocStateWrapper,
    },
    hypercore::{
        create_new_read_memory_hypercore, create_new_write_memory_hypercore, generate_keys,
        keys_from_public_key, HypercoreWrapper,
    },
    StateEvent, SynchronizeEvent,
};

/// Hypermerge is the main abstraction.
#[derive(derivative::Derivative)]
#[derivative(Clone(bound = ""))]
pub struct Hypermerge<T>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send,
{
    hypercores: Arc<DashMap<[u8; 32], Arc<Mutex<HypercoreWrapper<T>>>>>,
    doc_state: Arc<Mutex<DocStateWrapper<T>>>,
    state_event_sender: Arc<Mutex<Option<Sender<StateEvent>>>>,
    prefix: PathBuf,
    peer_name: String,
    discovery_key: [u8; 32],
    doc_url: String,
}

impl<T> Hypermerge<T>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send + 'static,
{
    #[instrument(skip(self))]
    pub async fn watch(&mut self, ids: Vec<ObjId>) {
        let mut doc_state = self.doc_state.lock().await;
        doc_state.watch(ids);
    }

    #[instrument(skip(self, obj, prop), fields(obj = obj.as_ref().to_string()))]
    pub async fn get<O: AsRef<ObjId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
    ) -> anyhow::Result<Option<(Value, ObjId)>> {
        let doc_state = &self.doc_state;
        let result = {
            let doc_state = doc_state.lock().await;
            if let Some(doc) = doc_state.doc() {
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

    #[instrument(skip(self, obj), fields(obj = obj.as_ref().to_string()))]
    pub async fn realize_text<O: AsRef<ObjId>>(&self, obj: O) -> anyhow::Result<Option<String>> {
        let doc_state = &self.doc_state;
        let result = {
            let doc_state = doc_state.lock().await;
            if let Some(doc) = doc_state.doc() {
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

    #[instrument(skip(self, obj, prop), fields(obj = obj.as_ref().to_string()))]
    pub async fn put_object<O: AsRef<ObjId>, P: Into<Prop>>(
        &mut self,
        obj: O,
        prop: P,
        object: ObjType,
    ) -> anyhow::Result<ObjId> {
        let id = {
            let mut doc_state = self.doc_state.lock().await;
            let (entry, id) = if let Some(doc) = doc_state.doc_mut() {
                put_object_autocommit(doc, obj, prop, object).unwrap()
            } else {
                unimplemented!(
                    "TODO: No proper error code for trying to change before a document is synced"
                );
            };

            let write_discovery_key = doc_state.write_discovery_key();
            let length = {
                let write_hypercore = self.hypercores.get_mut(&write_discovery_key).unwrap();
                let mut write_hypercore = write_hypercore.lock().await;
                write_hypercore.append(&serialize_entry(&entry)).await?
            };
            doc_state.set_cursor(&write_discovery_key, length).await;
            id
        };
        {
            self.notify_of_document_changes().await;
        }
        Ok(id)
    }

    #[instrument(skip(self, obj, prop, value), fields(obj = obj.as_ref().to_string()))]
    pub async fn put_scalar<O: AsRef<ObjId>, P: Into<Prop>, V: Into<ScalarValue>>(
        &mut self,
        obj: O,
        prop: P,
        value: V,
    ) -> anyhow::Result<()> {
        {
            let mut doc_state = self.doc_state.lock().await;
            let entry = if let Some(doc) = doc_state.doc_mut() {
                put_scalar_autocommit(doc, obj, prop, value).unwrap()
            } else {
                unimplemented!(
                    "TODO: No proper error code for trying to change before a document is synced"
                );
            };

            let write_discovery_key = doc_state.write_discovery_key();
            let length = {
                let write_hypercore = self.hypercores.get_mut(&write_discovery_key).unwrap();
                let mut write_hypercore = write_hypercore.lock().await;
                write_hypercore.append(&serialize_entry(&entry)).await?
            };
            doc_state.set_cursor(&write_discovery_key, length).await;
        };
        {
            self.notify_of_document_changes().await;
        }
        Ok(())
    }

    #[instrument(skip(self, obj), fields(obj = obj.as_ref().to_string()))]
    pub async fn splice_text<O: AsRef<ObjId>>(
        &mut self,
        obj: O,
        index: usize,
        delete: usize,
        text: &str,
    ) -> anyhow::Result<()> {
        {
            let mut doc_state = self.doc_state.lock().await;
            let entry = if let Some(doc) = doc_state.doc_mut() {
                splice_text(doc, obj, index, delete, text)?
            } else {
                unimplemented!(
                "TODO: No proper error code for trying to splice text before a document is synced"
            );
            };
            let write_discovery_key = doc_state.write_discovery_key();
            let length = {
                let write_hypercore_wrapper =
                    self.hypercores.get_mut(&write_discovery_key).unwrap();
                let mut write_hypercore = write_hypercore_wrapper.lock().await;
                write_hypercore.append(&serialize_entry(&entry)).await?
            };
            doc_state.set_cursor(&write_discovery_key, length).await;
        }
        {
            self.notify_of_document_changes().await;
        }
        Ok(())
    }

    #[instrument(skip(self))]
    pub async fn cork(&mut self) {
        let doc_state = self.doc_state.lock().await;
        let write_discovery_key = doc_state.write_discovery_key();
        let write_hypercore_wrapper = self.hypercores.get_mut(&write_discovery_key).unwrap();
        let mut write_hypercore = write_hypercore_wrapper.lock().await;
        write_hypercore.cork();
    }

    #[instrument(skip(self))]
    pub async fn uncork(&mut self) -> anyhow::Result<()> {
        let doc_state = self.doc_state.lock().await;
        let write_discovery_key = doc_state.write_discovery_key();
        let write_hypercore_wrapper = self.hypercores.get_mut(&write_discovery_key).unwrap();
        let mut write_hypercore = write_hypercore_wrapper.lock().await;
        write_hypercore.uncork().await?;
        Ok(())
    }

    #[instrument(skip(self))]
    pub async fn connect_document(
        &mut self,
        state_event_sender: Sender<StateEvent>,
        sync_event_receiver: &mut Receiver<SynchronizeEvent>,
    ) -> anyhow::Result<()> {
        // First let's drain any patches that are not yet sent out, and push them out
        {
            *self.state_event_sender.lock().await = Some(state_event_sender.clone());
        }
        {
            self.notify_of_document_changes().await;
        }
        // Then start listening for any sync events
        debug!("Start listening for SynchronizeEvents");
        while let Some(event) = sync_event_receiver.next().await {
            match event {
                SynchronizeEvent::DocumentChanged(patches) => {
                    state_event_sender
                        .send(StateEvent::DocumentChanged(patches))
                        .await
                        .unwrap();
                }
                SynchronizeEvent::NewPeersBroadcasted(_) => {
                    // TODO: ignore for now
                }
                SynchronizeEvent::PeersSynced(len) => {
                    state_event_sender
                        .send(StateEvent::PeersSynced(len))
                        .await
                        .unwrap();
                }
                SynchronizeEvent::RemotePeerSynced() => {
                    state_event_sender
                        .send(StateEvent::RemotePeerSynced())
                        .await
                        .unwrap();
                }
            }
        }
        Ok(())
    }

    #[instrument(skip(self))]
    pub fn doc_url(&self) -> String {
        self.doc_url.clone()
    }

    async fn notify_of_document_changes(&mut self) {
        let mut doc_state = self.doc_state.lock().await;
        if let Some(doc) = doc_state.doc_mut() {
            let mut state_event_sender = self.state_event_sender.lock().await;
            if let Some(sender) = state_event_sender.as_mut() {
                if sender.is_closed() {
                    *state_event_sender = None;
                } else {
                    let patches = doc.observer().take_patches();
                    if patches.len() > 0 {
                        sender
                            .send(StateEvent::DocumentChanged(patches))
                            .await
                            .unwrap();
                    }
                }
            }
        }
    }
}

impl Hypermerge<RandomAccessMemory> {
    pub async fn create_doc_memory<P: Into<Prop>, V: Into<ScalarValue>>(
        peer_name: &str,
        root_scalars: Vec<(P, V)>,
    ) -> Self {
        // Generate a key pair, its discovery key and the public key string
        let (key_pair, encoded_doc_public_key, discovery_key) = generate_keys();
        let (doc, data) = init_doc_with_root_scalars(peer_name, &discovery_key, root_scalars);
        let doc_public_key = *key_pair.public.as_bytes();

        // Create the memory hypercore
        let (length, hypercore) = create_new_write_memory_hypercore(
            key_pair,
            serialize_entry(&Entry::new_init_doc(data.clone())),
        )
        .await;
        let content = DocContent::new(
            data,
            vec![DocCursor::new(discovery_key.clone(), length)],
            doc,
        );
        let doc_url = to_doc_url(&encoded_doc_public_key);

        Self::new_memory(
            (doc_public_key.clone(), discovery_key.clone(), hypercore),
            vec![],
            Some(content),
            discovery_key,
            peer_name,
            doc_public_key,
            &doc_url,
        )
        .await
    }

    pub async fn register_doc_memory(peer_name: &str, doc_url: &str) -> Self {
        // Process keys from doc URL
        let doc_public_key = to_public_key(doc_url);
        let (doc_public_key, doc_discovery_key) = keys_from_public_key(&doc_public_key);

        // Create the doc hypercore
        let (_, doc_hypercore) = create_new_read_memory_hypercore(&doc_public_key).await;

        // Create the write hypercore
        let (write_key_pair, _, write_discovery_key) = generate_keys();
        let write_public_key = *write_key_pair.public.as_bytes();
        let (_, write_hypercore) = create_new_write_memory_hypercore(
            write_key_pair,
            serialize_entry(&Entry::new_init_peer(doc_discovery_key)),
        )
        .await;

        Self::new_memory(
            (write_public_key, write_discovery_key, write_hypercore),
            vec![(
                doc_public_key.clone(),
                doc_discovery_key.clone(),
                doc_hypercore,
            )],
            None,
            doc_discovery_key,
            peer_name,
            doc_public_key,
            doc_url,
        )
        .await
    }

    #[instrument(skip_all, fields(peer_name = self.peer_name))]
    pub async fn connect_protocol_memory<IO>(
        &mut self,
        protocol: &mut Protocol<IO>,
        sync_event_sender: &mut Sender<SynchronizeEvent>,
    ) -> anyhow::Result<()>
    where
        IO: AsyncWrite + AsyncRead + Send + Unpin + 'static,
    {
        let (mut peer_event_sender, peer_event_receiver): (Sender<PeerEvent>, Receiver<PeerEvent>) =
            unbounded();

        let sync_event_sender_for_task = sync_event_sender.clone();
        let doc_state = self.doc_state.clone();
        let discovery_key_for_task = self.discovery_key.clone();
        let hypercores_for_task = self.hypercores.clone();
        let peer_name_for_task = self.peer_name.clone();
        let task_span = tracing::debug_span!("call_on_peer_event_memory").or_current();
        #[cfg(not(target_arch = "wasm32"))]
        task::spawn(async move {
            let _entered = task_span.enter();
            on_peer_event_memory(
                &discovery_key_for_task,
                peer_event_receiver,
                sync_event_sender_for_task,
                doc_state,
                hypercores_for_task,
                &peer_name_for_task,
            )
            .await;
        });
        #[cfg(target_arch = "wasm32")]
        spawn_local(async move {
            let _entered = task_span.enter();
            on_peer_event_memory(
                &discovery_key_for_task,
                peer_event_receiver,
                sync_event_sender_for_task,
                doc_state,
                hypercores_for_task,
                &peer_name_for_task,
            )
            .await;
        });

        on_protocol(
            protocol,
            self.doc_state.clone(),
            self.hypercores.clone(),
            &mut peer_event_sender,
        )
        .await?;
        Ok(())
    }

    async fn new_memory(
        write_hypercore: ([u8; 32], [u8; 32], HypercoreWrapper<RandomAccessMemory>),
        peer_hypercores: Vec<([u8; 32], [u8; 32], HypercoreWrapper<RandomAccessMemory>)>,
        content: Option<DocContent>,
        discovery_key: [u8; 32],
        peer_name: &str,
        doc_public_key: [u8; 32],
        doc_url: &str,
    ) -> Self {
        let hypercores: DashMap<[u8; 32], Arc<Mutex<HypercoreWrapper<RandomAccessMemory>>>> =
            DashMap::new();
        let (write_public_key, write_discovery_key, write_hypercore) = write_hypercore;
        hypercores.insert(write_discovery_key, Arc::new(Mutex::new(write_hypercore)));
        let mut peer_public_keys = vec![];
        for (peer_public_key, peer_discovery_key, peer_hypercore) in peer_hypercores {
            peer_public_keys.push(peer_public_key);
            hypercores.insert(peer_discovery_key, Arc::new(Mutex::new(peer_hypercore)));
        }
        let doc_state = DocStateWrapper::new_memory(
            doc_public_key,
            Some(write_public_key),
            peer_public_keys,
            content,
        )
        .await;
        Self {
            hypercores: Arc::new(hypercores),
            doc_state: Arc::new(Mutex::new(doc_state)),
            state_event_sender: Arc::new(Mutex::new(None)),
            prefix: PathBuf::new(),
            discovery_key,
            doc_url: doc_url.to_string(),
            peer_name: peer_name.to_string(),
        }
    }
}

#[instrument(level = "debug", skip_all)]
async fn on_peer_event_memory(
    doc_discovery_key: &[u8; 32],
    mut peer_event_receiver: Receiver<PeerEvent>,
    sync_event_sender: Sender<SynchronizeEvent>,
    doc_state: Arc<Mutex<DocStateWrapper<RandomAccessMemory>>>,
    hypercores: Arc<DashMap<[u8; 32], Arc<Mutex<HypercoreWrapper<RandomAccessMemory>>>>>,
    peer_name: &str,
) {
    while let Some(event) = peer_event_receiver.next().await {
        debug!("Received event {:?}", event);
        match event {
            PeerEvent::NewPeersBroadcasted(public_keys) => {
                let len = public_keys.len();
                let added = {
                    // Save new keys to state
                    let mut doc_state = doc_state.lock().await;
                    doc_state
                        .add_peer_public_keys_to_state(public_keys.clone())
                        .await
                };
                if added {
                    {
                        // Create and insert all new hypercores
                        create_and_insert_read_memory_hypercores(public_keys, hypercores.clone())
                            .await;
                    }
                    {
                        // Send message to all hypercores that new peers have been created to trigger
                        // close/reopen
                        for hypercore in hypercores.iter() {
                            let mut hypercore = hypercore.lock().await;
                            hypercore.notify_new_peers_created(len).await.unwrap();
                        }
                    }
                    sync_event_sender
                        .send(SynchronizeEvent::NewPeersBroadcasted(len))
                        .await
                        .unwrap();
                }
            }
            PeerEvent::PeerDisconnected(_) => {
                // This is an FYI message, just continue for now
            }
            PeerEvent::RemotePeerSynced(_) => {
                sync_event_sender
                    .send(SynchronizeEvent::RemotePeerSynced())
                    .await
                    .unwrap();
            }
            PeerEvent::PeerSyncStarted(public_key) => {
                // Set peer to not-synced
                let mut doc_state = doc_state.lock().await;
                doc_state.set_synced_to_state(public_key, false).await;
            }
            PeerEvent::PeerSynced((public_key, synced_contiguous_length)) => {
                let sync_info = {
                    // Set peer to synced
                    let mut doc_state = doc_state.lock().await;
                    let sync_status_changed = doc_state.set_synced_to_state(public_key, true).await;
                    if sync_status_changed {
                        // Find out if now all peers are synced
                        let peers_synced = doc_state.peers_synced();
                        if let Some(peers_synced) = peers_synced {
                            // All peers are synced, so it should be possible to create a coherent
                            // document now.
                            let mut patches = if let Some(content) = doc_state.content_mut() {
                                let patches =
                                    update_content(content, hypercores.clone()).await.unwrap();
                                doc_state.persist_content().await;
                                patches
                            } else {
                                let write_discovery_key = doc_state.write_discovery_key();
                                let (content, patches) = create_content(
                                    doc_discovery_key,
                                    peer_name,
                                    &write_discovery_key,
                                    hypercores.clone(),
                                )
                                .await
                                .unwrap();
                                doc_state.set_content(content).await;
                                patches
                            };

                            // Filter out unwatched patches
                            let watched_ids = &doc_state.state().watched_ids;
                            patches.retain(|patch| match patch {
                                Patch::Put { obj, .. } => watched_ids.contains(obj),
                                Patch::Insert { obj, .. } => watched_ids.contains(obj),
                                Patch::Delete { obj, .. } => watched_ids.contains(obj),
                                Patch::Increment { obj, .. } => watched_ids.contains(obj),
                                Patch::Expose { obj, .. } => watched_ids.contains(obj),
                                Patch::Splice { obj, .. } => watched_ids.contains(obj),
                            });
                            Some((peers_synced, patches))
                        } else {
                            None
                        }
                    } else {
                        // If nothing changed, don't reannounce
                        None
                    }
                };

                if let Some((peers_synced, patches)) = sync_info {
                    sync_event_sender
                        .send(SynchronizeEvent::PeersSynced(peers_synced))
                        .await
                        .unwrap();
                    if patches.len() > 0 {
                        sync_event_sender
                            .send(SynchronizeEvent::DocumentChanged(patches))
                            .await
                            .unwrap();
                    }
                }

                // Finally, notify about the new sync so that other protocols can get synced as
                // well. The message reaches the same hypercore that sent this, but is disregarded.
                let discovery_key = discovery_key_from_public_key(&public_key);
                {
                    let hypercore = hypercores.get_mut(&discovery_key).unwrap();
                    let mut hypercore = hypercore.lock().await;
                    hypercore
                        .notify_peer_synced(synced_contiguous_length)
                        .await
                        .unwrap();
                }
            }
        }
    }
    debug!("Exiting");
}

async fn create_and_insert_read_memory_hypercores(
    public_keys: Vec<[u8; 32]>,
    hypercores: Arc<DashMap<[u8; 32], Arc<Mutex<HypercoreWrapper<RandomAccessMemory>>>>>,
) {
    for public_key in public_keys {
        let discovery_key = discovery_key_from_public_key(&public_key);
        let (_, hypercore) = create_new_read_memory_hypercore(&public_key).await;
        hypercores.insert(discovery_key, Arc::new(Mutex::new(hypercore)));
    }
}

async fn create_content<T>(
    doc_discovery_key: &[u8; 32],
    write_peer_name: &str,
    write_discovery_key: &[u8; 32],
    hypercores: Arc<DashMap<[u8; 32], Arc<Mutex<HypercoreWrapper<T>>>>>,
) -> anyhow::Result<(DocContent, Vec<Patch>)>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send + 'static,
{
    // The document starts from the origin, so get that first
    let (mut cursors, mut entries) = {
        let origin_hypercore = hypercores.get(doc_discovery_key).unwrap();
        let mut origin_hypercore = origin_hypercore.lock().await;
        let entries = origin_hypercore.entries(0).await?;
        (
            vec![DocCursor::new(
                doc_discovery_key.clone(),
                entries.len() as u64,
            )],
            entries,
        )
    };
    for kv in hypercores.iter() {
        let discovery_key = kv.key();
        let hypercore = kv.value();
        if discovery_key != doc_discovery_key {
            let mut hypercore = hypercore.lock().await;
            let new_entries = hypercore.entries(0).await?;
            cursors.push(DocCursor::new(
                doc_discovery_key.clone(),
                new_entries.len() as u64,
            ));
            entries.extend(new_entries);
        }
    }

    // Create DocContent from the hypercore
    let (mut doc, data) = init_doc_from_entries(write_peer_name, write_discovery_key, entries);
    let patches = doc.observer().take_patches();
    Ok((DocContent::new(data, cursors, doc), patches))
}

async fn update_content<T>(
    content: &mut DocContent,
    hypercores: Arc<DashMap<[u8; 32], Arc<Mutex<HypercoreWrapper<T>>>>>,
) -> anyhow::Result<Vec<Patch>>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send + 'static,
{
    let entries = {
        let mut entries = vec![];
        // TODO: This is slightly inefficient because we now go through all peers even though
        // it should be possible for the caller to know what changed.
        for kv in hypercores.iter() {
            let discovery_key = kv.key();
            let hypercore_wrapper = kv.value();
            let mut hypercore = hypercore_wrapper.lock().await;
            let old_length = content.cursor_length(discovery_key);
            let new_entries = hypercore.entries(old_length).await?;
            if !new_entries.is_empty() {
                content.set_cursor(&discovery_key, new_entries.len() as u64);
                entries.extend(new_entries);
            }
        }
        entries
    };
    {
        let doc = content.doc.as_mut().unwrap();
        apply_changes_autocommit(doc, entries)?;
        Ok(doc.observer().take_patches())
    }
}

fn serialize_entry(entry: &Entry) -> Vec<u8> {
    let mut enc_state = State::new();
    enc_state.preencode(entry);
    let mut buffer = enc_state.create_buffer();
    enc_state.encode(entry, &mut buffer);
    buffer.to_vec()
}

fn to_doc_url(public_key: &str) -> String {
    format!("hypermerge:/{}", public_key)
}

fn to_public_key(doc_url: &str) -> String {
    doc_url[12..].to_string()
}
