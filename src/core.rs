use async_std::sync::{Arc, Mutex};
#[cfg(not(target_arch = "wasm32"))]
use async_std::task;
use automerge::Change;
use automerge::{transaction::Transactable, ObjId, ObjType, Patch, Prop, ScalarValue, Value};
use dashmap::DashMap;
use futures::channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use futures::{AsyncRead, AsyncWrite, StreamExt};
use hypercore_protocol::hypercore::compact_encoding::{CompactEncoding, State};
use hypercore_protocol::Protocol;
use random_access_memory::RandomAccessMemory;
use random_access_storage::RandomAccess;
use std::collections::HashMap;
use std::{fmt::Debug, path::PathBuf};
use tracing::{debug, instrument, warn};
#[cfg(target_arch = "wasm32")]
use wasm_bindgen_futures::spawn_local;

use crate::automerge::{
    apply_entries_autocommit, init_doc_from_entries, put_scalar_autocommit, splice_text,
    UnappliedEntries,
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
    StateEvent,
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
    state_event_sender: Arc<Mutex<Option<UnboundedSender<StateEvent>>>>,
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

    #[instrument(skip(self, obj, prop), fields(obj = obj.as_ref().to_string(), peer_name = self.peer_name))]
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

    #[instrument(skip(self, obj), fields(obj = obj.as_ref().to_string(), peer_name = self.peer_name))]
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

    #[instrument(skip(self, obj, prop), fields(obj = obj.as_ref().to_string(), peer_name = self.peer_name))]
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

    #[instrument(skip(self, obj, prop, value), fields(obj = obj.as_ref().to_string(), peer_name = self.peer_name))]
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

    #[instrument(skip(self, obj), fields(obj = obj.as_ref().to_string(), peer_name = self.peer_name))]
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
                            .unbounded_send(StateEvent::DocumentChanged(patches))
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
            serialize_entry(&Entry::new_init_doc(peer_name, data.clone())),
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
            serialize_entry(&Entry::new_init_peer(peer_name, doc_discovery_key)),
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
        state_event_sender: &mut UnboundedSender<StateEvent>,
    ) -> anyhow::Result<()>
    where
        IO: AsyncWrite + AsyncRead + Send + Unpin + 'static,
    {
        // First let's drain any patches that are not yet sent out, and push them out. These can
        // be created
        {
            *self.state_event_sender.lock().await = Some(state_event_sender.clone());
        }
        {
            self.notify_of_document_changes().await;
        }
        let (mut peer_event_sender, peer_event_receiver): (
            UnboundedSender<PeerEvent>,
            UnboundedReceiver<PeerEvent>,
        ) = unbounded();

        let state_event_sender_for_task = state_event_sender.clone();
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
                state_event_sender_for_task,
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
                state_event_sender_for_task,
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
            hypercores.insert(peer_discovery_key, Arc::new(Mutex::new(peer_hypercore)));
            peer_public_keys.push(peer_public_key);
        }
        let mut doc_state =
            DocStateWrapper::new_memory(doc_public_key, Some(write_public_key), content).await;
        doc_state
            .add_peer_public_keys_to_state(peer_public_keys)
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
    mut peer_event_receiver: UnboundedReceiver<PeerEvent>,
    state_event_sender: UnboundedSender<StateEvent>,
    doc_state: Arc<Mutex<DocStateWrapper<RandomAccessMemory>>>,
    hypercores: Arc<DashMap<[u8; 32], Arc<Mutex<HypercoreWrapper<RandomAccessMemory>>>>>,
    peer_name: &str,
) {
    while let Some(event) = peer_event_receiver.next().await {
        debug!("Received event {:?}", event);
        match event {
            PeerEvent::NewPeersBroadcasted(public_keys) => {
                let changed = {
                    let mut doc_state = doc_state.lock().await;
                    doc_state
                        .add_peer_public_keys_to_state(public_keys.clone())
                        .await
                };
                if changed {
                    {
                        // Create and insert all new hypercores
                        create_and_insert_read_memory_hypercores(
                            public_keys.clone(),
                            hypercores.clone(),
                        )
                        .await;
                    }
                    {
                        // Send message to doc hypercore that new peers have been created to get all open protocols to
                        // open channels to it.
                        let doc_hypercore = hypercores.get(doc_discovery_key).unwrap();
                        let mut doc_hypercore = doc_hypercore.lock().await;
                        doc_hypercore
                            .notify_new_peers_created(public_keys.clone())
                            .await
                            .unwrap();
                    }
                }
            }
            PeerEvent::PeerDisconnected(_) => {
                // This is an FYI message, just continue for now
            }
            PeerEvent::RemotePeerSynced((discovery_key, synced_contiguous_length)) => {
                match state_event_sender.unbounded_send(StateEvent::RemotePeerSynced((
                    discovery_key,
                    synced_contiguous_length,
                ))) {
                    Ok(()) => {}
                    Err(err) => warn!(
                        "{}: could not notify remote peer synced to len {}, err {}",
                        peer_name.to_string(),
                        synced_contiguous_length,
                        err
                    ),
                }
            }
            PeerEvent::PeerSynced((discovery_key, synced_contiguous_length)) => {
                let (peer_sync_events, patches): (Vec<StateEvent>, Vec<Patch>) = {
                    // Sync doc state exclusively...
                    let mut doc_state = doc_state.lock().await;
                    let (mut patches, peer_syncs) = if let Some((content, unapplied_entries)) =
                        doc_state.content_and_unapplied_entries_mut()
                    {
                        let (patches, new_peer_names, peer_syncs) = update_content(
                            &discovery_key,
                            synced_contiguous_length,
                            content,
                            hypercores.clone(),
                            unapplied_entries,
                        )
                        .await
                        .unwrap();
                        doc_state
                            .persist_content_and_new_peer_names(new_peer_names)
                            .await;
                        (patches, peer_syncs)
                    } else {
                        let write_discovery_key = doc_state.write_discovery_key();
                        let unapplied_entries = doc_state.unappliend_entries_mut();
                        if let Some((content, patches, new_peer_names, peer_syncs)) =
                            create_content(
                                &discovery_key,
                                synced_contiguous_length,
                                doc_discovery_key,
                                peer_name,
                                &write_discovery_key,
                                hypercores.clone(),
                                unapplied_entries,
                            )
                            .await
                            .unwrap()
                        {
                            doc_state
                                .set_content_and_new_peer_names(content, new_peer_names)
                                .await;
                            (patches, peer_syncs)
                        } else {
                            // Could not create content from this peer's data, needs more peers
                            (vec![], vec![])
                        }
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

                    let peer_synced_events: Vec<StateEvent> = peer_syncs
                        .iter()
                        .map(|sync| {
                            let name = doc_state.peer_name(&sync.0).unwrap();
                            StateEvent::PeerSynced((name, sync.0.clone(), sync.1))
                        })
                        .collect();
                    (peer_synced_events, patches)
                    // ..doc state sync ready, release lock
                };

                for peer_sync_event in peer_sync_events {
                    state_event_sender.unbounded_send(peer_sync_event).unwrap();
                }
                if patches.len() > 0 {
                    state_event_sender
                        .unbounded_send(StateEvent::DocumentChanged(patches))
                        .unwrap();
                }

                // Finally, notify about the new sync so that other protocols can get synced as
                // well. The message reaches the same hypercore that sent this, but is disregarded.
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
        // Make sure to insert only once even if two protocols notice the same new
        // hypercore at the same timeusing the entry API.
        let entry = hypercores.entry(discovery_key);
        match entry {
            dashmap::mapref::entry::Entry::Occupied(_) => {
                debug!("Concurrent creating of hypercores noticed, continuing.");
            }
            dashmap::mapref::entry::Entry::Vacant(vacant) => {
                let (_, hypercore) = create_new_read_memory_hypercore(&public_key).await;
                vacant.insert(Arc::new(Mutex::new(hypercore)));
            }
        }
    }
}

async fn create_content<T>(
    synced_discovery_key: &[u8; 32],
    synced_contiguous_length: u64,
    doc_discovery_key: &[u8; 32],
    write_peer_name: &str,
    write_discovery_key: &[u8; 32],
    hypercores: Arc<DashMap<[u8; 32], Arc<Mutex<HypercoreWrapper<T>>>>>,
    unapplied_entries: &mut UnappliedEntries,
) -> anyhow::Result<
    Option<(
        DocContent,
        Vec<Patch>,
        Vec<([u8; 32], String)>,
        Vec<([u8; 32], u64)>,
    )>,
>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send + 'static,
{
    // The document starts from the doc hypercore, so get that first
    if synced_discovery_key == doc_discovery_key {
        let entries = {
            let doc_hypercore = hypercores.get(doc_discovery_key).unwrap();
            let mut doc_hypercore = doc_hypercore.lock().await;
            doc_hypercore.entries(0, synced_contiguous_length).await?
        };

        // Create DocContent from the hypercore
        let (mut doc, data, result) = init_doc_from_entries(
            write_peer_name,
            write_discovery_key,
            synced_discovery_key,
            entries,
            unapplied_entries,
        )?;
        let cursors: Vec<DocCursor> = result
            .iter()
            .map(|value| DocCursor::new(value.0.clone(), value.1 .0))
            .collect();
        let patches = if !cursors.is_empty() {
            doc.observer().take_patches()
        } else {
            vec![]
        };
        let new_names: Vec<([u8; 32], String)> = result
            .iter()
            .filter(|value| value.1 .1.is_some())
            .map(|value| (value.0.clone(), value.1 .1.clone().unwrap()))
            .collect();
        let peer_syncs: Vec<([u8; 32], u64)> = result
            .iter()
            .map(|value| (value.0.clone(), value.1 .0))
            .collect();
        Ok(Some((
            DocContent::new(data, cursors, doc),
            patches,
            new_names,
            peer_syncs,
        )))
    } else {
        // Got first some other peer's data, need to store it to unapplied changes
        let hypercore = hypercores.get(synced_discovery_key).unwrap();
        let mut hypercore = hypercore.lock().await;
        let start_index = unapplied_entries.current_length(synced_discovery_key);
        let entries = hypercore
            .entries(start_index, synced_contiguous_length - start_index)
            .await?;
        let mut index = start_index;
        for entry in entries {
            unapplied_entries.add(synced_discovery_key, index, entry);
            index += 1;
        }
        Ok(None)
    }
}

async fn update_content<T>(
    synced_discovery_key: &[u8; 32],
    synced_contiguous_length: u64,
    content: &mut DocContent,
    hypercores: Arc<DashMap<[u8; 32], Arc<Mutex<HypercoreWrapper<T>>>>>,
    unapplied_entries: &mut UnappliedEntries,
) -> anyhow::Result<(Vec<Patch>, Vec<([u8; 32], String)>, Vec<([u8; 32], u64)>)>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send + 'static,
{
    let entries = {
        let hypercore = hypercores.get(synced_discovery_key).unwrap();
        let mut hypercore = hypercore.lock().await;
        hypercore
            .entries(
                content.cursor_length(synced_discovery_key),
                synced_contiguous_length,
            )
            .await?
    };

    let (patches, new_names, peer_syncs) = {
        let doc = content.doc.as_mut().unwrap();
        let result = apply_entries_autocommit(
            doc,
            synced_discovery_key,
            synced_contiguous_length,
            entries,
            unapplied_entries,
        )?;
        let new_names: Vec<([u8; 32], String)> = result
            .iter()
            .filter(|value| value.1 .1.is_some())
            .map(|value| (value.0.clone(), value.1 .1.clone().unwrap()))
            .collect();
        let peer_syncs: Vec<([u8; 32], u64)> = result
            .iter()
            .map(|value| (value.0.clone(), value.1 .0))
            .collect();
        let patches = if !peer_syncs.is_empty() {
            doc.observer().take_patches()
        } else {
            vec![]
        };
        (patches, new_names, peer_syncs)
    };

    for (discovery_key, length) in &peer_syncs[..] {
        content.set_cursor(&discovery_key, *length);
    }

    Ok((patches, new_names, peer_syncs))
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
