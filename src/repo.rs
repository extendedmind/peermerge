use async_channel::{unbounded, Receiver, Sender};
use async_std::sync::{Arc, Mutex};
#[cfg(not(target_arch = "wasm32"))]
use async_std::task;
use automerge::{ObjId, ObjType, Patch, Prop, ScalarValue, Value};
use futures_lite::{AsyncRead, AsyncWrite, StreamExt};
use hypercore_protocol::Protocol;
#[cfg(not(target_arch = "wasm32"))]
use random_access_disk::RandomAccessDisk;
use random_access_memory::RandomAccessMemory;
use random_access_storage::RandomAccess;
#[cfg(not(target_arch = "wasm32"))]
use std::path::PathBuf;
use std::{collections::HashMap, fmt::Debug};
#[cfg(target_arch = "wasm32")]
use wasm_bindgen_futures::spawn_local;

use crate::{
    automerge::init_doc_with_root_scalars,
    common::{storage::DocStateWrapper, PeerEvent, SynchronizeEvent},
    hypercore::{generate_keys, on_protocol, HypercoreWrapper},
    store::{create_and_insert_read_memory_hypercores, create_content, update_content, DocStore},
    StateEvent,
};

/// Repo is the main abstraction of hypermerge
#[derive(derivative::Derivative)]
#[derivative(Clone(bound = ""))]
pub struct Repo<T>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send,
{
    store: Arc<Mutex<DocStore<T>>>,
    state: Arc<Mutex<T>>,
}

impl<T> Repo<T>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send + 'static,
{
    pub async fn connect_document(
        &mut self,
        discovery_key: [u8; 32],
        state_event_sender: Sender<StateEvent>,
        sync_event_receiver: &mut Receiver<SynchronizeEvent>,
    ) -> anyhow::Result<()> {
        println!("connect_document: start listening");
        while let Some(event) = sync_event_receiver.next().await {
            match event {
                SynchronizeEvent::DocumentChanged(patches) => {
                    state_event_sender
                        .send(StateEvent::DocumentChanged(patches))
                        .await
                        .unwrap();
                }
                SynchronizeEvent::NewPeersAdvertised(_) => {
                    // TODO: ignore for now
                }
                SynchronizeEvent::PeersSynced(len) => {
                    state_event_sender
                        .send(StateEvent::PeersSynced(len))
                        .await
                        .unwrap();
                }
            }
            // let store = self.store.lock().await;

            // TODO: Do something here with sync event to trigger state event
        }
        Ok(())
    }

    pub async fn put_object<O: AsRef<ObjId>, P: Into<Prop>>(
        &mut self,
        discovery_key: &[u8; 32],
        obj: O,
        prop: P,
        object: ObjType,
    ) -> anyhow::Result<ObjId> {
        self.store
            .lock()
            .await
            .put_object(discovery_key, obj, prop, object)
            .await
    }

    pub async fn get<O: AsRef<ObjId>, P: Into<Prop>>(
        &self,
        discovery_key: &[u8; 32],
        obj: O,
        prop: P,
    ) -> anyhow::Result<Option<(Value, ObjId)>> {
        let store = self.store.lock().await;
        if let Some((value, id)) = store.get(discovery_key, obj, prop).await? {
            Ok(Some((value.to_owned(), id.to_owned())))
        } else {
            Ok(None)
        }
    }

    pub async fn watch(&mut self, discovery_key: &[u8; 32], ids: Vec<&ObjId>) {
        let ids = ids.iter().map(|id| id.clone().to_owned()).collect();
        self.store.lock().await.watch(discovery_key, ids).await;
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl Repo<RandomAccessDisk> {
    pub async fn new_disk(data_root_dir: &PathBuf) -> Self {
        let store: DocStore<RandomAccessDisk> = DocStore::new_disk(data_root_dir).await;
        let state_path = data_root_dir.join(PathBuf::from("state.bin"));
        let state = RandomAccessDisk::builder(state_path).build().await.unwrap();
        Self {
            store: Arc::new(Mutex::new(store)),
            state: Arc::new(Mutex::new(state)),
        }
    }

    pub async fn create_document_disk<P: Into<Prop>, V: Into<ScalarValue>>(
        &mut self,
        root_scalars: Vec<(P, V)>,
    ) -> ([u8; 32], String) {
        // Generate a key pair, its discovery key and the public key string
        let (key_pair, encoded_public_key, discovery_key) = generate_keys();
        let (doc, data) = init_doc_with_root_scalars(&discovery_key, root_scalars);
        self.store
            .lock()
            .await
            .add_doc_disk(key_pair, encoded_public_key, discovery_key, doc, data)
            .await
    }

    pub async fn register_doc_disk(&mut self, doc_url: &str) -> [u8; 32] {
        self.store.lock().await.register_doc_disk(doc_url).await
    }

    pub async fn connect_protocol<IO>(
        &mut self,
        discovery_key: &[u8; 32],
        protocol: &mut Protocol<IO>,
        sync_event_sender: &mut Sender<SynchronizeEvent>,
    ) -> anyhow::Result<()>
    where
        IO: AsyncWrite + AsyncRead + Send + Unpin + 'static,
    {
        // TODO: The disk versio of below
        Ok(())
    }
}

impl Repo<RandomAccessMemory> {
    pub async fn new_memory() -> Self {
        let store: DocStore<RandomAccessMemory> = DocStore::new_memory().await;
        let state = RandomAccessMemory::default();
        Self {
            store: Arc::new(Mutex::new(store)),
            state: Arc::new(Mutex::new(state)),
        }
    }

    pub async fn create_doc_memory<P: Into<Prop>, V: Into<ScalarValue>>(
        &mut self,
        root_scalars: Vec<(P, V)>,
    ) -> ([u8; 32], String) {
        // Generate a key pair, its discovery key and the public key string
        let (key_pair, encoded_public_key, discovery_key) = generate_keys();
        let (doc, data) = init_doc_with_root_scalars(&discovery_key, root_scalars);
        self.store
            .lock()
            .await
            .add_doc_memory(key_pair, encoded_public_key, discovery_key, doc, data)
            .await
    }

    pub async fn register_doc_memory(&mut self, doc_url: &str) -> [u8; 32] {
        self.store.lock().await.register_doc_memory(doc_url).await
    }

    pub async fn connect_protocol<IO>(
        &mut self,
        discovery_key: &[u8; 32],
        protocol: &mut Protocol<IO>,
        sync_event_sender: &mut Sender<SynchronizeEvent>,
    ) -> anyhow::Result<()>
    where
        IO: AsyncWrite + AsyncRead + Send + Unpin + 'static,
    {
        if let Some(hypercore_store) = self.store.lock().await.hypercore_store_mut(discovery_key) {
            let (mut peer_event_sender, peer_event_receiver): (
                Sender<PeerEvent>,
                Receiver<PeerEvent>,
            ) = unbounded();

            let sync_event_sender_for_task = sync_event_sender.clone();
            let doc_state = hypercore_store.doc_state();
            let hypercores = hypercore_store.hypercores();
            let is_initiator = protocol.is_initiator();
            let discovery_key_for_task = discovery_key.clone();
            #[cfg(not(target_arch = "wasm32"))]
            task::spawn(async move {
                on_peer_event_memory(
                    &discovery_key_for_task,
                    peer_event_receiver,
                    sync_event_sender_for_task,
                    doc_state,
                    hypercores,
                    is_initiator,
                )
                .await;
            });
            #[cfg(target_arch = "wasm32")]
            spawn_local(async move {
                on_peer_event_memory(
                    &discovery_key_for_task,
                    peer_event_receiver,
                    sync_event_sender_for_task,
                    doc_state,
                    hypercores,
                    is_initiator,
                )
                .await;
            });

            on_protocol(
                protocol,
                hypercore_store,
                &mut peer_event_sender,
                sync_event_sender,
                is_initiator,
            )
            .await?
        }
        Ok(())
    }
}

async fn on_peer_event_memory(
    discovery_key: &[u8; 32],
    mut peer_event_receiver: Receiver<PeerEvent>,
    sync_event_sender: Sender<SynchronizeEvent>,
    doc_state: Arc<Mutex<DocStateWrapper<RandomAccessMemory>>>,
    hypercores: Arc<Mutex<HashMap<[u8; 32], Arc<Mutex<HypercoreWrapper<RandomAccessMemory>>>>>>,
    is_initiator: bool,
) {
    while let Some(event) = peer_event_receiver.next().await {
        match event {
            PeerEvent::NewPeersAdvertised(public_keys) => {
                let len = public_keys.len();
                {
                    // Save new keys to state
                    let mut doc_state = doc_state.lock().await;
                    doc_state
                        .add_peer_public_keys_to_state(public_keys.clone())
                        .await;
                }
                {
                    // Create and insert all new hypercores
                    create_and_insert_read_memory_hypercores(public_keys, hypercores.clone()).await;
                }

                sync_event_sender
                    .send(SynchronizeEvent::NewPeersAdvertised(len))
                    .await
                    .unwrap();
            }
            PeerEvent::PeerDisconnected(_) => {
                // This is an FYI message, just continue for now
            }
            PeerEvent::RemotePeerSynced(_) => {
                // This is an FYI message, just continue for now
            }
            PeerEvent::PeerSyncStarted(public_key) => {
                // Set peer to not-synced
                let mut doc_state = doc_state.lock().await;
                doc_state.set_synced_to_state(public_key, false).await;
            }
            PeerEvent::PeerSynced(public_key) => {
                let peers_synced = {
                    // Set peer to synced
                    let mut doc_state = doc_state.lock().await;
                    let sync_status_changed = doc_state.set_synced_to_state(public_key, true).await;
                    if sync_status_changed {
                        // Find out if now all peers are synced
                        let peers_synced = doc_state.peers_synced();
                        if let Some(peers_synced) = peers_synced {
                            // All peers are synced, so it should be possible to create a coherent
                            // document now.
                            // Process all new events and take patches
                            let mut patches = if let Some(content) = doc_state.content_mut() {
                                update_content(discovery_key, content, hypercores.clone())
                                    .await
                                    .unwrap()
                            } else {
                                let write_discovery_key = doc_state.write_discovery_key();
                                let (content, patches) = create_content(
                                    discovery_key,
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

                if let Some((peers_synced, patches)) = peers_synced {
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
            }
        }
    }
    println!("on_peer_event({}): Returning", is_initiator);
}
