use async_channel::{unbounded, Receiver, Sender};
use async_std::sync::{Arc, Mutex};
#[cfg(not(target_arch = "wasm32"))]
use async_std::task;
use automerge::{ObjId, ObjType, Prop, ScalarValue};
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
    hypercore::{on_protocol, HypercoreWrapper},
    store::{create_and_insert_read_memory_hypercores, DocStore},
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
        while let Some(event) = sync_event_receiver.next().await {
            println!("connect_document: received sync event: {:?}", event);
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
    ) -> anyhow::Result<()> {
        self.store
            .lock()
            .await
            .put_object(discovery_key, obj, prop, object)
            .await
    }

    pub async fn watch_root_props<P: Into<Prop>>(
        &mut self,
        discovery_key: &[u8; 32],
        root_props: Vec<P>,
    ) {
        let mut watch_root_props: Vec<Prop> = Vec::with_capacity(root_props.len());
        for root_prop in root_props {
            watch_root_props.push(root_prop.into());
        }
        self.store
            .lock()
            .await
            .watch_root_props(discovery_key, watch_root_props)
            .await;
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
        let doc = init_doc_with_root_scalars(root_scalars);
        self.store.lock().await.add_doc_disk(doc).await
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
        let doc = init_doc_with_root_scalars(root_scalars);
        self.store.lock().await.add_doc_memory(doc).await
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
        if let Some(hypercore_store) = self.store.lock().await.get_mut(discovery_key) {
            let (mut peer_event_sender, peer_event_receiver): (
                Sender<PeerEvent>,
                Receiver<PeerEvent>,
            ) = unbounded();

            let sync_event_sender_for_task = sync_event_sender.clone();
            let doc_state = hypercore_store.doc_state();
            let hypercores = hypercore_store.hypercores();
            let is_initiator = protocol.is_initiator();
            #[cfg(not(target_arch = "wasm32"))]
            task::spawn(async move {
                on_peer_event_memory(
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
            PeerEvent::PeerSyncStarted(public_key) => {
                {
                    // Set peer to not-synced
                    let mut doc_state = doc_state.lock().await;
                    doc_state.set_synced_to_state(public_key, false).await;
                }
            }
            PeerEvent::PeerSynced(public_key) => {
                let (peers_synced, cursors) = {
                    // Set peer to synced
                    let mut doc_state = doc_state.lock().await;
                    let sync_status_changed = doc_state.set_synced_to_state(public_key, true).await;
                    if sync_status_changed {
                        // Find out if now all peers are synced
                        let peers_synced = doc_state.peers_synced();
                        // Also return all of the current cursors to be able to read the changes
                        let cursors = doc_state.cursors();

                        (peers_synced, cursors)
                    } else {
                        // If nothing changed, don't reannounce
                        (None, None)
                    }
                };

                if let Some(len) = peers_synced {
                    sync_event_sender
                        .send(SynchronizeEvent::PeersSynced(len))
                        .await
                        .unwrap();
                }
            }
        }
    }
    println!("on_peer_event({}): Returning", is_initiator);
}
