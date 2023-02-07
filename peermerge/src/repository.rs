use automerge::Patch;
use dashmap::DashMap;
use futures::{
    channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender},
    StreamExt,
};
#[cfg(not(target_arch = "wasm32"))]
use random_access_disk::RandomAccessDisk;
use random_access_memory::RandomAccessMemory;
use random_access_storage::RandomAccess;
use std::fmt::Debug;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{debug, instrument, warn};

#[cfg(all(not(target_arch = "wasm32"), feature = "async-std"))]
use async_std::task;
#[cfg(feature = "async-std")]
use async_std::{sync::Mutex, task::yield_now};
#[cfg(all(not(target_arch = "wasm32"), feature = "tokio"))]
use tokio::task;
#[cfg(feature = "tokio")]
use tokio::{sync::Mutex, task::yield_now};
#[cfg(target_arch = "wasm32")]
use wasm_bindgen_futures::spawn_local;

#[cfg(not(target_arch = "wasm32"))]
use crate::feed::FeedDiskPersistence;
use crate::{common::PeerEvent, feed::on_protocol_new, DocumentId, IO};
use crate::{
    common::PeerEventContent,
    feed::{create_new_read_memory_feed, Feed, FeedMemoryPersistence, FeedPersistence, Protocol},
};
use crate::{
    common::{keys::discovery_key_from_public_key, storage::RepositoryStateWrapper},
    StateEventContent,
};
use crate::{Peermerge, StateEvent};

/// PeermergeRepository is a store for multiple Peermerges
#[derive(Debug, Clone)]
pub struct PeermergeRepository<T, U>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send,
    U: FeedPersistence,
{
    /// Name of the repository
    name: String,
    /// Current storable state
    repository_state: Arc<Mutex<RepositoryStateWrapper<T>>>,
    /// Created documents
    documents: Arc<DashMap<DocumentId, Peermerge<T, U>>>,
    /// Map of independent docs' discovery keys with their children.
    peermerge_dependencies: Arc<DashMap<[u8; 32], Vec<[u8; 32]>>>,
    /// Sender for events
    state_event_sender: Option<UnboundedSender<StateEvent>>,
}

impl<T, U> PeermergeRepository<T, U>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send + 'static,
    U: FeedPersistence,
{
    async fn notify_of_document_changes(&mut self) {
        if let Some(sender) = self.state_event_sender.as_mut() {
            if sender.is_closed() {
                self.state_event_sender = None;
            } else {
                let mut peermerge_patches: Vec<([u8; 32], Vec<Patch>)> = vec![];
                for mut peermerge in self.documents.iter_mut() {
                    let new_patches = peermerge.take_patches().await;
                    if new_patches.len() > 0 {
                        peermerge_patches.push((peermerge.id(), new_patches))
                    }
                }
                for (doc_discovery_key, patches) in peermerge_patches {
                    sender
                        .unbounded_send(StateEvent::new(
                            doc_discovery_key,
                            StateEventContent::DocumentChanged(patches),
                        ))
                        .unwrap();
                }
            }
        }
    }
}

//////////////////////////////////////////////////////
//
// Memory

impl PeermergeRepository<RandomAccessMemory, FeedMemoryPersistence> {
    pub async fn create_new_memory() -> Self {
        let state = RepositoryStateWrapper::new_memory().await;
        Self {
            name: "".to_string(),
            repository_state: Arc::new(Mutex::new(state)),
            documents: Arc::new(DashMap::new()),
            peermerge_dependencies: Arc::new(DashMap::new()),
            state_event_sender: None,
        }
    }

    #[instrument(skip_all, fields(name = self.name))]
    pub async fn connect_protocol_memory<T>(
        &mut self,
        protocol: &mut Protocol<T>,
        state_event_sender: &mut UnboundedSender<StateEvent>,
    ) -> anyhow::Result<()>
    where
        T: IO,
    {
        // First let's drain any patches that are not yet sent out, and push them out. These can
        // be created by scalar values inserted with peermerge.create_doc_memory() or other
        // mutating calls executed before this call.
        self.state_event_sender = Some(state_event_sender.clone());
        {
            self.notify_of_document_changes().await;
        }
        let (mut peer_event_sender, peer_event_receiver): (
            UnboundedSender<PeerEvent>,
            UnboundedReceiver<PeerEvent>,
        ) = unbounded();

        let state_event_sender_for_task = state_event_sender.clone();
        let repository_state = self.repository_state.clone();
        let documents_for_task = self.documents.clone();
        let name_for_task = self.name.clone();
        let task_span = tracing::debug_span!("call_on_peer_event_memory").or_current();
        #[cfg(not(target_arch = "wasm32"))]
        task::spawn(async move {
            let _entered = task_span.enter();
            on_peer_event_memory(
                peer_event_receiver,
                state_event_sender_for_task,
                repository_state,
                documents_for_task,
                &name_for_task,
            )
            .await;
        });
        #[cfg(target_arch = "wasm32")]
        spawn_local(async move {
            let _entered = task_span.enter();
            on_peer_event_memory(
                peer_event_receiver,
                state_event_sender_for_task,
                repository_state,
                documents_for_task,
                &name_for_task,
            )
            .await;
        });

        on_protocol_new(protocol, self.documents.clone(), &mut peer_event_sender).await?;
        Ok(())
    }
}

#[instrument(level = "debug", skip_all)]
async fn on_peer_event_memory(
    mut peer_event_receiver: UnboundedReceiver<PeerEvent>,
    mut state_event_sender: UnboundedSender<StateEvent>,
    mut repository_state: Arc<Mutex<RepositoryStateWrapper<RandomAccessMemory>>>,
    mut documents: Arc<DashMap<DocumentId, Peermerge<RandomAccessMemory, FeedMemoryPersistence>>>,
    name: &str,
) {
    while let Some(event) = peer_event_receiver.next().await {
        debug!("Received event {:?}", event);
        match event.content {
            PeerEventContent::NewPeersBroadcasted(public_keys) => {
                let mut document = documents.get_mut(&event.doc_discovery_key).unwrap();
                document
                    .process_new_peers_broadcasted_memory(public_keys)
                    .await;
            }
            _ => {
                process_peer_event(
                    event,
                    &mut state_event_sender,
                    &mut repository_state,
                    &mut documents,
                    name,
                )
                .await
            }
        }
    }
    debug!("Exiting");
}

//////////////////////////////////////////////////////
//
// Disk

#[cfg(not(target_arch = "wasm32"))]
impl PeermergeRepository<RandomAccessDisk, FeedDiskPersistence> {
    pub async fn open_disk(data_root_dir: PathBuf) -> Self {
        let state = RepositoryStateWrapper::open_disk(&data_root_dir).await;
        Self {
            name: "".to_string(),
            repository_state: Arc::new(Mutex::new(state)),
            documents: Arc::new(DashMap::new()),
            peermerge_dependencies: Arc::new(DashMap::new()),
            state_event_sender: None,
        }
    }
}

//////////////////////////////////////////////////////
//
// Utilities

#[instrument(level = "debug", skip_all)]
async fn process_peer_event<T, U>(
    event: PeerEvent,
    state_event_sender: &mut UnboundedSender<StateEvent>,
    repository_state: &mut Arc<Mutex<RepositoryStateWrapper<T>>>,
    documents: &mut Arc<DashMap<DocumentId, Peermerge<T, U>>>,
    name: &str,
) where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send + 'static,
    U: FeedPersistence,
{
    match event.content {
        PeerEventContent::NewPeersBroadcasted(_) => unreachable!("Implemented by concrete type"),
        PeerEventContent::PeerDisconnected(_) => {
            // This is an FYI message, just continue for now
        }
        PeerEventContent::RemotePeerSynced((discovery_key, synced_contiguous_length)) => {
            match state_event_sender.unbounded_send(StateEvent::new(
                event.doc_discovery_key,
                StateEventContent::RemotePeerSynced((discovery_key, synced_contiguous_length)),
            )) {
                Ok(()) => {}
                Err(err) => warn!(
                    "{}: could not notify remote peer synced to len {}, err {}",
                    name.to_string(),
                    synced_contiguous_length,
                    err
                ),
            }
        }
        PeerEventContent::PeerSynced((discovery_key, synced_contiguous_length)) => {
            let mut document = documents.get_mut(&event.doc_discovery_key).unwrap();
            let state_events = document
                .process_peer_synced(discovery_key, synced_contiguous_length)
                .await;
            for state_event in state_events {
                state_event_sender.unbounded_send(state_event).unwrap();
            }
        }
    }
}
