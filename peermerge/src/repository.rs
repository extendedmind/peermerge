use automerge::{ObjId, ObjType, Patch, Prop, ScalarValue};
use dashmap::DashMap;
use futures::{
    channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender},
    StreamExt,
};
#[cfg(not(target_arch = "wasm32"))]
use random_access_disk::RandomAccessDisk;
use random_access_memory::RandomAccessMemory;
use random_access_storage::RandomAccess;
use std::path::PathBuf;
use std::sync::Arc;
use std::{collections::HashMap, fmt::Debug};
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
use crate::{common::storage::RepositoryStateWrapper, StateEventContent};
use crate::{common::PeerEvent, feed::on_protocol_new, DocumentId, IO};
use crate::{
    common::PeerEventContent,
    feed::{FeedMemoryPersistence, FeedPersistence, Protocol},
};
use crate::{Peermerge, StateEvent};

/// PeermergeRepository is a store for multiple Peermerges
#[derive(derivative::Derivative)]
#[derivative(Clone(bound = ""))]
#[derive(Debug)]
pub struct PeermergeRepository<T, U>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send,
    U: FeedPersistence,
{
    /// Name of the repository
    name: String,
    /// Prefix
    prefix: PathBuf,
    /// Current storable state
    repository_state: Arc<Mutex<RepositoryStateWrapper<T>>>,
    /// Created documents
    documents: Arc<DashMap<DocumentId, Peermerge<T, U>>>,
    /// Sender for events
    state_event_sender: Arc<Mutex<Option<UnboundedSender<StateEvent>>>>,
}

impl<T, U> PeermergeRepository<T, U>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send + 'static,
    U: FeedPersistence,
{
    pub fn name(&self) -> String {
        self.name.clone()
    }

    #[instrument(skip(self))]
    pub async fn watch(&mut self, document_id: &DocumentId, ids: Vec<ObjId>) {
        let mut document = self.documents.get_mut(document_id).unwrap();
        document.watch(ids).await;
    }

    #[instrument(skip(self, obj, prop), fields(obj = obj.as_ref().to_string(), peer_name = self.name))]
    pub async fn get_id<O: AsRef<ObjId>, P: Into<Prop>>(
        &self,
        document_id: &DocumentId,
        obj: O,
        prop: P,
    ) -> anyhow::Result<Option<ObjId>> {
        let document = self.documents.get(document_id).unwrap();
        document.get_id(obj, prop).await
    }

    #[instrument(skip(self, obj, prop), fields(obj = obj.as_ref().to_string(), peer_name = self.name))]
    pub async fn get_scalar<O: AsRef<ObjId>, P: Into<Prop>>(
        &self,
        document_id: &DocumentId,
        obj: O,
        prop: P,
    ) -> anyhow::Result<Option<ScalarValue>> {
        let document = self.documents.get(document_id).unwrap();
        document.get_scalar(obj, prop).await
    }

    #[instrument(skip(self, obj), fields(obj = obj.as_ref().to_string(), peer_name = self.name))]
    pub async fn realize_text<O: AsRef<ObjId>>(
        &self,
        document_id: &DocumentId,
        obj: O,
    ) -> anyhow::Result<Option<String>> {
        let document = self.documents.get(document_id).unwrap();
        document.realize_text(obj).await
    }

    #[instrument(skip(self, obj, prop), fields(obj = obj.as_ref().to_string(), peer_name = self.name))]
    pub async fn put_object<O: AsRef<ObjId>, P: Into<Prop>>(
        &mut self,
        document_id: &DocumentId,
        obj: O,
        prop: P,
        object: ObjType,
    ) -> anyhow::Result<ObjId> {
        let result = {
            let mut document = self.documents.get_mut(document_id).unwrap();
            document.put_object(obj, prop, object).await?
        };
        {
            self.notify_of_document_changes().await;
        }
        Ok(result)
    }

    #[instrument(skip(self, obj, prop, value), fields(obj = obj.as_ref().to_string(), peer_name = self.name))]
    pub async fn put_scalar<O: AsRef<ObjId>, P: Into<Prop>, V: Into<ScalarValue>>(
        &mut self,
        document_id: &DocumentId,
        obj: O,
        prop: P,
        value: V,
    ) -> anyhow::Result<()> {
        let result = {
            let mut document = self.documents.get_mut(document_id).unwrap();
            document.put_scalar(obj, prop, value).await?
        };
        {
            self.notify_of_document_changes().await;
        }
        Ok(result)
    }

    #[instrument(skip(self, obj), fields(obj = obj.as_ref().to_string(), peer_name = self.name))]
    pub async fn splice_text<O: AsRef<ObjId>>(
        &mut self,
        document_id: &DocumentId,
        obj: O,
        index: usize,
        delete: usize,
        text: &str,
    ) -> anyhow::Result<()> {
        let result = {
            let mut document = self.documents.get_mut(document_id).unwrap();
            document.splice_text(obj, index, delete, text).await?
        };
        {
            self.notify_of_document_changes().await;
        }
        Ok(result)
    }

    #[instrument(skip(self))]
    pub async fn cork(&mut self, document_id: &DocumentId) {
        let mut document = self.documents.get_mut(document_id).unwrap();
        document.cork().await
    }

    #[instrument(skip(self))]
    pub async fn uncork(&mut self, document_id: &DocumentId) -> anyhow::Result<()> {
        let mut document = self.documents.get_mut(document_id).unwrap();
        document.uncork().await
    }

    #[instrument(skip(self))]
    pub fn doc_url(&self, document_id: &DocumentId) -> String {
        let document = self.documents.get(document_id).unwrap();
        document.doc_url()
    }

    #[instrument(skip(self))]
    pub fn encryption_key(&self, document_id: &DocumentId) -> Option<Vec<u8>> {
        let document = self.documents.get(document_id).unwrap();
        document.encryption_key()
    }

    async fn notify_of_document_changes(&mut self) {
        let mut state_event_sender = self.state_event_sender.lock().await;
        if let Some(sender) = state_event_sender.as_mut() {
            if sender.is_closed() {
                *state_event_sender = None;
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

    async fn add_document(&mut self, document: Peermerge<T, U>) -> DocumentId {
        let mut state = self.repository_state.lock().await;
        let id = document.id();
        self.documents.insert(id, document);
        state.add_document_id_to_state(&id).await;
        id
    }
}

//////////////////////////////////////////////////////
//
// Memory

impl PeermergeRepository<RandomAccessMemory, FeedMemoryPersistence> {
    pub async fn new_memory(name: &str) -> Self {
        let state = RepositoryStateWrapper::new_memory(name).await;
        Self {
            name: name.to_string(),
            prefix: PathBuf::new(),
            repository_state: Arc::new(Mutex::new(state)),
            documents: Arc::new(DashMap::new()),
            state_event_sender: Arc::new(Mutex::new(None)),
        }
    }

    pub async fn create_new_document_memory<P: Into<Prop>, V: Into<ScalarValue>>(
        &mut self,
        root_scalars: Vec<(P, V)>,
        encrypted: bool,
    ) -> DocumentId {
        let document = Peermerge::create_new_memory(&self.name, root_scalars, encrypted).await;
        self.add_document(document).await
    }

    pub async fn attach_writer_document_memory(
        &mut self,
        doc_url: &str,
        encryption_key: &Option<Vec<u8>>,
    ) -> DocumentId {
        let document = Peermerge::attach_writer_memory(&self.name, doc_url, encryption_key).await;
        self.add_document(document).await
    }

    pub async fn attach_proxy_document_memory(&mut self, doc_url: &str) -> DocumentId {
        let document = Peermerge::attach_proxy_memory(&self.name, doc_url).await;
        self.add_document(document).await
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
    pub async fn create_new_disk(name: &str, data_root_dir: &PathBuf) -> Self {
        let state = RepositoryStateWrapper::new_disk(name, data_root_dir).await;
        Self {
            name: name.to_string(),
            prefix: data_root_dir.clone(),
            repository_state: Arc::new(Mutex::new(state)),
            documents: Arc::new(DashMap::new()),
            state_event_sender: Arc::new(Mutex::new(None)),
        }
    }

    pub async fn open_disk(
        encryption_keys: HashMap<DocumentId, Vec<u8>>,
        data_root_dir: &PathBuf,
    ) -> Self {
        let state_wrapper = RepositoryStateWrapper::open_disk(data_root_dir).await;
        let state = state_wrapper.state();
        let name = state.name.clone();
        let documents: DashMap<DocumentId, Peermerge<RandomAccessDisk, FeedDiskPersistence>> =
            DashMap::new();
        for document_id in &state_wrapper.state.document_ids {
            let encryption_key: Option<Vec<u8>> = encryption_keys.get(document_id).cloned();
            let document = Peermerge::open_disk(&encryption_key, data_root_dir).await;
            documents.insert(document_id.clone(), document);
        }

        Self {
            name,
            prefix: data_root_dir.clone(),
            repository_state: Arc::new(Mutex::new(state_wrapper)),
            documents: Arc::new(documents),
            state_event_sender: Arc::new(Mutex::new(None)),
        }
    }

    pub async fn create_new_document_disk<P: Into<Prop>, V: Into<ScalarValue>>(
        &mut self,
        root_scalars: Vec<(P, V)>,
        encrypted: bool,
    ) -> DocumentId {
        let document =
            Peermerge::create_new_disk(&self.name, root_scalars, encrypted, &self.prefix).await;
        self.add_document(document).await
    }

    pub async fn attach_writer_document_disk(
        &mut self,
        doc_url: &str,
        encryption_key: &Option<Vec<u8>>,
    ) -> DocumentId {
        let document =
            Peermerge::attach_writer_disk(&self.name, doc_url, encryption_key, &self.prefix).await;
        self.add_document(document).await
    }

    pub async fn attach_proxy_document_disk(&mut self, doc_url: &str) -> DocumentId {
        let document = Peermerge::attach_proxy_disk(&self.name, doc_url, &self.prefix).await;
        self.add_document(document).await
    }

    #[instrument(skip_all, fields(name = self.name))]
    pub async fn connect_protocol_disk<T>(
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
        let repository_state = self.repository_state.clone();
        let documents_for_task = self.documents.clone();
        let name_for_task = self.name.clone();
        let task_span = tracing::debug_span!("call_on_peer_event_disk").or_current();
        task::spawn(async move {
            let _entered = task_span.enter();
            on_peer_event_disk(
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

#[cfg(not(target_arch = "wasm32"))]
#[instrument(level = "debug", skip_all)]
async fn on_peer_event_disk(
    mut peer_event_receiver: UnboundedReceiver<PeerEvent>,
    mut state_event_sender: UnboundedSender<StateEvent>,
    mut repository_state: Arc<Mutex<RepositoryStateWrapper<RandomAccessDisk>>>,
    mut documents: Arc<DashMap<DocumentId, Peermerge<RandomAccessDisk, FeedDiskPersistence>>>,
    name: &str,
) {
    while let Some(event) = peer_event_receiver.next().await {
        debug!("Received event {:?}", event);
        match event.content {
            PeerEventContent::NewPeersBroadcasted(public_keys) => {
                let mut document = documents.get_mut(&event.doc_discovery_key).unwrap();
                document
                    .process_new_peers_broadcasted_disk(public_keys)
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
