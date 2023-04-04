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
#[cfg(all(not(target_arch = "wasm32"), feature = "tokio"))]
use tokio::task;
#[cfg(target_arch = "wasm32")]
use wasm_bindgen_futures::spawn_local;

#[cfg(not(target_arch = "wasm32"))]
use crate::feed::FeedDiskPersistence;
use crate::{
    common::{
        cipher::{
            decode_encryption_key, decode_key_pair, encode_document_id, encode_encryption_key,
            encode_key_pair,
        },
        keys::{key_pair_from_bytes, partial_key_pair_to_bytes},
        storage::PeermergeStateWrapper,
        utils::Mutex,
        PeerEventContent,
    },
    feed::{FeedMemoryPersistence, FeedPersistence, Protocol},
    PeermergeError, StateEventContent,
};
use crate::{
    common::{DocumentInfo, PeerEvent},
    document::{get_document, get_document_ids},
    feed::on_protocol,
    DocumentId, NameDescription, IO,
};
use crate::{document::Document, StateEvent};

/// Peermerge is the main abstraction and a store for multiple documents.
#[derive(derivative::Derivative)]
#[derivative(Clone(bound = ""))]
#[derive(Debug)]
pub struct Peermerge<T, U>
where
    T: RandomAccess + Debug + Send,
    U: FeedPersistence,
{
    /// Name and description of this peer
    peer_header: NameDescription,
    /// Prefix
    prefix: PathBuf,
    /// Current storable state
    peermerge_state: Arc<Mutex<PeermergeStateWrapper<T>>>,
    /// Created documents
    documents: Arc<DashMap<DocumentId, Document<T, U>>>,
    /// Sender for events
    state_event_sender: Arc<Mutex<Option<UnboundedSender<StateEvent>>>>,
}

impl<T, U> Peermerge<T, U>
where
    T: RandomAccess + Debug + Send + 'static,
    U: FeedPersistence,
{
    pub fn peer_name(&self) -> String {
        self.peer_header.name.clone()
    }

    #[instrument(skip(self))]
    pub async fn watch(&mut self, document_id: &DocumentId, ids: Vec<ObjId>) {
        let mut document = get_document(&self.documents, document_id).await.unwrap();
        document.watch(ids).await;
    }

    #[instrument(skip(self, obj, prop), fields(obj = obj.as_ref().to_string(), peer_name = self.peer_header.name))]
    pub async fn get_id<O: AsRef<ObjId>, P: Into<Prop>>(
        &self,
        document_id: &DocumentId,
        obj: O,
        prop: P,
    ) -> Result<Option<ObjId>, PeermergeError> {
        let document = get_document(&self.documents, document_id).await.unwrap();
        document.get_id(obj, prop).await
    }

    #[instrument(skip(self, obj, prop), fields(obj = obj.as_ref().to_string(), peer_name = self.peer_header.name))]
    pub async fn get_scalar<O: AsRef<ObjId>, P: Into<Prop>>(
        &self,
        document_id: &DocumentId,
        obj: O,
        prop: P,
    ) -> Result<Option<ScalarValue>, PeermergeError> {
        let document = get_document(&self.documents, document_id).await.unwrap();
        document.get_scalar(obj, prop).await
    }

    #[instrument(skip(self, obj), fields(obj = obj.as_ref().to_string(), peer_name = self.peer_header.name))]
    pub async fn realize_text<O: AsRef<ObjId>>(
        &self,
        document_id: &DocumentId,
        obj: O,
    ) -> Result<Option<String>, PeermergeError> {
        let document = get_document(&self.documents, document_id).await.unwrap();
        document.realize_text(obj).await
    }

    #[instrument(skip(self, obj, prop), fields(obj = obj.as_ref().to_string(), peer_name = self.peer_header.name))]
    pub async fn put_object<O: AsRef<ObjId>, P: Into<Prop>>(
        &mut self,
        document_id: &DocumentId,
        obj: O,
        prop: P,
        object: ObjType,
    ) -> Result<ObjId, PeermergeError> {
        let result = {
            let mut document = get_document(&self.documents, document_id).await.unwrap();
            document.put_object(obj, prop, object).await?
        };
        {
            self.notify_of_document_changes().await;
        }
        Ok(result)
    }

    #[instrument(skip(self, obj, prop, value), fields(obj = obj.as_ref().to_string(), peer_name = self.peer_header.name))]
    pub async fn put_scalar<O: AsRef<ObjId>, P: Into<Prop>, V: Into<ScalarValue>>(
        &mut self,
        document_id: &DocumentId,
        obj: O,
        prop: P,
        value: V,
    ) -> Result<(), PeermergeError> {
        let result = {
            let mut document = get_document(&self.documents, document_id).await.unwrap();
            document.put_scalar(obj, prop, value).await?
        };
        {
            self.notify_of_document_changes().await;
        }
        Ok(result)
    }

    #[instrument(skip(self, obj), fields(obj = obj.as_ref().to_string(), peer_name = self.peer_header.name))]
    pub async fn splice_text<O: AsRef<ObjId>>(
        &mut self,
        document_id: &DocumentId,
        obj: O,
        index: usize,
        delete: usize,
        text: &str,
    ) -> Result<(), PeermergeError> {
        let result = {
            let mut document = get_document(&self.documents, document_id).await.unwrap();
            document.splice_text(obj, index, delete, text).await?
        };
        {
            self.notify_of_document_changes().await;
        }
        Ok(result)
    }

    #[instrument(skip(self), fields(peer_name = self.peer_header.name))]
    pub async fn close(&mut self) -> Result<(), PeermergeError> {
        for document_id in get_document_ids(&self.documents).await {
            let mut document = get_document(&self.documents, &document_id).await.unwrap();
            document.close().await?;
        }
        Ok(())
    }

    #[instrument(skip(self), fields(peer_name = self.peer_header.name))]
    pub async fn cork(&mut self, document_id: &DocumentId) {
        let mut document = get_document(&self.documents, document_id).await.unwrap();
        document.cork().await
    }

    #[instrument(skip(self), fields(peer_name = self.peer_header.name))]
    pub async fn uncork(&mut self, document_id: &DocumentId) -> Result<(), PeermergeError> {
        let mut document = get_document(&self.documents, document_id).await.unwrap();
        document.uncork().await
    }

    #[instrument(skip(self), fields(peer_name = self.peer_header.name))]
    pub async fn doc_url(&self, document_id: &DocumentId) -> String {
        let document = get_document(&self.documents, document_id).await.unwrap();
        document.doc_url()
    }

    #[instrument(skip(self), fields(peer_name = self.peer_header.name))]
    pub async fn proxy_doc_url(&self, document_id: &DocumentId) -> String {
        let document = get_document(&self.documents, document_id).await.unwrap();
        document.proxy_doc_url()
    }

    #[instrument(skip(self), fields(peer_name = self.peer_header.name))]
    pub async fn feed_discovery_keys(&self, document_id: &DocumentId) -> Vec<[u8; 32]> {
        let document = get_document(&self.documents, document_id).await.unwrap();
        document.feed_discovery_keys().await
    }

    #[instrument(skip(self), fields(peer_name = self.peer_header.name))]
    pub async fn encryption_key(&self, document_id: &DocumentId) -> Option<String> {
        let document = get_document(&self.documents, document_id).await.unwrap();
        document
            .encryption_key()
            .map(|encryption_key| encode_encryption_key(&encryption_key))
    }

    #[instrument(skip(self), fields(peer_name = self.peer_header.name))]
    pub async fn write_key_pair(&self, document_id: &DocumentId) -> String {
        let document = get_document(&self.documents, document_id).await.unwrap();
        let key_pair = document.write_key_pair().await;
        encode_key_pair(&partial_key_pair_to_bytes(key_pair))
    }

    async fn notify_of_document_changes(&mut self) {
        let mut state_event_sender = self.state_event_sender.lock().await;
        if let Some(sender) = state_event_sender.as_mut() {
            if sender.is_closed() {
                *state_event_sender = None;
            } else {
                let mut document_patches: Vec<([u8; 32], Vec<Patch>)> = vec![];
                for document_id in get_document_ids(&self.documents).await {
                    let mut document = get_document(&self.documents, &document_id).await.unwrap();
                    let new_patches = document.take_patches().await;
                    if new_patches.len() > 0 {
                        document_patches.push((document.id(), new_patches))
                    }
                }
                for (doc_discovery_key, patches) in document_patches {
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

    async fn add_document(&mut self, document: Document<T, U>) -> DocumentId {
        let mut state = self.peermerge_state.lock().await;
        let id = document.id();
        self.documents.insert(id, document);
        state.add_document_id_to_state(&id).await;
        id
    }
}

//////////////////////////////////////////////////////
//
// Memory

impl Peermerge<RandomAccessMemory, FeedMemoryPersistence> {
    pub async fn new_memory(peer_header: NameDescription) -> Self {
        let state = PeermergeStateWrapper::new_memory(&peer_header).await;
        Self {
            peer_header,
            prefix: PathBuf::new(),
            peermerge_state: Arc::new(Mutex::new(state)),
            documents: Arc::new(DashMap::new()),
            state_event_sender: Arc::new(Mutex::new(None)),
        }
    }

    pub async fn create_new_document_memory<P: Into<Prop>, V: Into<ScalarValue>>(
        &mut self,
        document_header: NameDescription,
        doc_root_scalars: Vec<(P, V)>,
        encrypted: bool,
    ) -> Result<DocumentId, PeermergeError> {
        let document = Document::create_new_memory(
            &self.peer_header,
            document_header,
            doc_root_scalars,
            encrypted,
        )
        .await?;
        Ok(self.add_document(document).await)
    }

    pub async fn attach_writer_document_memory(
        &mut self,
        doc_url: &str,
        encryption_key: &Option<String>,
    ) -> Result<DocumentId, PeermergeError> {
        let document = Document::attach_writer_memory(
            &self.peer_header,
            doc_url,
            &decode_encryption_key(encryption_key),
        )
        .await?;
        Ok(self.add_document(document).await)
    }

    /// Reattaches a writer based on given write_key_pair and peer name
    pub async fn reattach_writer_document_memory(
        &mut self,
        doc_url: &str,
        encryption_key: &Option<String>,
        write_key_pair: &str,
    ) -> Result<DocumentId, PeermergeError> {
        let document = Document::reattach_writer_memory(
            key_pair_from_bytes(&decode_key_pair(write_key_pair)),
            &self.peer_header.name,
            doc_url,
            &decode_encryption_key(encryption_key),
        )
        .await?;
        Ok(self.add_document(document).await)
    }

    pub async fn attach_proxy_document_memory(&mut self, doc_url: &str) -> DocumentId {
        let document = Document::attach_proxy_memory(&self.peer_header, doc_url).await;
        self.add_document(document).await
    }

    #[instrument(skip_all, fields(peer_name = self.peer_header.name))]
    pub async fn connect_protocol_memory<T>(
        &mut self,
        protocol: &mut Protocol<T>,
        state_event_sender: &mut UnboundedSender<StateEvent>,
    ) -> Result<(), PeermergeError>
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
        let peermerge_state = self.peermerge_state.clone();
        let documents_for_task = self.documents.clone();
        let peer_name_for_task = self.peer_header.name.clone();
        let task_span = tracing::debug_span!("call_on_peer_event_memory").or_current();
        #[cfg(not(target_arch = "wasm32"))]
        task::spawn(async move {
            let _entered = task_span.enter();
            on_peer_event_memory(
                peer_event_receiver,
                state_event_sender_for_task,
                peermerge_state,
                documents_for_task,
                &peer_name_for_task,
            )
            .await;
        });
        #[cfg(target_arch = "wasm32")]
        spawn_local(async move {
            let _entered = task_span.enter();
            on_peer_event_memory(
                peer_event_receiver,
                state_event_sender_for_task,
                peermerge_state,
                documents_for_task,
                &peer_name_for_task,
            )
            .await;
        });

        on_protocol(protocol, self.documents.clone(), &mut peer_event_sender).await?;
        Ok(())
    }
}

#[instrument(level = "debug", skip_all)]
async fn on_peer_event_memory(
    mut peer_event_receiver: UnboundedReceiver<PeerEvent>,
    mut state_event_sender: UnboundedSender<StateEvent>,
    mut peermerge_state: Arc<Mutex<PeermergeStateWrapper<RandomAccessMemory>>>,
    mut documents: Arc<DashMap<DocumentId, Document<RandomAccessMemory, FeedMemoryPersistence>>>,
    peer_name: &str,
) {
    while let Some(event) = peer_event_receiver.next().await {
        debug!("Received event {:?}", event);
        match event.content {
            PeerEventContent::NewPeersBroadcasted(public_keys) => {
                let mut document = get_document(&documents, &event.doc_discovery_key)
                    .await
                    .unwrap();
                document
                    .process_new_peers_broadcasted_memory(public_keys)
                    .await;
            }
            _ => {
                process_peer_event(
                    event,
                    &mut state_event_sender,
                    &mut peermerge_state,
                    &mut documents,
                    &peer_name,
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
impl Peermerge<RandomAccessDisk, FeedDiskPersistence> {
    pub async fn create_new_disk(peer_header: NameDescription, data_root_dir: &PathBuf) -> Self {
        let state = PeermergeStateWrapper::new_disk(&peer_header, data_root_dir).await;
        Self {
            peer_header,
            prefix: data_root_dir.clone(),
            peermerge_state: Arc::new(Mutex::new(state)),
            documents: Arc::new(DashMap::new()),
            state_event_sender: Arc::new(Mutex::new(None)),
        }
    }

    pub async fn document_infos_disk(
        data_root_dir: &PathBuf,
    ) -> Result<Option<Vec<DocumentInfo>>, PeermergeError> {
        if let Some(state_wrapper) = PeermergeStateWrapper::open_disk(data_root_dir).await? {
            let mut document_infos: Vec<DocumentInfo> = vec![];
            for document_id in &state_wrapper.state.document_ids {
                let postfix = encode_document_id(&document_id);
                let document_data_root_dir = data_root_dir.join(postfix);
                document_infos.push(Document::info_disk(&document_data_root_dir).await?);
            }
            Ok(Some(document_infos))
        } else {
            Ok(None)
        }
    }

    pub async fn open_disk(
        encryption_keys: HashMap<DocumentId, String>,
        data_root_dir: &PathBuf,
    ) -> Result<Self, PeermergeError> {
        let state_wrapper = PeermergeStateWrapper::open_disk(data_root_dir)
            .await?
            .expect("Not a valid peermerge directory");
        let state = state_wrapper.state();
        let peer_header = state.peer_header.clone();
        let documents: DashMap<DocumentId, Document<RandomAccessDisk, FeedDiskPersistence>> =
            DashMap::new();
        for document_id in &state_wrapper.state.document_ids {
            let encryption_key: Option<Vec<u8>> =
                decode_encryption_key(&encryption_keys.get(document_id).cloned());
            let postfix = encode_document_id(&document_id);
            let document_data_root_dir = data_root_dir.join(postfix);
            let document =
                Document::open_disk(&peer_header, &encryption_key, &document_data_root_dir).await?;
            documents.insert(document_id.clone(), document);
        }

        Ok(Self {
            peer_header,
            prefix: data_root_dir.clone(),
            peermerge_state: Arc::new(Mutex::new(state_wrapper)),
            documents: Arc::new(documents),
            state_event_sender: Arc::new(Mutex::new(None)),
        })
    }

    pub async fn create_new_document_disk<P: Into<Prop>, V: Into<ScalarValue>>(
        &mut self,
        document_header: NameDescription,
        doc_root_scalars: Vec<(P, V)>,
        encrypted: bool,
    ) -> Result<DocumentId, PeermergeError> {
        let document = Document::create_new_disk(
            &self.peer_header,
            document_header,
            doc_root_scalars,
            encrypted,
            &self.prefix,
        )
        .await?;
        Ok(self.add_document(document).await)
    }

    pub async fn attach_writer_document_disk(
        &mut self,
        doc_url: &str,
        encryption_key: &Option<String>,
    ) -> Result<DocumentId, PeermergeError> {
        let document = Document::attach_writer_disk(
            &self.peer_header,
            doc_url,
            &decode_encryption_key(encryption_key),
            &self.prefix,
        )
        .await?;
        Ok(self.add_document(document).await)
    }

    pub async fn attach_proxy_document_disk(&mut self, doc_url: &str) -> DocumentId {
        let document = Document::attach_proxy_disk(&self.peer_header, doc_url, &self.prefix).await;
        self.add_document(document).await
    }

    #[instrument(skip_all, fields(name = self.peer_header.name))]
    pub async fn connect_protocol_disk<T>(
        &mut self,
        protocol: &mut Protocol<T>,
        state_event_sender: &mut UnboundedSender<StateEvent>,
    ) -> Result<(), PeermergeError>
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
        let peermerge_state = self.peermerge_state.clone();
        let documents_for_task = self.documents.clone();
        let peer_name_for_task = self.peer_header.name.clone();
        let task_span = tracing::debug_span!("call_on_peer_event_disk").or_current();
        task::spawn(async move {
            let _entered = task_span.enter();
            on_peer_event_disk(
                peer_event_receiver,
                state_event_sender_for_task,
                peermerge_state,
                documents_for_task,
                &peer_name_for_task,
            )
            .await;
        });

        on_protocol(protocol, self.documents.clone(), &mut peer_event_sender).await?;
        Ok(())
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[instrument(level = "debug", skip_all)]
async fn on_peer_event_disk(
    mut peer_event_receiver: UnboundedReceiver<PeerEvent>,
    mut state_event_sender: UnboundedSender<StateEvent>,
    mut peermerge_state: Arc<Mutex<PeermergeStateWrapper<RandomAccessDisk>>>,
    mut documents: Arc<DashMap<DocumentId, Document<RandomAccessDisk, FeedDiskPersistence>>>,
    peer_name: &str,
) {
    while let Some(event) = peer_event_receiver.next().await {
        debug!("Received event {:?}", event);
        match event.content {
            PeerEventContent::NewPeersBroadcasted(public_keys) => {
                let mut document = get_document(&documents, &event.doc_discovery_key)
                    .await
                    .unwrap();
                document
                    .process_new_peers_broadcasted_disk(public_keys)
                    .await;
            }
            _ => {
                process_peer_event(
                    event,
                    &mut state_event_sender,
                    &mut peermerge_state,
                    &mut documents,
                    &peer_name,
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
    _peermerge_state: &mut Arc<Mutex<PeermergeStateWrapper<T>>>,
    documents: &mut Arc<DashMap<DocumentId, Document<T, U>>>,
    peer_name: &str,
) where
    T: RandomAccess + Debug + Send + 'static,
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
                    peer_name.to_string(),
                    synced_contiguous_length,
                    err
                ),
            }
        }
        PeerEventContent::PeerSynced((discovery_key, synced_contiguous_length)) => {
            let mut document = get_document(&documents, &event.doc_discovery_key)
                .await
                .unwrap();
            let state_events = document
                .process_peer_synced(discovery_key, synced_contiguous_length)
                .await;
            for state_event in state_events {
                state_event_sender.unbounded_send(state_event).unwrap();
            }
        }
    }
}
