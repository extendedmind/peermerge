use automerge::{transaction::Transaction, AutomergeError, ObjId, Patch};
use dashmap::DashMap;
use futures::{
    channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender},
    StreamExt,
};
use hypercore_protocol::hypercore::SigningKey;
#[cfg(not(target_arch = "wasm32"))]
use random_access_disk::RandomAccessDisk;
use random_access_memory::RandomAccessMemory;
use random_access_storage::RandomAccess;
use std::path::PathBuf;
use std::sync::Arc;
use std::{collections::HashMap, fmt::Debug};
use tracing::{debug, instrument};

#[cfg(all(not(target_arch = "wasm32"), feature = "async-std"))]
use async_std::task;
#[cfg(all(not(target_arch = "wasm32"), feature = "tokio"))]
use tokio::task;
#[cfg(target_arch = "wasm32")]
use wasm_bindgen_futures::spawn_local;

use crate::{
    common::{
        cipher::{
            decode_doc_url, decode_document_secret, decode_reattach_secret, encode_document_id,
            encode_document_secret, encode_reattach_secret, DocumentSecret,
        },
        keys::{signing_key_from_bytes, signing_key_to_bytes},
        storage::PeermergeStateWrapper,
        utils::Mutex,
        FeedEventContent,
    },
    document::{
        get_document_by_discovery_key, DocumentParent, DocumentSettings, NewDocumentResult,
    },
    feeds::{FeedMemoryPersistence, FeedPersistence, Protocol},
    options::PeermergeMemoryOptions,
    AttachDocumentMemoryOptions, AutomergeDoc, CreateNewDocumentMemoryOptions, DocumentSharingInfo,
    PeerId, PeermergeError, StateEventContent,
};
use crate::{
    common::{DocumentInfo, FeedEvent},
    document::{get_document, get_document_ids},
    feeds::on_protocol,
    DocumentId, NameDescription, IO,
};
use crate::{document::Document, StateEvent};
#[cfg(not(target_arch = "wasm32"))]
use crate::{
    feeds::FeedDiskPersistence,
    options::{AttachDocumentDiskOptions, CreateNewDocumentDiskOptions, PeermergeDiskOptions},
};

/// Peermerge is the main abstraction and a store for multiple documents.
#[derive(derivative::Derivative)]
#[derivative(Clone(bound = ""))]
#[derive(Debug)]
pub struct Peermerge<T, U>
where
    T: RandomAccess + Debug + Send,
    U: FeedPersistence,
{
    /// Id of this peer
    peer_id: PeerId,
    /// Name and description of this peer that's given by default
    /// to new documents. Can be something different within individual
    /// documents as they change over time.
    default_peer_header: NameDescription,
    // General settings for all documents
    document_settings: DocumentSettings,
    /// Prefix
    prefix: PathBuf,
    /// Current storable state
    peermerge_state: Arc<Mutex<PeermergeStateWrapper<T>>>,
    /// Created documents
    documents: Arc<DashMap<DocumentId, Document<T, U>>>,
    /// Sender for events
    state_event_sender: Arc<Mutex<Option<UnboundedSender<StateEvent>>>>,
    /// Transient save of reattach secrets for child documents, used only for memory
    /// peermerges. Need to be stored because child documents' write feeds can't be
    /// created immediately.
    reattach_secrets: Option<HashMap<DocumentId, SigningKey>>,
}

impl<T, U> Peermerge<T, U>
where
    T: RandomAccess + Debug + Send + 'static,
    U: FeedPersistence,
{
    /// Get my peer id
    #[instrument(skip(self), fields(peer_name = self.default_peer_header.name))]
    pub fn peer_id(&self) -> PeerId {
        self.peer_id
    }

    /// Get all known peer ids in a document
    #[instrument(skip(self), fields(peer_name = self.default_peer_header.name))]
    pub async fn peer_ids(&self, document_id: &DocumentId) -> Result<Vec<PeerId>, PeermergeError> {
        let document = self.get_document(document_id).await?;
        Ok(document.peer_ids().await)
    }

    /// Get my default peer header given to a new document
    pub fn default_peer_header(&self) -> NameDescription {
        self.default_peer_header.clone()
    }

    /// Get the current peer header value in a new document
    pub async fn peer_header(
        &self,
        document_id: &DocumentId,
        peer_id: &PeerId,
    ) -> Result<Option<NameDescription>, PeermergeError> {
        let document = self.get_document(document_id).await?;
        Ok(document.peer_header(peer_id).await)
    }

    pub async fn set_state_event_sender(
        &mut self,
        state_event_sender: Option<UnboundedSender<StateEvent>>,
    ) -> Result<(), PeermergeError> {
        let not_empty = state_event_sender.is_some();

        {
            *self.state_event_sender.lock().await = state_event_sender;
        }
        if not_empty {
            // Let's drain any patches that are not yet sent out, and push them out. These can
            // be created by values inserted with peermerge.create_new_document_memory/disk()
            // or other mutating calls executed before this call without a state_event_sender.
            let mut state_event_sender = self.state_event_sender.lock().await;
            if let Some(sender) = state_event_sender.as_mut() {
                if sender.is_closed() {
                    *state_event_sender = None;
                } else {
                    let mut document_patches: Vec<(DocumentId, Vec<Patch>)> = vec![];
                    for document_id in get_document_ids(&self.documents).await {
                        let mut document = self.get_document(&document_id).await?;
                        let new_patches = document.take_patches().await;
                        if !new_patches.is_empty() {
                            document_patches.push((document_id, new_patches))
                        }
                    }
                    for (document_id, patches) in document_patches {
                        sender
                            .unbounded_send(StateEvent::new(
                                document_id,
                                StateEventContent::DocumentChanged {
                                    change_id: None,
                                    patches,
                                },
                            ))
                            .unwrap();
                    }
                }
            }
        }
        Ok(())
    }

    #[instrument(skip(self, cb), fields(peer_name = self.default_peer_header.name))]
    pub async fn transact<F, O>(&self, document_id: &DocumentId, cb: F) -> Result<O, PeermergeError>
    where
        F: FnOnce(&AutomergeDoc) -> Result<O, AutomergeError>,
    {
        let result = {
            let document = self.get_document(document_id).await?;
            document.transact(cb).await?
        };
        Ok(result)
    }

    #[instrument(skip(self, cb), fields(peer_name = self.default_peer_header.name))]
    pub async fn transact_mut<F, O>(
        &mut self,
        document_id: &DocumentId,
        cb: F,
        change_id: Option<Vec<u8>>,
    ) -> Result<O, PeermergeError>
    where
        F: FnOnce(&mut AutomergeDoc) -> Result<O, AutomergeError>,
    {
        let (result, state_events) = {
            let mut document = self.get_document(document_id).await?;
            document.transact_mut(cb, change_id).await?
        };
        if !state_events.is_empty() {
            if let Some(state_event_sender) = self.state_event_sender.lock().await.as_mut() {
                send_state_events(state_event_sender, state_events, &self.peermerge_state).await;
            }
        }
        Ok(result)
    }

    #[instrument(skip(self))]
    pub async fn watch(
        &mut self,
        document_id: &DocumentId,
        ids: Option<Vec<ObjId>>,
    ) -> Result<(), PeermergeError> {
        let mut document = self.get_document(document_id).await?;
        document.watch(ids).await;
        Ok(())
    }

    /// Reserve a given object for only local changes, preventing any peers from making changes
    /// to it at the same time before `unreserve_object` has been called. Useful especially when
    /// editing a text to avoid having to update remote changes to the field while typing.
    /// Reserve is not persisted to storage.
    #[instrument(skip(self, obj), fields(obj = obj.as_ref().to_string(), peer_name = self.default_peer_header.name))]
    pub async fn reserve_object<O: AsRef<ObjId>>(
        &mut self,
        document_id: &DocumentId,
        obj: O,
    ) -> Result<(), PeermergeError> {
        let mut document = self.get_document(document_id).await?;
        document.reserve_object(obj.as_ref().clone()).await
    }

    /// Un-reserve a given object previusly reserved with `reserve_object`.
    #[instrument(skip(self, obj), fields(obj = obj.as_ref().to_string(), peer_name = self.default_peer_header.name))]
    pub async fn unreserve_object<O: AsRef<ObjId>>(
        &mut self,
        document_id: &DocumentId,
        obj: O,
    ) -> Result<(), PeermergeError> {
        let mut document = self.get_document(document_id).await?;
        let state_events = document.unreserve_object(obj).await?;
        if !state_events.is_empty() {
            if let Some(state_event_sender) = self.state_event_sender.lock().await.as_mut() {
                send_state_events(state_event_sender, state_events, &self.peermerge_state).await;
            }
        }
        Ok(())
    }

    #[instrument(skip(self), fields(peer_name = self.default_peer_header.name))]
    pub async fn sharing_info(
        &self,
        document_id: &DocumentId,
    ) -> Result<DocumentSharingInfo, PeermergeError> {
        let document = self.get_document(document_id).await?;
        document.sharing_info().await
    }

    #[instrument(skip(self), fields(peer_name = self.default_peer_header.name))]
    pub async fn document_secret(
        &self,
        document_id: &DocumentId,
    ) -> Result<Option<String>, PeermergeError> {
        let document = self.get_document(document_id).await?;
        let document_secret = document.document_secret();
        Ok(document_secret.as_ref().map(encode_document_secret))
    }

    #[instrument(skip(self), fields(peer_name = self.default_peer_header.name))]
    pub async fn reattach_secret(
        &self,
        document_id: &DocumentId,
    ) -> Result<String, PeermergeError> {
        let document = self.get_document(document_id).await?;
        let write_feed_signing_key = document.write_feed_signing_key().await;
        Ok(encode_reattach_secret(
            &self.peer_id,
            &signing_key_to_bytes(&write_feed_signing_key),
        ))
    }

    #[instrument(skip(self), fields(peer_name = self.default_peer_header.name))]
    pub async fn close(&mut self) -> Result<(), PeermergeError> {
        for document_id in get_document_ids(&self.documents).await {
            let mut document = self.get_document(&document_id).await?;
            document.close().await?;
        }
        Ok(())
    }

    // Private
    async fn get_document(
        &self,
        document_id: &DocumentId,
    ) -> Result<Document<T, U>, PeermergeError> {
        get_document(&self.documents, document_id)
            .await
            .ok_or_else(|| PeermergeError::BadArgument {
                context: format!("No document found with given document id: {document_id:02X?}"),
            })
    }

    async fn add_document(
        &mut self,
        document: Document<T, U>,
        parent_id: Option<DocumentId>,
    ) -> DocumentInfo {
        let mut state = self.peermerge_state.lock().await;
        let info = document.info().await;
        self.documents.insert(info.id(), document);
        state.add_document_id_to_state(info.id(), parent_id).await;
        info
    }

    async fn parent_document_info(
        &self,
        parent_id: Option<DocumentId>,
        parent_header: Option<NameDescription>,
    ) -> Result<
        (
            Option<Document<T, U>>,
            Option<(DocumentId, SigningKey, NameDescription)>,
        ),
        PeermergeError,
    > {
        if let Some(parent_id) = parent_id {
            let document = self.get_document(&parent_id).await?;
            let signing_key = document.doc_signature_signing_key().ok_or_else(|| {
                PeermergeError::BadArgument {
                    context: "Can not create a child to parent without write access".to_string(),
                }
            })?;
            let parent_header: NameDescription = if let Some(parent_header) = parent_header {
                parent_header
            } else {
                document
                    .document_header()
                    .await
                    .ok_or_else(|| PeermergeError::BadArgument {
                        context: "Without parent_header the parent document needs \
                                  a header to create a child document"
                            .to_string(),
                    })?
            };
            Ok((
                Some(document),
                Some((parent_id, signing_key, parent_header)),
            ))
        } else {
            Ok((None, None))
        }
    }

    async fn process_new_document_result(
        &self,
        result: NewDocumentResult<T, U>,
        mut parent_document: Option<Document<T, U>>,
    ) -> Result<(Document<T, U>, Option<DocumentId>), PeermergeError> {
        if !result.state_events.is_empty() {
            if let Some(state_event_sender) = self.state_event_sender.lock().await.as_mut() {
                send_state_events(
                    state_event_sender,
                    result.state_events,
                    &self.peermerge_state,
                )
                .await;
            }
        }
        let parent_id: Option<DocumentId> =
            if let Some(child_document_info) = result.child_document_info {
                let mut parent_document = parent_document.take().unwrap();
                let parent_id = parent_document.id();
                let document_secret = result.document.document_secret().unwrap();
                let document_url = &result
                    .document
                    .sharing_info()
                    .await
                    .unwrap()
                    .read_write_document_url;
                parent_document
                    .add_created_child_document(child_document_info, document_url, document_secret)
                    .await?;
                Some(parent_id)
            } else {
                None
            };
        Ok((result.document, parent_id))
    }
}

//////////////////////////////////////////////////////
//
// Memory

impl Peermerge<RandomAccessMemory, FeedMemoryPersistence> {
    pub async fn new_memory(options: PeermergeMemoryOptions) -> Result<Self, PeermergeError> {
        let document_settings = DocumentSettings {
            max_entry_data_size_bytes: options.max_entry_data_size_bytes,
            max_write_feed_length: options.max_write_feed_length,
        };
        let (reattach_secrets, peer_id) = if let Some(reattach_secrets) = options.reattach_secrets {
            let mut secrets: HashMap<DocumentId, SigningKey> = HashMap::new();
            let mut new_peer_id: Option<PeerId> = None;
            for (document_id, reattach_secret) in reattach_secrets {
                let (peer_id, write_feed_key_pair_bytes) =
                    decode_reattach_secret(&reattach_secret)?;
                if let Some(id) = new_peer_id {
                    if peer_id != id {
                        return Err(PeermergeError::BadArgument {
                            context: "Invalid reattach secrets, peer id is not the same"
                                .to_string(),
                        });
                    }
                } else {
                    new_peer_id = Some(peer_id);
                }
                let write_feed_signing_key = signing_key_from_bytes(&write_feed_key_pair_bytes);
                secrets.insert(document_id, write_feed_signing_key);
            }
            (Some(secrets), new_peer_id)
        } else {
            (None, None)
        };
        let wrapper = PeermergeStateWrapper::new_memory(
            &options.default_peer_header,
            document_settings.clone(),
            peer_id,
        )
        .await;
        Ok(Self {
            peer_id: wrapper.state.peer_id,
            default_peer_header: options.default_peer_header,
            prefix: PathBuf::new(),
            peermerge_state: Arc::new(Mutex::new(wrapper)),
            documents: Arc::new(DashMap::new()),
            state_event_sender: Arc::new(Mutex::new(options.state_event_sender)),
            document_settings,
            reattach_secrets,
        })
    }

    pub async fn create_new_document_memory<F, O>(
        &mut self,
        options: CreateNewDocumentMemoryOptions,
        init_cb: F,
        change_id: Option<Vec<u8>>,
    ) -> Result<(DocumentInfo, O), PeermergeError>
    where
        F: FnOnce(&mut Transaction) -> Result<O, AutomergeError>,
    {
        let (parent_document, parent_id_signing_key_and_header) = self
            .parent_document_info(options.parent_id, options.parent_header)
            .await?;
        let (create_result, init_result) = Document::create_new_memory(
            self.peer_id,
            &self.default_peer_header,
            &options.document_type,
            options.document_header,
            options.encrypted,
            parent_id_signing_key_and_header,
            self.document_settings.clone(),
            init_cb,
            change_id,
        )
        .await?;
        let (document, parent_id) = self
            .process_new_document_result(create_result, parent_document)
            .await?;
        Ok((self.add_document(document, parent_id).await, init_result))
    }

    pub async fn attach_document_memory(
        &mut self,
        options: AttachDocumentMemoryOptions,
    ) -> Result<DocumentInfo, PeermergeError> {
        let (parent_document, parent_id_signing_key_and_header) = self
            .parent_document_info(options.parent_id, options.parent_header)
            .await?;
        let document_secret = options
            .document_secret
            .map(|secret| decode_document_secret(&secret))
            .transpose()?;
        let decoded_document_url = decode_doc_url(&options.document_url, &document_secret)?;

        // If reattach secrets have been given, there are conditions to attaching
        if self.reattach_secrets.is_some() {
            if decoded_document_url.static_info.child {
                return Err(PeermergeError::BadArgument {
                    context: "Can not reattach a child document".to_string(),
                });
            }
            if !self.documents.is_empty() {
                return Err(PeermergeError::BadArgument {
                    context: "Can only reattach to an empty peermerge".to_string(),
                });
            }
        }

        let attach_result = Document::attach_memory(
            self.peer_id,
            &self.default_peer_header,
            decoded_document_url,
            self.reattach_secrets.as_mut(),
            parent_id_signing_key_and_header.map(|value| DocumentParent::New {
                parent_id: value.0,
                signing_key: value.1,
                parent_header: value.2,
            }),
            self.document_settings.clone(),
        )
        .await?;
        let (document, parent_id) = self
            .process_new_document_result(attach_result, parent_document)
            .await?;
        Ok(self.add_document(document, parent_id).await)
    }

    #[instrument(skip_all, fields(peer_name = self.default_peer_header.name))]
    pub async fn connect_protocol_memory<T>(
        &mut self,
        protocol: &mut Protocol<T>,
    ) -> Result<(), PeermergeError>
    where
        T: IO,
    {
        let (mut feed_event_sender, feed_event_receiver): (
            UnboundedSender<FeedEvent>,
            UnboundedReceiver<FeedEvent>,
        ) = unbounded();
        if self.state_event_sender.lock().await.is_none() {
            return Err(PeermergeError::BadArgument {
                context: "State event sender must be set before connecting protocol".to_string(),
            });
        };
        let state_event_sender_for_task = self.state_event_sender.clone();
        let documents_for_task = self.documents.clone();
        let peermerge_state_for_task = self.peermerge_state.clone();
        let task_span = tracing::debug_span!("call_on_feed_event_memory").or_current();
        let peer_id = self.peer_id;
        let default_peer_header = self.default_peer_header.clone();
        let document_settings = self.document_settings.clone();
        let reattach_secrets = self.reattach_secrets.clone();

        #[cfg(not(target_arch = "wasm32"))]
        task::spawn(async move {
            let _entered = task_span.enter();
            on_feed_event_memory(
                peer_id,
                default_peer_header,
                document_settings,
                feed_event_receiver,
                state_event_sender_for_task,
                documents_for_task,
                peermerge_state_for_task,
                reattach_secrets,
            )
            .await;
        });
        #[cfg(target_arch = "wasm32")]
        spawn_local(async move {
            let _entered = task_span.enter();
            on_feed_event_memory(
                peer_id,
                default_peer_header,
                document_settings,
                feed_event_receiver,
                state_event_sender_for_task,
                documents_for_task,
                peermerge_state_for_task,
                reattach_secrets,
            )
            .await;
        });

        on_protocol(
            self.peer_id,
            protocol,
            self.documents.clone(),
            &mut feed_event_sender,
        )
        .await?;
        Ok(())
    }
}

#[instrument(level = "debug", skip_all)]
async fn on_feed_event_memory(
    peer_id: PeerId,
    default_peer_header: NameDescription,
    document_settings: DocumentSettings,
    mut feed_event_receiver: UnboundedReceiver<FeedEvent>,
    state_event_sender_mutex: Arc<Mutex<Option<UnboundedSender<StateEvent>>>>,
    mut documents: Arc<DashMap<DocumentId, Document<RandomAccessMemory, FeedMemoryPersistence>>>,
    peermerge_state: Arc<Mutex<PeermergeStateWrapper<RandomAccessMemory>>>,
    mut reattach_secrets: Option<HashMap<DocumentId, SigningKey>>,
) {
    let mut state_event_sender: UnboundedSender<StateEvent> = {
        state_event_sender_mutex
            .lock()
            .await
            .clone()
            .expect("Should always be present")
    };
    while let Some(event) = feed_event_receiver.next().await {
        debug!("Received event {:?}", event);
        // The state event sender might change so that the other side closes
        if state_event_sender.is_closed() {
            if let Some(sender) = state_event_sender_mutex.lock().await.clone() {
                state_event_sender = sender;
            }
        }
        match event.content {
            FeedEventContent::NewFeedsBroadcasted { new_feeds } => {
                let mut document =
                    get_document_by_discovery_key(&documents, &event.doc_discovery_key)
                        .await
                        .unwrap();
                let state_events = document
                    .process_new_feeds_broadcasted_memory(new_feeds)
                    .await;

                if !state_events.is_empty() {
                    send_state_events(&mut state_event_sender, state_events, &peermerge_state)
                        .await;
                }
            }
            FeedEventContent::NewChildDocumentsBroadcasted {
                new_child_documents,
            } => {
                let mut parent_document =
                    get_document_by_discovery_key(&documents, &event.doc_discovery_key)
                        .await
                        .unwrap();
                let parent_id = parent_document.id();
                for mut new_child_document in new_child_documents {
                    if let Some(decoded_document_url) = parent_document
                        .merge_remote_child_document(&mut new_child_document)
                        .await
                        .unwrap()
                    {
                        let document_id = decoded_document_url.static_info.document_id;

                        // It is possible that this child document has multiple parents, and is already
                        // attached by another parent.
                        if !documents.contains_key(&decoded_document_url.static_info.document_id) {
                            let attach_result = Document::attach_memory(
                                peer_id,
                                &default_peer_header,
                                decoded_document_url,
                                reattach_secrets.as_mut(),
                                Some(DocumentParent::Registered {
                                    child_document_info: new_child_document.clone(),
                                    parent_id,
                                }),
                                document_settings.clone(),
                            )
                            .await
                            .unwrap();
                            if !attach_result.state_events.is_empty() {
                                send_state_events(
                                    &mut state_event_sender,
                                    attach_result.state_events,
                                    &peermerge_state,
                                )
                                .await;
                            }
                            documents.insert(document_id, attach_result.document);
                        }
                        {
                            let mut state = peermerge_state.lock().await;
                            state
                                .add_document_id_to_state(document_id, Some(parent_id))
                                .await;
                        }
                        // Finally, set child document to created to parent
                        parent_document
                            .set_child_document_created(&new_child_document)
                            .await
                            .unwrap();
                    }
                }
            }
            FeedEventContent::FeedMaxLengthReached { discovery_key } => {
                let mut document =
                    get_document_by_discovery_key(&documents, &event.doc_discovery_key)
                        .await
                        .unwrap();
                let state_events = document
                    .replace_write_feed_memory(&discovery_key)
                    .await
                    .unwrap();
                send_state_events(&mut state_event_sender, state_events, &peermerge_state).await;
            }
            _ => {
                process_feed_event(
                    event,
                    &mut state_event_sender,
                    &mut documents,
                    &peermerge_state,
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
    pub async fn new_disk(options: PeermergeDiskOptions) -> Result<Self, PeermergeError> {
        let document_settings = DocumentSettings {
            max_entry_data_size_bytes: options.max_entry_data_size_bytes,
            max_write_feed_length: options.max_write_feed_length,
        };
        let wrapper = PeermergeStateWrapper::new_disk(
            &options.default_peer_header,
            &options.data_root_dir,
            document_settings.clone(),
        )
        .await?;
        Ok(Self {
            peer_id: wrapper.state.peer_id,
            default_peer_header: options.default_peer_header,
            prefix: options.data_root_dir.clone(),
            peermerge_state: Arc::new(Mutex::new(wrapper)),
            documents: Arc::new(DashMap::new()),
            state_event_sender: Arc::new(Mutex::new(options.state_event_sender)),
            document_settings,
            reattach_secrets: None,
        })
    }

    pub async fn document_infos_disk(
        data_root_dir: &PathBuf,
    ) -> Result<Option<Vec<DocumentInfo>>, PeermergeError> {
        if let Some(state_wrapper) = PeermergeStateWrapper::open_disk(data_root_dir).await? {
            let mut document_infos: Vec<DocumentInfo> = vec![];
            for document_id_with_parents in &state_wrapper.state.document_ids {
                let postfix = encode_document_id(&document_id_with_parents.document_id);
                let document_data_root_dir = data_root_dir.join(postfix);
                document_infos.push(Document::info_disk(&document_data_root_dir).await?);
            }
            Ok(Some(document_infos))
        } else {
            Ok(None)
        }
    }

    pub async fn open_disk(
        document_secrets: HashMap<DocumentId, String>,
        data_root_dir: &PathBuf,
        mut state_event_sender: Option<UnboundedSender<StateEvent>>,
    ) -> Result<Self, PeermergeError> {
        let state_wrapper = PeermergeStateWrapper::open_disk(data_root_dir)
            .await?
            .expect("Not a valid peermerge directory");
        let state = state_wrapper.state();
        let peer_id = state.peer_id;
        let default_peer_header = state.default_peer_header.clone();
        let document_settings = state.document_settings.clone();
        let documents: DashMap<DocumentId, Document<RandomAccessDisk, FeedDiskPersistence>> =
            DashMap::new();
        let mut state_events: Vec<StateEvent> = vec![];
        for document_id_with_parents in &state_wrapper.state.document_ids {
            let document_id = &document_id_with_parents.document_id;
            let document_secret: Option<DocumentSecret> =
                if let Some(document_secret) = &document_secrets.get(document_id).cloned() {
                    Some(decode_document_secret(document_secret)?)
                } else {
                    None
                };
            let postfix = encode_document_id(document_id);
            let document_data_root_dir = data_root_dir.join(postfix);
            let (document, document_state_events) = Document::open_disk(
                peer_id,
                document_secret,
                &document_data_root_dir,
                document_settings.clone(),
            )
            .await?;
            state_events.extend(document_state_events);
            documents.insert(*document_id, document);
        }
        let documents = Arc::new(documents);
        let peermerge_state = Arc::new(Mutex::new(state_wrapper));
        if let Some(state_event_sender) = state_event_sender.as_mut() {
            send_state_events(state_event_sender, state_events, &peermerge_state).await;
        }

        Ok(Self {
            peer_id,
            default_peer_header,
            prefix: data_root_dir.clone(),
            peermerge_state,
            documents,
            state_event_sender: Arc::new(Mutex::new(state_event_sender)),
            document_settings,
            reattach_secrets: None,
        })
    }

    pub async fn create_new_document_disk<F, O>(
        &mut self,
        options: CreateNewDocumentDiskOptions,
        init_cb: F,
        change_id: Option<Vec<u8>>,
    ) -> Result<(DocumentInfo, O), PeermergeError>
    where
        F: FnOnce(&mut Transaction) -> Result<O, AutomergeError>,
    {
        let (parent_document, parent_id_signing_key_and_header) = self
            .parent_document_info(options.parent_id, options.parent_header)
            .await?;
        let (create_result, init_result) = Document::create_new_disk(
            self.peer_id,
            &self.default_peer_header,
            &options.document_type,
            options.document_header,
            options.encrypted,
            parent_id_signing_key_and_header,
            self.document_settings.clone(),
            init_cb,
            change_id,
            &self.prefix,
        )
        .await?;
        let (document, parent_id) = self
            .process_new_document_result(create_result, parent_document)
            .await?;
        Ok((self.add_document(document, parent_id).await, init_result))
    }

    pub async fn attach_document_disk(
        &mut self,
        options: AttachDocumentDiskOptions,
    ) -> Result<DocumentInfo, PeermergeError> {
        let (parent_document, parent_id_signing_key_and_header) = self
            .parent_document_info(options.parent_id, options.parent_header)
            .await?;
        let document_secret = options
            .document_secret
            .map(|secret| decode_document_secret(&secret))
            .transpose()?;
        let decoded_document_url = decode_doc_url(&options.document_url, &document_secret)?;
        let attach_result = Document::attach_disk(
            self.peer_id,
            &self.default_peer_header,
            decoded_document_url,
            parent_id_signing_key_and_header.map(|value| DocumentParent::New {
                parent_id: value.0,
                signing_key: value.1,
                parent_header: value.2,
            }),
            &self.prefix,
            self.document_settings.clone(),
        )
        .await?;
        let (document, parent_id) = self
            .process_new_document_result(attach_result, parent_document)
            .await?;
        Ok(self.add_document(document, parent_id).await)
    }

    #[instrument(skip_all, fields(name = self.default_peer_header.name))]
    pub async fn connect_protocol_disk<T>(
        &mut self,
        protocol: &mut Protocol<T>,
    ) -> Result<(), PeermergeError>
    where
        T: IO,
    {
        let (mut feed_event_sender, feed_event_receiver): (
            UnboundedSender<FeedEvent>,
            UnboundedReceiver<FeedEvent>,
        ) = unbounded();
        if self.state_event_sender.lock().await.is_none() {
            return Err(PeermergeError::BadArgument {
                context: "State event sender must be set before connecting protocol".to_string(),
            });
        };
        let state_event_sender_for_task = self.state_event_sender.clone();
        let documents_for_task = self.documents.clone();
        let peemerge_state_for_task = self.peermerge_state.clone();
        let peer_id = self.peer_id;
        let default_peer_header = self.default_peer_header.clone();
        let document_settings = self.document_settings.clone();
        let prefix = self.prefix.clone();
        let task_span = tracing::debug_span!("call_on_feed_event_disk").or_current();
        task::spawn(async move {
            let _entered = task_span.enter();
            on_feed_event_disk(
                peer_id,
                default_peer_header,
                document_settings,
                &prefix,
                feed_event_receiver,
                state_event_sender_for_task,
                documents_for_task,
                peemerge_state_for_task,
            )
            .await;
        });

        on_protocol(
            self.peer_id,
            protocol,
            self.documents.clone(),
            &mut feed_event_sender,
        )
        .await?;
        Ok(())
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[instrument(level = "debug", skip_all)]
async fn on_feed_event_disk(
    peer_id: PeerId,
    default_peer_header: NameDescription,
    document_settings: DocumentSettings,
    prefix: &PathBuf,
    mut feed_event_receiver: UnboundedReceiver<FeedEvent>,
    state_event_sender_mutex: Arc<Mutex<Option<UnboundedSender<StateEvent>>>>,
    mut documents: Arc<DashMap<DocumentId, Document<RandomAccessDisk, FeedDiskPersistence>>>,
    peermerge_state: Arc<Mutex<PeermergeStateWrapper<RandomAccessDisk>>>,
) {
    let mut state_event_sender: UnboundedSender<StateEvent> = {
        state_event_sender_mutex
            .lock()
            .await
            .clone()
            .expect("Should always be present")
    };
    while let Some(event) = feed_event_receiver.next().await {
        // The state event sender might change so that the other side closes
        if state_event_sender.is_closed() {
            if let Some(sender) = state_event_sender_mutex.lock().await.clone() {
                state_event_sender = sender;
            }
        }
        debug!("Received event {:?}", event);
        match event.content {
            FeedEventContent::NewFeedsBroadcasted { new_feeds } => {
                let mut document =
                    get_document_by_discovery_key(&documents, &event.doc_discovery_key)
                        .await
                        .unwrap();
                let state_events = document
                    .process_new_feeds_broadcasted_disk(new_feeds)
                    .await
                    .unwrap();
                if !state_events.is_empty() {
                    send_state_events(&mut state_event_sender, state_events, &peermerge_state)
                        .await;
                }
            }
            FeedEventContent::NewChildDocumentsBroadcasted {
                new_child_documents,
            } => {
                let mut parent_document =
                    get_document_by_discovery_key(&documents, &event.doc_discovery_key)
                        .await
                        .unwrap();
                let parent_id = parent_document.id();
                for mut new_child_document in new_child_documents {
                    if let Some(decoded_document_url) = parent_document
                        .merge_remote_child_document(&mut new_child_document)
                        .await
                        .unwrap()
                    {
                        let document_id = decoded_document_url.static_info.document_id;
                        // It is possible that this child document has multiple parents, and is already
                        // attached by another parent.
                        if !documents.contains_key(&document_id) {
                            let attach_result = Document::attach_disk(
                                peer_id,
                                &default_peer_header,
                                decoded_document_url,
                                Some(DocumentParent::Registered {
                                    child_document_info: new_child_document.clone(),
                                    parent_id,
                                }),
                                prefix,
                                document_settings.clone(),
                            )
                            .await
                            .unwrap();

                            if !attach_result.state_events.is_empty() {
                                send_state_events(
                                    &mut state_event_sender,
                                    attach_result.state_events,
                                    &peermerge_state,
                                )
                                .await;
                            }
                            documents.insert(document_id, attach_result.document);
                        }
                        {
                            let mut state = peermerge_state.lock().await;
                            state
                                .add_document_id_to_state(document_id, Some(parent_id))
                                .await;
                        }

                        // Finally, set child document to created to parent
                        parent_document
                            .set_child_document_created(&new_child_document)
                            .await
                            .unwrap();
                    }
                }
            }
            FeedEventContent::FeedMaxLengthReached { discovery_key } => {
                let mut document =
                    get_document_by_discovery_key(&documents, &event.doc_discovery_key)
                        .await
                        .unwrap();
                let state_events = document
                    .replace_write_feed_disk(&discovery_key)
                    .await
                    .unwrap();
                send_state_events(&mut state_event_sender, state_events, &peermerge_state).await;
            }
            _ => {
                process_feed_event(
                    event,
                    &mut state_event_sender,
                    &mut documents,
                    &peermerge_state,
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
//

async fn send_state_events<T>(
    state_event_sender: &mut UnboundedSender<StateEvent>,
    state_events: Vec<StateEvent>,
    peermerge_state: &Arc<Mutex<PeermergeStateWrapper<T>>>,
) where
    T: RandomAccess + Debug + Send + 'static,
{
    if !state_event_sender.is_closed() {
        for mut state_event in state_events {
            post_process_state_event(&mut state_event, peermerge_state).await;
            state_event_sender.unbounded_send(state_event).unwrap();
        }
    }
}

#[instrument(level = "debug", skip_all)]
async fn process_feed_event<T, U>(
    event: FeedEvent,
    state_event_sender: &mut UnboundedSender<StateEvent>,
    documents: &mut Arc<DashMap<DocumentId, Document<T, U>>>,
    peermerge_state: &Arc<Mutex<PeermergeStateWrapper<T>>>,
) where
    T: RandomAccess + Debug + Send + 'static,
    U: FeedPersistence,
{
    match event.content {
        FeedEventContent::NewFeedsBroadcasted { .. } => {
            unreachable!("Implemented by concrete type")
        }
        FeedEventContent::NewChildDocumentsBroadcasted { .. } => {
            unreachable!("Implemented by concrete type")
        }
        FeedEventContent::FeedMaxLengthReached { .. } => {
            unreachable!("Implemented by concrete type")
        }
        FeedEventContent::FeedDisconnected { .. } => {
            // This is an FYI message, just continue for now
        }
        FeedEventContent::FeedVerified {
            peer_id,
            discovery_key,
            verified,
        } => {
            let document = get_document_by_discovery_key(documents, &event.doc_discovery_key)
                .await
                .unwrap();
            if verified {
                document.set_feed_verified(&discovery_key, &peer_id).await;
            } else {
                unimplemented!("TODO: Invalid feed deletion");
            }
        }
        FeedEventContent::RemoteFeedSynced {
            peer_id,
            discovery_key,
            contiguous_length,
        } => {
            let document = get_document_by_discovery_key(documents, &event.doc_discovery_key)
                .await
                .unwrap();
            let state_events = document
                .process_remote_feed_synced(peer_id, discovery_key, contiguous_length)
                .await;
            send_state_events(state_event_sender, state_events, peermerge_state).await;
        }
        FeedEventContent::FeedSynced {
            peer_id,
            discovery_key,
            contiguous_length,
        } => {
            let mut document = get_document_by_discovery_key(documents, &event.doc_discovery_key)
                .await
                .unwrap();
            let state_events = document
                .process_feed_synced(peer_id, discovery_key, contiguous_length)
                .await;
            send_state_events(state_event_sender, state_events, peermerge_state).await;
        }
    }
}

async fn post_process_state_event<T>(
    state_event: &mut StateEvent,
    peermerge_state: &Arc<Mutex<PeermergeStateWrapper<T>>>,
) where
    T: RandomAccess + Debug + Send + 'static,
{
    if let StateEventContent::DocumentInitialized {
        child,
        ref mut parent_document_ids,
        ..
    } = state_event.content
    {
        if child && parent_document_ids.is_empty() {
            // Parents are saved in peermerge state
            let peermerge_state = peermerge_state.lock().await;
            parent_document_ids.extend(
                peermerge_state
                    .state
                    .document_ids
                    .iter()
                    .find(|id_with_parents| state_event.document_id == id_with_parents.document_id)
                    .unwrap()
                    .parent_document_ids
                    .clone(),
            );
        }
    }
}
