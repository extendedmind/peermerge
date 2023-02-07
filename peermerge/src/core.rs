use automerge::{ObjId, ObjType, Patch, Prop, ReadDoc, ScalarValue, Value};
use dashmap::DashMap;
use futures::channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use futures::{AsyncRead, AsyncWrite, StreamExt};
use hypercore_protocol::hypercore::compact_encoding::{CompactEncoding, State};
#[cfg(not(target_arch = "wasm32"))]
use random_access_disk::RandomAccessDisk;
use random_access_memory::RandomAccessMemory;
use random_access_storage::RandomAccess;
use std::sync::Arc;
use std::{fmt::Debug, path::PathBuf};
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

use crate::automerge::{
    apply_entries_autocommit, init_doc_from_data, init_doc_from_entries, put_scalar_autocommit,
    splice_text, AutomergeDoc, UnappliedEntries,
};
use crate::common::cipher::{doc_url_to_public_key, keys_to_doc_url};
use crate::common::keys::{discovery_key_from_public_key, generate_keys, Keypair};
use crate::common::{PeerEvent, PeerEventContent, StateEventContent};
use crate::feed::on_protocol;
#[cfg(not(target_arch = "wasm32"))]
use crate::feed::{create_new_read_disk_feed, create_new_write_disk_feed, open_disk_feed};
#[cfg(not(target_arch = "wasm32"))]
use crate::FeedDiskPersistence;
use crate::{
    automerge::{init_doc_with_root_scalars, put_object_autocommit},
    common::{
        entry::Entry,
        state::{DocContent, DocCursor},
        storage::DocStateWrapper,
    },
    feed::{create_new_read_memory_feed, create_new_write_memory_feed, Feed, Protocol},
    FeedMemoryPersistence, FeedPersistence, StateEvent,
};

/// Peermerge is the main abstraction.
#[derive(derivative::Derivative)]
#[derivative(Clone(bound = ""))]
#[derive(Debug)]
pub struct Peermerge<T, U>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send,
    U: FeedPersistence,
{
    feeds: Arc<DashMap<[u8; 32], Arc<Mutex<Feed<U>>>>>,
    doc_state: Arc<Mutex<DocStateWrapper<T>>>,
    state_event_sender: Arc<Mutex<Option<UnboundedSender<StateEvent>>>>,
    prefix: PathBuf,
    name: String,
    proxy: bool,
    doc_discovery_key: [u8; 32],
    doc_url: String,
    encrypted: bool,
    encryption_key: Option<Vec<u8>>,
}

impl<T, U> Peermerge<T, U>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send + 'static,
    U: FeedPersistence,
{
    pub fn name(&self) -> String {
        self.name.clone()
    }

    #[instrument(skip(self))]
    pub async fn watch(&mut self, ids: Vec<ObjId>) {
        if self.proxy {
            panic!("Can not watch on a proxy");
        }
        let mut doc_state = self.doc_state.lock().await;
        doc_state.watch(ids);
    }

    #[instrument(skip(self, obj, prop), fields(obj = obj.as_ref().to_string(), peer_name = self.name))]
    pub async fn get<O: AsRef<ObjId>, P: Into<Prop>>(
        &self,
        obj: O,
        prop: P,
    ) -> anyhow::Result<Option<(Value, ObjId)>> {
        if self.proxy {
            panic!("Can not get document values on a proxy");
        }
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

    #[instrument(skip(self, obj), fields(obj = obj.as_ref().to_string(), peer_name = self.name))]
    pub async fn realize_text<O: AsRef<ObjId>>(&self, obj: O) -> anyhow::Result<Option<String>> {
        if self.proxy {
            panic!("Can not realize text on a proxy");
        }
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

    #[instrument(skip(self, obj, prop), fields(obj = obj.as_ref().to_string(), peer_name = self.name))]
    pub async fn put_object<O: AsRef<ObjId>, P: Into<Prop>>(
        &mut self,
        obj: O,
        prop: P,
        object: ObjType,
    ) -> anyhow::Result<ObjId> {
        if self.proxy {
            panic!("Can not put object on a proxy");
        }
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
                let write_feed = self.feeds.get_mut(&write_discovery_key).unwrap();
                let mut write_feed = write_feed.lock().await;
                write_feed.append(&serialize_entry(&entry)).await?
            };
            doc_state.set_cursor(&write_discovery_key, length).await;
            id
        };
        {
            self.notify_of_document_changes().await;
        }
        Ok(id)
    }

    #[instrument(skip(self, obj, prop, value), fields(obj = obj.as_ref().to_string(), peer_name = self.name))]
    pub async fn put_scalar<O: AsRef<ObjId>, P: Into<Prop>, V: Into<ScalarValue>>(
        &mut self,
        obj: O,
        prop: P,
        value: V,
    ) -> anyhow::Result<()> {
        if self.proxy {
            panic!("Can not put scalar on a proxy");
        }
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
                let write_feed = self.feeds.get_mut(&write_discovery_key).unwrap();
                let mut write_feed = write_feed.lock().await;
                write_feed.append(&serialize_entry(&entry)).await?
            };
            doc_state.set_cursor(&write_discovery_key, length).await;
        };
        {
            self.notify_of_document_changes().await;
        }
        Ok(())
    }

    #[instrument(skip(self, obj), fields(obj = obj.as_ref().to_string(), peer_name = self.name))]
    pub async fn splice_text<O: AsRef<ObjId>>(
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
                let write_feed_wrapper = self.feeds.get_mut(&write_discovery_key).unwrap();
                let mut write_feed = write_feed_wrapper.lock().await;
                write_feed.append(&serialize_entry(&entry)).await?
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
        if self.proxy {
            panic!("Can not cork a proxy");
        }
        let doc_state = self.doc_state.lock().await;
        let write_discovery_key = doc_state.write_discovery_key();
        let write_feed_wrapper = self.feeds.get_mut(&write_discovery_key).unwrap();
        let mut write_feed = write_feed_wrapper.lock().await;
        write_feed.cork();
    }

    #[instrument(skip(self))]
    pub async fn uncork(&mut self) -> anyhow::Result<()> {
        if self.proxy {
            panic!("Can not uncork a proxy");
        }
        let doc_state = self.doc_state.lock().await;
        let write_discovery_key = doc_state.write_discovery_key();
        let write_feed_wrapper = self.feeds.get_mut(&write_discovery_key).unwrap();
        let mut write_feed = write_feed_wrapper.lock().await;
        write_feed.uncork().await?;
        Ok(())
    }

    #[instrument(skip(self))]
    pub fn doc_url(&self) -> String {
        self.doc_url.clone()
    }

    #[instrument(skip(self))]
    pub fn encryption_key(&self) -> Option<Vec<u8>> {
        if self.proxy {
            panic!("A proxy does not store the encryption key");
        }
        self.encryption_key.clone()
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
                            .unbounded_send(StateEvent::new(
                                self.doc_discovery_key.clone(),
                                StateEventContent::DocumentChanged(patches),
                            ))
                            .unwrap();
                    }
                }
            }
        }
    }
}

//////////////////////////////////////////////////////
//
// RandomAccessMemory

impl Peermerge<RandomAccessMemory, FeedMemoryPersistence> {
    pub async fn create_new_memory<P: Into<Prop>, V: Into<ScalarValue>>(
        name: &str,
        root_scalars: Vec<(P, V)>,
        encrypted: bool,
    ) -> Self {
        let result = prepare_create(name, root_scalars).await;

        // Create the memory feed
        let (length, feed, generated_encryption_key) = create_new_write_memory_feed(
            result.key_pair,
            serialize_entry(&Entry::new_init_doc(name, result.data.clone())),
            encrypted,
            &None,
        )
        .await;
        let doc_url = keys_to_doc_url(&result.doc_public_key, &generated_encryption_key);
        let content = DocContent::new(
            result.data,
            vec![DocCursor::new(result.discovery_key.clone(), length)],
            result.doc,
        );
        Self::new_memory(
            Some((
                result.doc_public_key.clone(),
                result.discovery_key.clone(),
                feed,
            )),
            vec![],
            Some(content),
            result.discovery_key,
            name,
            false,
            &doc_url,
            encrypted,
            generated_encryption_key,
        )
        .await
    }

    pub async fn attach_writer_memory(
        name: &str,
        doc_url: &str,
        encryption_key: &Option<Vec<u8>>,
    ) -> Self {
        let proxy = false;

        // Process keys from doc URL
        let (doc_public_key, encrypted) = doc_url_to_public_key(doc_url, &encryption_key);
        if encrypted && encryption_key.is_none() {
            panic!("Can not attach a peer to an encrypted peermerge without an encryption key");
        }
        let doc_discovery_key = discovery_key_from_public_key(&doc_public_key);

        // Create the doc feed
        let (_, doc_feed) =
            create_new_read_memory_feed(&doc_public_key, proxy, encrypted, encryption_key).await;

        // Create the write feed
        let (write_key_pair, write_discovery_key) = generate_keys();
        let write_public_key = *write_key_pair.public.as_bytes();
        let (_, write_feed, _) = create_new_write_memory_feed(
            write_key_pair,
            serialize_entry(&Entry::new_init_peer(name, doc_discovery_key)),
            encrypted,
            &encryption_key,
        )
        .await;

        Self::new_memory(
            Some((write_public_key, write_discovery_key, write_feed)),
            vec![(doc_public_key.clone(), doc_discovery_key.clone(), doc_feed)],
            None,
            doc_discovery_key,
            name,
            proxy,
            doc_url,
            encrypted,
            encryption_key.clone(),
        )
        .await
    }

    pub async fn attach_proxy_memory(name: &str, doc_url: &str) -> Self {
        let proxy = true;

        // Process keys from doc URL
        let (doc_public_key, encrypted) = doc_url_to_public_key(doc_url, &None);
        let doc_discovery_key = discovery_key_from_public_key(&doc_public_key);

        // Create the doc feed
        let (_, doc_feed) =
            create_new_read_memory_feed(&doc_public_key, proxy, encrypted, &None).await;

        Self::new_memory(
            None,
            vec![(doc_public_key.clone(), doc_discovery_key.clone(), doc_feed)],
            None,
            doc_discovery_key,
            name,
            proxy,
            doc_url,
            encrypted,
            None,
        )
        .await
    }

    #[instrument(skip_all, fields(peer_name = self.name))]
    pub async fn connect_protocol_memory<IO>(
        &mut self,
        protocol: &mut Protocol<IO>,
        state_event_sender: &mut UnboundedSender<StateEvent>,
    ) -> anyhow::Result<()>
    where
        IO: AsyncWrite + AsyncRead + Send + Unpin + 'static,
    {
        // First let's drain any patches that are not yet sent out, and push them out. These can
        // be created by scalar values inserted with create_doc_memory or other appending calls
        // executed before this call.
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
        let discovery_key_for_task = self.doc_discovery_key.clone();
        let feeds_for_task = self.feeds.clone();
        let name_for_task = self.name.clone();
        let task_span = tracing::debug_span!("call_on_peer_event_memory").or_current();
        let encryption_key_for_task = self.encryption_key.clone();
        let proxy = self.proxy;
        #[cfg(not(target_arch = "wasm32"))]
        task::spawn(async move {
            let _entered = task_span.enter();
            on_peer_event_memory(
                &discovery_key_for_task,
                peer_event_receiver,
                state_event_sender_for_task,
                doc_state,
                feeds_for_task,
                &name_for_task,
                proxy,
                &encryption_key_for_task,
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
                feeds_for_task,
                &name_for_task,
                proxy,
                &encryption_key_for_task,
            )
            .await;
        });

        on_protocol(
            protocol,
            self.doc_state.clone(),
            self.feeds.clone(),
            &mut peer_event_sender,
        )
        .await?;
        Ok(())
    }

    async fn new_memory(
        write_feed: Option<([u8; 32], [u8; 32], Feed<FeedMemoryPersistence>)>,
        peer_feeds: Vec<([u8; 32], [u8; 32], Feed<FeedMemoryPersistence>)>,
        content: Option<DocContent>,
        doc_discovery_key: [u8; 32],
        name: &str,
        proxy: bool,
        doc_url: &str,
        encrypted: bool,
        encryption_key: Option<Vec<u8>>,
    ) -> Self {
        let feeds: DashMap<[u8; 32], Arc<Mutex<Feed<FeedMemoryPersistence>>>> = DashMap::new();
        let write_public_key =
            if let Some((write_public_key, write_discovery_key, write_feed)) = write_feed {
                feeds.insert(write_discovery_key, Arc::new(Mutex::new(write_feed)));
                Some(write_public_key)
            } else {
                None
            };
        let mut peer_public_keys = vec![];
        for (peer_public_key, peer_discovery_key, peer_feed) in peer_feeds {
            feeds.insert(peer_discovery_key, Arc::new(Mutex::new(peer_feed)));
            peer_public_keys.push(peer_public_key);
        }
        let doc_state = DocStateWrapper::new_memory(
            doc_url,
            name,
            proxy,
            write_public_key,
            peer_public_keys,
            content,
        )
        .await;
        Self {
            feeds: Arc::new(feeds),
            doc_state: Arc::new(Mutex::new(doc_state)),
            state_event_sender: Arc::new(Mutex::new(None)),
            prefix: PathBuf::new(),
            doc_discovery_key,
            doc_url: doc_url.to_string(),
            name: name.to_string(),
            proxy,
            encrypted,
            encryption_key,
        }
    }
}

#[instrument(level = "debug", skip_all)]
async fn on_peer_event_memory(
    doc_discovery_key: &[u8; 32],
    mut peer_event_receiver: UnboundedReceiver<PeerEvent>,
    mut state_event_sender: UnboundedSender<StateEvent>,
    mut doc_state: Arc<Mutex<DocStateWrapper<RandomAccessMemory>>>,
    mut feeds: Arc<DashMap<[u8; 32], Arc<Mutex<Feed<FeedMemoryPersistence>>>>>,
    name: &str,
    proxy: bool,
    encryption_key: &Option<Vec<u8>>,
) {
    while let Some(event) = peer_event_receiver.next().await {
        debug!("Received event {:?}", event);
        match event.content {
            PeerEventContent::NewPeersBroadcasted(public_keys) => {
                let changed = {
                    let mut doc_state = doc_state.lock().await;
                    doc_state
                        .add_peer_public_keys_to_state(public_keys.clone())
                        .await
                };
                if changed {
                    {
                        // Create and insert all new feeds
                        create_and_insert_read_memory_feeds(
                            public_keys.clone(),
                            feeds.clone(),
                            proxy,
                            encryption_key,
                        )
                        .await;
                    }
                    {
                        notify_new_peers_created(&doc_discovery_key, &mut feeds, public_keys).await;
                    }
                }
            }
            _ => {
                process_peer_event(
                    event,
                    doc_discovery_key,
                    &mut state_event_sender,
                    &mut doc_state,
                    &mut feeds,
                    name,
                    proxy,
                )
                .await
            }
        }
    }
    debug!("Exiting");
}

async fn create_and_insert_read_memory_feeds(
    public_keys: Vec<[u8; 32]>,
    feeds: Arc<DashMap<[u8; 32], Arc<Mutex<Feed<FeedMemoryPersistence>>>>>,
    proxy: bool,
    encryption_key: &Option<Vec<u8>>,
) {
    for public_key in public_keys {
        let discovery_key = discovery_key_from_public_key(&public_key);
        // Make sure to insert only once even if two protocols notice the same new
        // feed at the same time using the entry API.

        // There is a deadlock possibility with entry(), so we need to loop and yield
        let mut entry_found = false;
        while !entry_found {
            if let Some(entry) = feeds.try_entry(discovery_key.clone()) {
                match entry {
                    dashmap::mapref::entry::Entry::Occupied(_) => {
                        debug!("Concurrent creating of feeds noticed, continuing.");
                    }
                    dashmap::mapref::entry::Entry::Vacant(vacant) => {
                        let (_, feed) = create_new_read_memory_feed(
                            &public_key,
                            proxy,
                            encryption_key.is_some(),
                            encryption_key,
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

//////////////////////////////////////////////////////
//
// RandomAccessDisk

#[cfg(not(target_arch = "wasm32"))]
impl Peermerge<RandomAccessDisk, FeedDiskPersistence> {
    pub async fn create_new_disk<P: Into<Prop>, V: Into<ScalarValue>>(
        name: &str,
        root_scalars: Vec<(P, V)>,
        encrypted: bool,
        data_root_dir: &PathBuf,
    ) -> Self {
        let result = prepare_create(name, root_scalars).await;

        // Create the disk feed
        let (length, feed, encryption_key) = create_new_write_disk_feed(
            &data_root_dir,
            result.key_pair,
            &result.discovery_key,
            serialize_entry(&Entry::new_init_doc(name, result.data.clone())),
            encrypted,
            &None,
        )
        .await;
        let doc_url = keys_to_doc_url(&result.doc_public_key, &encryption_key);
        let content = DocContent::new(
            result.data,
            vec![DocCursor::new(result.discovery_key.clone(), length)],
            result.doc,
        );

        Self::new_disk(
            Some((
                result.doc_public_key.clone(),
                result.discovery_key.clone(),
                feed,
            )),
            vec![],
            Some(content),
            result.discovery_key,
            name,
            false,
            &doc_url,
            encrypted,
            encryption_key,
            data_root_dir,
        )
        .await
    }

    pub async fn attach_writer_disk(
        name: &str,
        doc_url: &str,
        encryption_key: &Option<Vec<u8>>,
        data_root_dir: &PathBuf,
    ) -> Self {
        let proxy = false;

        // Process keys from doc URL
        let (doc_public_key, encrypted) = doc_url_to_public_key(doc_url, &encryption_key);
        if encrypted && encryption_key.is_none() {
            panic!("Can not attach a peer to an encrypted peermerge without an encryption key");
        }
        let doc_discovery_key = discovery_key_from_public_key(&doc_public_key);

        // Create/open the doc feed
        let (_, doc_feed) = create_new_read_disk_feed(
            &data_root_dir,
            &doc_public_key,
            &doc_discovery_key,
            proxy,
            encrypted,
            encryption_key,
        )
        .await;

        // Create the write feed
        let (write_key_pair, write_discovery_key) = generate_keys();
        let write_public_key = *write_key_pair.public.as_bytes();
        let (_, write_feed, encryption_key) = create_new_write_disk_feed(
            &data_root_dir,
            write_key_pair,
            &write_discovery_key,
            serialize_entry(&Entry::new_init_peer(name, doc_discovery_key)),
            encrypted,
            encryption_key,
        )
        .await;

        Self::new_disk(
            Some((write_public_key, write_discovery_key, write_feed)),
            vec![(doc_public_key.clone(), doc_discovery_key.clone(), doc_feed)],
            None,
            doc_discovery_key,
            name,
            proxy,
            doc_url,
            encrypted,
            encryption_key,
            data_root_dir,
        )
        .await
    }

    pub async fn attach_proxy_disk(name: &str, doc_url: &str, data_root_dir: &PathBuf) -> Self {
        let proxy = true;

        // Process keys from doc URL
        let (doc_public_key, encrypted) = doc_url_to_public_key(doc_url, &None);
        let doc_discovery_key = discovery_key_from_public_key(&doc_public_key);

        // Create/open the doc feed
        let (_, doc_feed) = create_new_read_disk_feed(
            &data_root_dir,
            &doc_public_key,
            &doc_discovery_key,
            proxy,
            encrypted,
            &None,
        )
        .await;

        Self::new_disk(
            None,
            vec![(doc_public_key.clone(), doc_discovery_key.clone(), doc_feed)],
            None,
            doc_discovery_key,
            name,
            proxy,
            doc_url,
            encrypted,
            None,
            data_root_dir,
        )
        .await
    }

    pub async fn open_disk(encryption_key: &Option<Vec<u8>>, data_root_dir: &PathBuf) -> Self {
        let mut doc_state_wrapper = DocStateWrapper::open_disk(data_root_dir).await;
        let state = doc_state_wrapper.state();
        let encrypted = state.encrypted;
        let proxy = state.proxy;
        let doc_discovery_key = state.doc_discovery_key.clone();
        let doc_url = state.doc_url.clone();
        let name = state.name.clone();

        let feeds: DashMap<[u8; 32], Arc<Mutex<Feed<FeedDiskPersistence>>>> = DashMap::new();

        let mut discovery_keys: Vec<[u8; 32]> = vec![];
        // Open doc feed
        let (_, doc_feed) = open_disk_feed(
            &data_root_dir,
            &state.doc_discovery_key,
            proxy,
            encrypted,
            encryption_key,
        )
        .await;
        feeds.insert(state.doc_discovery_key, Arc::new(Mutex::new(doc_feed)));
        discovery_keys.push(state.doc_discovery_key.clone());

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
            discovery_keys.push(peer.discovery_key.clone());
        }

        // Open write feed, if any
        let feeds = if let Some(write_public_key) = state.write_public_key {
            let write_discovery_key = discovery_key_from_public_key(&write_public_key);
            if write_public_key != state.doc_public_key {
                let (_, write_feed) = open_disk_feed(
                    &data_root_dir,
                    &write_discovery_key,
                    proxy,
                    encrypted,
                    encryption_key,
                )
                .await;
                feeds.insert(write_discovery_key, Arc::new(Mutex::new(write_feed)));
                discovery_keys.push(write_discovery_key.clone());
            }

            let feeds = Arc::new(feeds);

            // Initialize doc, fill unapplied changes and possibly save state if it had been left
            // unsaved
            if let Some((content, unapplied_entries)) =
                doc_state_wrapper.content_and_unapplied_entries_mut()
            {
                let doc = init_doc_from_data(&name, &write_discovery_key, &content.data);
                content.doc = Some(doc);

                let (changed, new_peer_names) =
                    update_content(discovery_keys, content, feeds.clone(), unapplied_entries)
                        .await
                        .unwrap();
                if changed {
                    doc_state_wrapper
                        .persist_content_and_new_peer_names(new_peer_names)
                        .await;
                }
            }
            feeds
        } else {
            Arc::new(feeds)
        };

        // Create Peermerge
        Self {
            feeds,
            doc_state: Arc::new(Mutex::new(doc_state_wrapper)),
            state_event_sender: Arc::new(Mutex::new(None)),
            prefix: data_root_dir.clone(),
            doc_discovery_key,
            doc_url,
            name,
            proxy,
            encrypted,
            encryption_key: encryption_key.clone(),
        }
    }

    #[instrument(skip_all, fields(peer_name = self.name))]
    pub async fn connect_protocol_disk<IO>(
        &mut self,
        protocol: &mut Protocol<IO>,
        state_event_sender: &mut UnboundedSender<StateEvent>,
    ) -> anyhow::Result<()>
    where
        IO: AsyncWrite + AsyncRead + Send + Unpin + 'static,
    {
        // First let's drain any patches that are not yet sent out, and push them out. These can
        // be created by scalar values inserted with create_doc_disk or other appending calls
        // executed before this call.
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
        let discovery_key_for_task = self.doc_discovery_key.clone();
        let feeds_for_task = self.feeds.clone();
        let name_for_task = self.name.clone();
        let data_root_dir = self.prefix.clone();
        let task_span = tracing::debug_span!("call_on_peer_event_memory").or_current();
        let encryption_key_for_task = self.encryption_key.clone();
        let proxy = self.proxy;
        #[cfg(not(target_arch = "wasm32"))]
        task::spawn(async move {
            let _entered = task_span.enter();
            on_peer_event_disk(
                &discovery_key_for_task,
                peer_event_receiver,
                state_event_sender_for_task,
                doc_state,
                feeds_for_task,
                &name_for_task,
                proxy,
                &encryption_key_for_task,
                &data_root_dir,
            )
            .await;
        });
        #[cfg(target_arch = "wasm32")]
        spawn_local(async move {
            let _entered = task_span.enter();
            on_peer_event_disk(
                &discovery_key_for_task,
                peer_event_receiver,
                state_event_sender_for_task,
                doc_state,
                feeds_for_task,
                &name_for_task,
                proxy,
                &encryption_key_for_task,
                &data_root_dir,
            )
            .await;
        });

        on_protocol(
            protocol,
            self.doc_state.clone(),
            self.feeds.clone(),
            &mut peer_event_sender,
        )
        .await?;
        Ok(())
    }

    async fn new_disk(
        write_feed: Option<([u8; 32], [u8; 32], Feed<FeedDiskPersistence>)>,
        peer_feeds: Vec<([u8; 32], [u8; 32], Feed<FeedDiskPersistence>)>,
        content: Option<DocContent>,
        doc_discovery_key: [u8; 32],
        name: &str,
        proxy: bool,
        doc_url: &str,
        encrypted: bool,
        encryption_key: Option<Vec<u8>>,
        data_root_dir: &PathBuf,
    ) -> Self {
        let feeds: DashMap<[u8; 32], Arc<Mutex<Feed<FeedDiskPersistence>>>> = DashMap::new();
        let write_public_key =
            if let Some((write_public_key, write_discovery_key, write_feed)) = write_feed {
                feeds.insert(write_discovery_key, Arc::new(Mutex::new(write_feed)));
                Some(write_public_key)
            } else {
                None
            };
        let mut peer_public_keys = vec![];
        for (peer_public_key, peer_discovery_key, peer_feed) in peer_feeds {
            feeds.insert(peer_discovery_key, Arc::new(Mutex::new(peer_feed)));
            peer_public_keys.push(peer_public_key);
        }
        let doc_state = DocStateWrapper::new_disk(
            doc_url,
            name,
            proxy,
            write_public_key,
            peer_public_keys,
            content,
            &data_root_dir,
        )
        .await;
        Self {
            feeds: Arc::new(feeds),
            doc_state: Arc::new(Mutex::new(doc_state)),
            state_event_sender: Arc::new(Mutex::new(None)),
            prefix: data_root_dir.clone(),
            doc_discovery_key,
            doc_url: doc_url.to_string(),
            name: name.to_string(),
            proxy,
            encrypted,
            encryption_key,
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[instrument(level = "debug", skip_all)]
async fn on_peer_event_disk(
    doc_discovery_key: &[u8; 32],
    mut peer_event_receiver: UnboundedReceiver<PeerEvent>,
    mut state_event_sender: UnboundedSender<StateEvent>,
    mut doc_state: Arc<Mutex<DocStateWrapper<RandomAccessDisk>>>,
    mut feeds: Arc<DashMap<[u8; 32], Arc<Mutex<Feed<FeedDiskPersistence>>>>>,
    name: &str,
    proxy: bool,
    encryption_key: &Option<Vec<u8>>,
    data_root_dir: &PathBuf,
) {
    while let Some(event) = peer_event_receiver.next().await {
        debug!("Received event {:?}", event);
        match event.content {
            PeerEventContent::NewPeersBroadcasted(public_keys) => {
                let changed = {
                    let mut doc_state = doc_state.lock().await;
                    doc_state
                        .add_peer_public_keys_to_state(public_keys.clone())
                        .await
                };
                if changed {
                    {
                        // Create and insert all new feeds
                        create_and_insert_read_disk_feeds(
                            public_keys.clone(),
                            feeds.clone(),
                            proxy,
                            encryption_key,
                            data_root_dir,
                        )
                        .await;
                    }
                    {
                        notify_new_peers_created(&doc_discovery_key, &mut feeds, public_keys).await;
                    }
                }
            }
            _ => {
                process_peer_event(
                    event,
                    doc_discovery_key,
                    &mut state_event_sender,
                    &mut doc_state,
                    &mut feeds,
                    name,
                    proxy,
                )
                .await
            }
        }
    }
    debug!("Exiting");
}

#[cfg(not(target_arch = "wasm32"))]
async fn create_and_insert_read_disk_feeds(
    public_keys: Vec<[u8; 32]>,
    feeds: Arc<DashMap<[u8; 32], Arc<Mutex<Feed<FeedDiskPersistence>>>>>,
    proxy: bool,
    encryption_key: &Option<Vec<u8>>,
    data_root_dir: &PathBuf,
) {
    for public_key in public_keys {
        let discovery_key = discovery_key_from_public_key(&public_key);
        // Make sure to insert only once even if two protocols notice the same new
        // feed at the same timeusing the entry API.

        // There is a deadlock possibility with entry(), so we need to loop and yield
        let mut entry_found = false;
        while !entry_found {
            if let Some(entry) = feeds.try_entry(discovery_key.clone()) {
                match entry {
                    dashmap::mapref::entry::Entry::Occupied(_) => {
                        debug!("Concurrent creating of feeds noticed, continuing.");
                    }
                    dashmap::mapref::entry::Entry::Vacant(vacant) => {
                        let (_, feed) = create_new_read_disk_feed(
                            &data_root_dir,
                            &public_key,
                            &discovery_key,
                            proxy,
                            encryption_key.is_some(),
                            encryption_key,
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

//////////////////////////////////////////////////////
//
// Utilities

#[instrument(level = "debug", skip_all)]
async fn notify_new_peers_created<T>(
    doc_discovery_key: &[u8; 32],
    feeds: &mut Arc<DashMap<[u8; 32], Arc<Mutex<Feed<T>>>>>,
    public_keys: Vec<[u8; 32]>,
) where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send + 'static,
{
    // Send message to doc feed that new peers have been created to get all open protocols to
    // open channels to it.
    let doc_feed = feeds.get(doc_discovery_key).unwrap();
    let mut doc_feed = doc_feed.lock().await;
    doc_feed
        .notify_new_peers_created(public_keys)
        .await
        .unwrap();
}

#[instrument(level = "debug", skip_all)]
async fn process_peer_event<T>(
    event: PeerEvent,
    doc_discovery_key: &[u8; 32],
    state_event_sender: &mut UnboundedSender<StateEvent>,
    doc_state: &mut Arc<Mutex<DocStateWrapper<T>>>,
    feeds: &mut Arc<DashMap<[u8; 32], Arc<Mutex<Feed<T>>>>>,
    name: &str,
    proxy: bool,
) where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send + 'static,
{
    match event.content {
        PeerEventContent::NewPeersBroadcasted(_) => unreachable!("Implemented by concrete type"),
        PeerEventContent::PeerDisconnected(_) => {
            // This is an FYI message, just continue for now
        }
        PeerEventContent::RemotePeerSynced((discovery_key, synced_contiguous_length)) => {
            match state_event_sender.unbounded_send(StateEvent::new(
                *doc_discovery_key,
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
            if proxy {
                // Just notify a peer sync forward
                match state_event_sender.unbounded_send(StateEvent::new(
                    *doc_discovery_key,
                    StateEventContent::PeerSynced((None, discovery_key, synced_contiguous_length)),
                )) {
                    Ok(()) => {}
                    Err(err) => warn!(
                        "{}: could not notify peer synced to len {}, err {}",
                        name.to_string(),
                        synced_contiguous_length,
                        err
                    ),
                };
                return;
            }
            let (peer_sync_events, patches): (Vec<StateEvent>, Vec<Patch>) = {
                // Sync doc state exclusively...
                let mut doc_state = doc_state.lock().await;
                let (mut patches, peer_syncs) = if let Some((content, unapplied_entries)) =
                    doc_state.content_and_unapplied_entries_mut()
                {
                    let (patches, new_peer_names, peer_syncs) = update_synced_content(
                        &discovery_key,
                        synced_contiguous_length,
                        content,
                        feeds.clone(),
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
                    if let Some((content, patches, new_peer_names, peer_syncs)) = create_content(
                        &discovery_key,
                        synced_contiguous_length,
                        &doc_discovery_key,
                        name,
                        &write_discovery_key,
                        feeds.clone(),
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
                        StateEvent::new(
                            *doc_discovery_key,
                            StateEventContent::PeerSynced((Some(name), sync.0.clone(), sync.1)),
                        )
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
                    .unbounded_send(StateEvent::new(
                        *doc_discovery_key,
                        StateEventContent::DocumentChanged(patches),
                    ))
                    .unwrap();
            }

            // Finally, notify about the new sync so that other protocols can get synced as
            // well. The message reaches the same feed that sent this, but is disregarded.
            {
                let feed = feeds.get_mut(&discovery_key).unwrap();
                let mut feed = feed.lock().await;
                feed.notify_peer_synced(synced_contiguous_length)
                    .await
                    .unwrap();
            }
        }
    }
}

struct PrepareCreateResult {
    key_pair: Keypair,
    discovery_key: [u8; 32],
    doc: AutomergeDoc,
    doc_public_key: [u8; 32],
    data: Vec<u8>,
}

async fn prepare_create<P: Into<Prop>, V: Into<ScalarValue>>(
    name: &str,
    root_scalars: Vec<(P, V)>,
) -> PrepareCreateResult {
    // Generate a key pair, its discovery key and the public key string
    let (key_pair, discovery_key) = generate_keys();
    let (doc, data) = init_doc_with_root_scalars(name, &discovery_key, root_scalars);
    let doc_public_key = *key_pair.public.as_bytes();
    PrepareCreateResult {
        key_pair,
        discovery_key,
        doc,
        doc_public_key,
        data,
    }
}

async fn create_content<T>(
    synced_discovery_key: &[u8; 32],
    synced_contiguous_length: u64,
    doc_discovery_key: &[u8; 32],
    writer_name: &str,
    write_discovery_key: &[u8; 32],
    feeds: Arc<DashMap<[u8; 32], Arc<Mutex<Feed<T>>>>>,
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
    // The document starts from the doc feed, so get that first
    if synced_discovery_key == doc_discovery_key {
        let entries = {
            let doc_feed = feeds.get(doc_discovery_key).unwrap();
            let mut doc_feed = doc_feed.lock().await;
            doc_feed.entries(0, synced_contiguous_length).await?
        };

        // Create DocContent from the feed
        let (mut doc, data, result) = init_doc_from_entries(
            writer_name,
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
        let feed = feeds.get(synced_discovery_key).unwrap();
        let mut feed = feed.lock().await;
        let start_index = unapplied_entries.current_length(synced_discovery_key);
        let entries = feed
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
    discovery_keys: Vec<[u8; 32]>,
    content: &mut DocContent,
    feeds: Arc<DashMap<[u8; 32], Arc<Mutex<Feed<T>>>>>,
    unapplied_entries: &mut UnappliedEntries,
) -> anyhow::Result<(bool, Vec<([u8; 32], String)>)>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send + 'static,
{
    let mut new_peer_names: Vec<([u8; 32], String)> = vec![];
    let mut changed = false;
    for discovery_key in discovery_keys {
        let (contiguous_length, entries) =
            get_new_entries(&discovery_key, None, content, &feeds).await?;

        let result = update_content_with_entries(
            entries,
            &discovery_key,
            contiguous_length,
            content,
            unapplied_entries,
        )
        .await?;
        if !result.0.is_empty() {
            changed = true;
        }
        new_peer_names.extend(result.1);
    }

    Ok((changed, new_peer_names))
}

async fn update_synced_content<T>(
    synced_discovery_key: &[u8; 32],
    synced_contiguous_length: u64,
    content: &mut DocContent,
    feeds: Arc<DashMap<[u8; 32], Arc<Mutex<Feed<T>>>>>,
    unapplied_entries: &mut UnappliedEntries,
) -> anyhow::Result<(Vec<Patch>, Vec<([u8; 32], String)>, Vec<([u8; 32], u64)>)>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send + 'static,
{
    let (_, entries) = get_new_entries(
        synced_discovery_key,
        Some(synced_contiguous_length),
        content,
        &feeds,
    )
    .await?;

    update_content_with_entries(
        entries,
        synced_discovery_key,
        synced_contiguous_length,
        content,
        unapplied_entries,
    )
    .await
}

async fn get_new_entries<T>(
    discovery_key: &[u8; 32],
    known_contiguous_length: Option<u64>,
    content: &mut DocContent,
    feeds: &Arc<DashMap<[u8; 32], Arc<Mutex<Feed<T>>>>>,
) -> anyhow::Result<(u64, Vec<Entry>)>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send + 'static,
{
    let feed = feeds.get(discovery_key).unwrap();
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
    content: &mut DocContent,
    unapplied_entries: &mut UnappliedEntries,
) -> anyhow::Result<(Vec<Patch>, Vec<([u8; 32], String)>, Vec<([u8; 32], u64)>)> {
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
