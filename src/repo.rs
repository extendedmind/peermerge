use async_channel::{Receiver, Sender};
use async_std::sync::{Arc, Mutex};
use automerge::{Prop, ScalarValue};
use futures_lite::{AsyncRead, AsyncWrite, StreamExt};
use hypercore_protocol::Protocol;
#[cfg(not(target_arch = "wasm32"))]
use random_access_disk::RandomAccessDisk;
use random_access_memory::RandomAccessMemory;
use random_access_storage::RandomAccess;
use std::fmt::Debug;
#[cfg(not(target_arch = "wasm32"))]
use std::path::PathBuf;
#[cfg(target_arch = "wasm32")]
use wasm_bindgen_futures::spawn_local;

use crate::{
    automerge::init_doc_with_root_props, common::SynchronizeEvent, hypercore::on_protocol,
    store::DocStore, StateEvent,
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
        root_props: Vec<(P, V)>,
    ) -> String {
        let doc = init_doc_with_root_props(root_props);
        self.store.lock().await.add_doc_disk(doc).await
    }

    pub async fn register_doc_disk(&mut self, doc_url: &str) {
        self.store.lock().await.register_doc_disk(doc_url).await
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
        root_props: Vec<(P, V)>,
    ) -> String {
        let doc = init_doc_with_root_props(root_props);
        self.store.lock().await.add_doc_memory(doc).await
    }

    pub async fn register_doc_memory(&mut self, doc_url: &str) {
        self.store.lock().await.register_doc_memory(doc_url).await
    }
}

impl<T> Repo<T>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send + 'static,
{
    pub async fn connect_protocol<IO>(
        &mut self,
        doc_url: &str,
        protocol: &mut Protocol<IO>,
        sync_event_sender: &mut Sender<SynchronizeEvent>,
    ) -> anyhow::Result<()>
    where
        IO: AsyncWrite + AsyncRead + Send + Unpin + 'static,
    {
        if let Some(hypercore_store) = self.store.lock().await.get_mut(doc_url) {
            on_protocol(protocol, hypercore_store, sync_event_sender).await?
        }
        Ok(())
    }

    pub async fn connect_document(
        &mut self,
        doc_url: &str,
        state_event_sender: Sender<StateEvent>,
        sync_event_receiver: &mut Receiver<SynchronizeEvent>,
    ) -> anyhow::Result<()> {
        while let Some(event) = sync_event_receiver.next().await {
            // TODO: Do something here with sync event to trigger state event
        }
        Ok(())
    }
}
