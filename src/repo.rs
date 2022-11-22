use crate::{automerge::init_doc_with_root_props, store::HypermergeStore};
use async_std::sync::{Arc, Mutex};
use automerge::{Prop, ScalarValue};
use futures_lite::{AsyncRead, AsyncWrite, StreamExt};
use hypercore_protocol::{Event, Protocol};
#[cfg(not(target_arch = "wasm32"))]
use random_access_disk::RandomAccessDisk;
use random_access_memory::RandomAccessMemory;
use random_access_storage::RandomAccess;
use std::fmt::Debug;
#[cfg(not(target_arch = "wasm32"))]
use std::path::PathBuf;

/// Repo is the main abstraction of hypermerge
pub struct Repo<T>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send,
{
    store: Arc<Mutex<HypermergeStore<T>>>,
    state: Arc<T>,
}

#[cfg(not(target_arch = "wasm32"))]
impl Repo<RandomAccessDisk> {
    pub async fn new_disk(data_root_dir: &PathBuf) -> Self {
        let store: HypermergeStore<RandomAccessDisk> =
            HypermergeStore::new_disk(data_root_dir).await;
        let state_path = data_root_dir.join(PathBuf::from("state.bin"));
        let state = RandomAccessDisk::builder(state_path).build().await.unwrap();
        Self {
            store: Arc::new(Mutex::new(store)),
            state: Arc::new(state),
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
        let store: HypermergeStore<RandomAccessMemory> = HypermergeStore::new_memory();
        let state = RandomAccessMemory::default();
        Self {
            store: Arc::new(Mutex::new(store)),
            state: Arc::new(state),
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
    pub async fn connect<IO>(
        &mut self,
        doc_url: &str,
        protocol: &mut Protocol<IO>,
    ) -> anyhow::Result<()>
    where
        IO: AsyncWrite + AsyncRead + Send + Unpin + 'static,
    {
        let is_initiator = protocol.is_initiator();

        if let Some((_doc, hypercore_store)) = self.store.lock().await.get_doc_state(doc_url) {
            while let Some(event) = protocol.next().await {
                let event = event?;
                match event {
                    Event::Handshake(_) => {
                        if is_initiator {
                            for hypercore in hypercore_store.values() {
                                let hypercore = hypercore.lock().await;
                                protocol.open(hypercore.key().clone()).await?;
                            }
                        }
                    }
                    Event::DiscoveryKey(dkey) => {
                        let discovery_key = hex::encode(dkey);
                        if let Some(hypercore) = hypercore_store.get(&discovery_key) {
                            let hypercore = hypercore.lock().await;
                            protocol.open(hypercore.key().clone()).await?;
                        }
                    }
                    Event::Channel(channel) => {
                        let discovery_key = hex::encode(channel.discovery_key());
                        if let Some(hypercore) = hypercore_store.get(&discovery_key) {
                            let hypercore = hypercore.lock().await;
                            hypercore.on_peer(channel);
                        }
                    }
                    Event::Close(_dkey) => {}
                    _ => {}
                }
            }
        }

        Ok(())
    }
}
