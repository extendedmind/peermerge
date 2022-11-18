use crate::{automerge::init_doc_with_root_props, store::HypermergeStore};
use async_std::sync::{Arc, Mutex};
use automerge::{Prop, ScalarValue};
use futures_lite::{AsyncRead, AsyncWrite};
use hypercore_protocol::Protocol;
use random_access_disk::RandomAccessDisk;
use random_access_memory::RandomAccessMemory;
use random_access_storage::RandomAccess;
use std::{fmt::Debug, path::PathBuf};

/// Repo is the main abstraction of hypermerge
pub struct Repo<T>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send,
    // IO: AsyncWrite + AsyncRead + Send + Unpin + 'static,
{
    store: Arc<Mutex<HypermergeStore<T>>>,
    state: Arc<T>,
    // create_protocol: Box<
    //     dyn Fn(
    //         String,
    //     )
    //         -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Protocol<IO>>> + Send>>,
    // >,
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
}

impl Repo<RandomAccessMemory> {
    // pub async fn new_memory<Cb>(create_protocol: Cb) -> Self
    // where
    //     Cb: Fn(
    //             String,
    //         )
    //             -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Protocol<IO>>> + Send>>
    //         + 'static,
    // {
    //     let mut hypercore_store: HypercoreStore<RandomAccessMemory> = HypercoreStore::new();
    //     let state = RandomAccessMemory::new(1000);
    //     Self {
    //         hypercore_store: Arc::new(hypercore_store),
    //         state: Arc::new(state),
    //         create_protocol: Box::new(create_protocol),
    //     }
    // }

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
}

impl<T> Repo<T>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send,
{
    pub async fn connect<IO>(&mut self, protocol: &mut Protocol<IO>)
    where
        IO: AsyncWrite + AsyncRead + Send + Unpin + 'static,
    {
    }
}
