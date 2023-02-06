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
use tracing::{debug, instrument};

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

use crate::common::PeerEvent;
use crate::common::{keys::discovery_key_from_public_key, storage::RepositoryStateWrapper};
#[cfg(not(target_arch = "wasm32"))]
use crate::feed::FeedDiskPersistence;
use crate::feed::{
    create_new_read_memory_feed, Feed, FeedMemoryPersistence, FeedPersistence, Protocol, IO,
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
    /// Created peermerges
    peermerges: Arc<DashMap<[u8; 32], Peermerge<T>>>,
    /// Map of independent docs' discovery keys with their children.
    peermerge_dependencies: Arc<DashMap<[u8; 32], Vec<[u8; 32]>>>,
    /// All feeds
    feeds: Arc<DashMap<[u8; 32], Arc<Mutex<Feed<U>>>>>,
    /// Sender for events
    state_event_sender: Option<UnboundedSender<StateEvent>>,
}

impl<T, U> PeermergeRepository<T, U>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send + 'static,
    U: FeedPersistence,
{
}

//////////////////////////////////////////////////////
//
// RandomAccessMemory

impl PeermergeRepository<RandomAccessMemory, FeedMemoryPersistence> {
    pub async fn create_new_memory() -> Self {
        let state = RepositoryStateWrapper::new_memory().await;
        Self {
            name: "".to_string(),
            repository_state: Arc::new(Mutex::new(state)),
            peermerges: Arc::new(DashMap::new()),
            peermerge_dependencies: Arc::new(DashMap::new()),
            feeds: Arc::new(DashMap::new()),
            state_event_sender: None,
        }
    }
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
impl PeermergeRepository<RandomAccessDisk, FeedDiskPersistence> {
    pub async fn open_disk(data_root_dir: PathBuf) -> Self {
        let state = RepositoryStateWrapper::open_disk(&data_root_dir).await;
        Self {
            name: "".to_string(),
            repository_state: Arc::new(Mutex::new(state)),
            peermerges: Arc::new(DashMap::new()),
            peermerge_dependencies: Arc::new(DashMap::new()),
            feeds: Arc::new(DashMap::new()),
            state_event_sender: None,
        }
    }
}

//////////////////////////////////////////////////////
//
// Utilities

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
