use dashmap::DashMap;
#[cfg(not(target_arch = "wasm32"))]
use random_access_disk::RandomAccessDisk;
use random_access_memory::RandomAccessMemory;
use random_access_storage::RandomAccess;
use std::fmt::Debug;
use std::path::PathBuf;
use std::sync::Arc;

#[cfg(feature = "async-std")]
use async_std::sync::Mutex;
#[cfg(feature = "tokio")]
use tokio::sync::Mutex;

use crate::common::storage::RepositoryStateWrapper;
use crate::Peermerge;

/// PeermergeRepository is a store for multiple Peermerges
#[derive(Debug, Clone)]
pub struct PeermergeRepository<T>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send,
{
    /// Current storable state
    repository_state: Arc<Mutex<RepositoryStateWrapper<T>>>,
    /// Created peermerges
    peermerges: Arc<DashMap<[u8; 32], Peermerge<T>>>,
    /// Map of independent docs' discovery keys with their children.
    peermerge_dependencies: Arc<DashMap<[u8; 32], Vec<[u8; 32]>>>,
}

//////////////////////////////////////////////////////
//
// RandomAccessMemory

impl PeermergeRepository<RandomAccessMemory> {
    pub async fn create_new_memory() -> Self {
        let state = RepositoryStateWrapper::new_memory().await;
        Self {
            repository_state: Arc::new(Mutex::new(state)),
            peermerges: Arc::new(DashMap::new()),
            peermerge_dependencies: Arc::new(DashMap::new()),
        }
    }
}

//////////////////////////////////////////////////////
//
// RandomAccessDisk

#[cfg(not(target_arch = "wasm32"))]
impl PeermergeRepository<RandomAccessDisk> {
    pub async fn open_disk(data_root_dir: PathBuf) -> Self {
        let state = RepositoryStateWrapper::open_disk(&data_root_dir).await;
        Self {
            repository_state: Arc::new(Mutex::new(state)),
            peermerges: Arc::new(DashMap::new()),
            peermerge_dependencies: Arc::new(DashMap::new()),
        }
    }
}
