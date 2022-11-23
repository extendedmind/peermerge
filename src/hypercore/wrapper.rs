use async_std::sync::{Arc, Mutex};
#[cfg(not(target_arch = "wasm32"))]
use async_std::task;
use hypercore_protocol::{hypercore::Hypercore, Channel};
#[cfg(not(target_arch = "wasm32"))]
use random_access_disk::RandomAccessDisk;
use random_access_memory::RandomAccessMemory;
use random_access_storage::RandomAccess;
use std::fmt::Debug;
#[cfg(target_arch = "wasm32")]
use wasm_bindgen_futures::spawn_local;

use super::{on_peer, PeerState};

#[derive(Debug, Clone)]
pub struct HypercoreWrapper<T>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send,
{
    pub(super) key: [u8; 32],
    pub(super) hypercore: Arc<Mutex<Hypercore<T>>>,
}

#[cfg(not(target_arch = "wasm32"))]
impl HypercoreWrapper<RandomAccessDisk> {
    pub fn from_disk_hypercore(hypercore: Hypercore<RandomAccessDisk>) -> Self {
        let key = hypercore.key_pair().public.to_bytes();
        HypercoreWrapper {
            key,
            hypercore: Arc::new(Mutex::new(hypercore)),
        }
    }
}

impl HypercoreWrapper<RandomAccessMemory> {
    pub fn from_memory_hypercore(hypercore: Hypercore<RandomAccessMemory>) -> Self {
        let key = hypercore.key_pair().public.to_bytes();
        HypercoreWrapper {
            key,
            hypercore: Arc::new(Mutex::new(hypercore)),
        }
    }
}

impl<T> HypercoreWrapper<T>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send + 'static,
{
    pub fn key(&self) -> &[u8; 32] {
        &self.key
    }

    pub fn on_peer(&self, channel: Channel) {
        let peer_state = PeerState::default();
        let hypercore = self.hypercore.clone();
        #[cfg(not(target_arch = "wasm32"))]
        task::spawn(async move {
            on_peer(hypercore, peer_state, channel).await;
        });
        #[cfg(target_arch = "wasm32")]
        spawn_local(async move {
            on_peer(hypercore, peer_state, channel).await;
        });
    }
}
