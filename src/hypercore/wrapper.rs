use async_channel::Sender;
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

use crate::common::PeerEvent;

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
    pub(crate) async fn append(&mut self, data: &[u8]) {
        let mut hypercore = self.hypercore.lock().await;
        hypercore.append(data).await;
    }

    pub(super) fn key(&self) -> &[u8; 32] {
        &self.key
    }

    pub(super) fn on_channel(
        &self,
        channel: Channel,
        public_keys: Vec<[u8; 32]>,
        peer_event_sender: &mut Sender<PeerEvent>,
        is_initiator: bool,
    ) {
        println!(
            "on_channel({}): id={}, len={}: {:?}",
            is_initiator,
            channel.id(),
            public_keys.len(),
            public_keys
        );
        let peer_state = PeerState::new(public_keys);
        let hypercore = self.hypercore.clone();
        let mut peer_event_sender_for_task = peer_event_sender.clone();
        #[cfg(not(target_arch = "wasm32"))]
        task::spawn(async move {
            on_peer(
                hypercore,
                peer_state,
                channel,
                &mut peer_event_sender_for_task,
                is_initiator,
            )
            .await
            .expect("peer connect failed");
        });
        #[cfg(target_arch = "wasm32")]
        spawn_local(async move {
            on_peer(
                hypercore,
                peer_state,
                channel,
                &mut peer_event_sender_for_task,
                is_initiator,
            )
            .await
            .expect("peer connect failed");
        });
    }
}
