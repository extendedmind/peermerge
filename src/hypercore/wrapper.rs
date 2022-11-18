use async_std::sync::Mutex;
use async_std::task;
use futures_lite::stream::StreamExt;
use hypercore_protocol::{discovery_key, hypercore::Hypercore, schema::*, Channel, Message};
use random_access_disk::RandomAccessDisk;
use random_access_memory::RandomAccessMemory;
use random_access_storage::RandomAccess;
use std::{fmt::Debug, sync::Arc};

use super::{on_message, PeerState};

#[derive(Debug, Clone)]
pub struct HypercoreWrapper<T>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send,
{
    pub(super) discovery_key: [u8; 32],
    pub(super) key: [u8; 32],
    pub(super) hypercore: Arc<Mutex<Hypercore<T>>>,
}

impl HypercoreWrapper<RandomAccessDisk> {
    pub fn from_disk_hypercore(hypercore: Hypercore<RandomAccessDisk>) -> Self {
        let key = hypercore.key_pair().public.to_bytes();
        HypercoreWrapper {
            key,
            discovery_key: discovery_key(&key),
            hypercore: Arc::new(Mutex::new(hypercore)),
        }
    }
}

impl HypercoreWrapper<RandomAccessMemory> {
    pub fn from_memory_hypercore(hypercore: Hypercore<RandomAccessMemory>) -> Self {
        let key = hypercore.key_pair().public.to_bytes();
        HypercoreWrapper {
            key,
            discovery_key: discovery_key(&key),
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

    pub fn on_peer(&self, mut channel: Channel) {
        let mut peer_state = PeerState::default();
        let mut hypercore = self.hypercore.clone();
        task::spawn(async move {
            let info = {
                let hypercore = hypercore.lock().await;
                hypercore.info()
            };

            if info.fork != peer_state.remote_fork {
                peer_state.can_upgrade = false;
            }
            let remote_length = if info.fork == peer_state.remote_fork {
                peer_state.remote_length
            } else {
                0
            };

            let sync_msg = Synchronize {
                fork: info.fork,
                length: info.length,
                remote_length,
                can_upgrade: peer_state.can_upgrade,
                uploading: true,
                downloading: true,
            };

            if info.contiguous_length > 0 {
                let range_msg = Range {
                    drop: false,
                    start: 0,
                    length: info.contiguous_length,
                };
                channel
                    .send_batch(&[Message::Synchronize(sync_msg), Message::Range(range_msg)])
                    .await
                    .unwrap();
            } else {
                channel.send(Message::Synchronize(sync_msg)).await.unwrap();
            }
            while let Some(message) = channel.next().await {
                let ready = on_message(&mut hypercore, &mut peer_state, &mut channel, message)
                    .await
                    .expect("on_message should return Ok");
                if ready {
                    channel.close().await.expect("Should be able to close");
                    break;
                }
            }
        });
    }
}
