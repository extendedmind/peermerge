use async_std::sync::{Arc, Mutex};
#[cfg(not(target_arch = "wasm32"))]
use async_std::task;
use chacha20poly1305::{
    aead::{Aead, KeyInit},
    XChaCha20Poly1305, XNonce,
};
use futures::channel::mpsc::UnboundedSender;
use hypercore_protocol::{
    hypercore::{
        compact_encoding::{CompactEncoding, State},
        Hypercore,
    },
    Channel, ChannelReceiver, ChannelSender, Message,
};
#[cfg(not(target_arch = "wasm32"))]
use random_access_disk::RandomAccessDisk;
use random_access_memory::RandomAccessMemory;
use random_access_storage::RandomAccess;
use std::fmt::Debug;
use tracing::{debug, instrument};
#[cfg(target_arch = "wasm32")]
use wasm_bindgen_futures::spawn_local;

use crate::common::{entry::Entry, PeerEvent};

use super::{
    messaging::{
        create_append_local_signal, create_new_peers_created_local_signal,
        create_peer_synced_local_signal,
    },
    on_doc_peer, on_peer, PeerState,
};

struct EntryCipher {
    cipher: XChaCha20Poly1305,
}

impl Debug for EntryCipher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "EntryCipherSuite(undisclosed)")
    }
}

#[derive(Debug)]
pub struct HypercoreWrapper<T>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send,
{
    pub(super) public_key: [u8; 32],
    pub(super) hypercore: Arc<Mutex<Hypercore<T>>>,
    pub(crate) encrypted: bool,
    entry_cipher: Option<EntryCipher>,
    channel_senders: Vec<ChannelSender<Message>>,
    corked: bool,
    message_queue: Vec<Message>,
}

#[cfg(not(target_arch = "wasm32"))]
impl HypercoreWrapper<RandomAccessDisk> {
    pub fn from_disk_hypercore(hypercore: Hypercore<RandomAccessDisk>) -> Self {
        let public_key = hypercore.key_pair().public.to_bytes();
        HypercoreWrapper {
            public_key,
            hypercore: Arc::new(Mutex::new(hypercore)),
            encrypted: false,
            entry_cipher: None,
            channel_senders: vec![],
            corked: false,
            message_queue: vec![],
        }
    }
}

impl HypercoreWrapper<RandomAccessMemory> {
    pub fn from_memory_hypercore(hypercore: Hypercore<RandomAccessMemory>) -> Self {
        let public_key = hypercore.key_pair().public.to_bytes();
        HypercoreWrapper {
            public_key,
            hypercore: Arc::new(Mutex::new(hypercore)),
            encrypted: false,
            entry_cipher: None,
            channel_senders: vec![],
            corked: false,
            message_queue: vec![],
        }
    }
}

impl<T> HypercoreWrapper<T>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send + 'static,
{
    pub(crate) fn set_encrypted(&mut self, encrypted: bool) {
        self.encrypted = encrypted;
    }

    pub(crate) fn set_encryption_key(&mut self, key: &[u8]) {
        self.entry_cipher = Some(EntryCipher {
            cipher: XChaCha20Poly1305::new_from_slice(key).unwrap(),
        });
        self.encrypted = true;
    }

    pub(crate) async fn append(&mut self, data: &[u8]) -> anyhow::Result<u64> {
        let outcome = {
            let mut hypercore = self.hypercore.lock().await;

            if let Some(entry_cipher) = &self.entry_cipher {
                if !self.encrypted {
                    panic!("Trying to append encrypted entry to an unencrypted hypercore");
                }
                let nonce = generate_nonce(&self.public_key, hypercore.info().length);
                let encrypted = entry_cipher.cipher.encrypt(&nonce, data).unwrap();
                hypercore.append(&encrypted).await?
            } else if self.encrypted {
                panic!("Trying to append to an encrypted hypercore without a key provided");
            } else {
                hypercore.append(data).await?
            }
        };
        if self.channel_senders.len() > 0 {
            let message = create_append_local_signal(outcome.length);
            self.notify_listeners(&message).await?;
        }
        Ok(outcome.length)
    }

    pub(crate) async fn notify_peer_synced(
        &mut self,
        contiguous_length: u64,
    ) -> anyhow::Result<()> {
        if self.channel_senders.len() > 0 {
            let message = create_peer_synced_local_signal(contiguous_length);
            self.notify_listeners(&message).await?;
        }
        Ok(())
    }

    pub(crate) async fn notify_new_peers_created(
        &mut self,
        public_keys: Vec<[u8; 32]>,
    ) -> anyhow::Result<()> {
        if self.channel_senders.len() > 0 {
            let message = create_new_peers_created_local_signal(public_keys);
            self.notify_listeners(&message).await?;
        }
        Ok(())
    }

    /// Cork sending notifications about this hypercore and start queuing in-memory messages
    /// about changes.
    pub(crate) fn cork(&mut self) {
        self.corked = true;
    }

    /// Remove cork and send out all of the messages that were corked to listeners.
    pub(crate) async fn uncork(&mut self) -> anyhow::Result<()> {
        let messages = {
            self.corked = false;
            self.message_queue.clone()
        };
        {
            for message in messages {
                self.notify_listeners(&message).await?;
            }
        }
        {
            self.message_queue = vec![];
        }
        Ok(())
    }

    pub(crate) async fn entries(&mut self, index: u64, len: u64) -> anyhow::Result<Vec<Entry>> {
        let mut hypercore = self.hypercore.lock().await;
        let mut entries: Vec<Entry> = vec![];
        for i in index..len {
            let data = hypercore.get(i).await.unwrap().unwrap();
            let mut dec_state = State::from_buffer(&data);
            let entry: Entry = dec_state.decode(&data);
            entries.push(entry);
        }
        Ok(entries)
    }

    pub(super) fn public_key(&self) -> &[u8; 32] {
        &self.public_key
    }

    #[instrument(level = "debug", skip_all)]
    pub(super) fn on_channel(
        &mut self,
        is_doc: bool,
        channel: Channel,
        channel_receiver: ChannelReceiver<Message>,
        channel_sender: ChannelSender<Message>,
        write_public_key: Option<[u8; 32]>,
        peer_public_keys: Vec<[u8; 32]>,
        peer_event_sender: &mut UnboundedSender<PeerEvent>,
    ) {
        debug!("Processing channel id={}", channel.id(),);
        self.channel_senders.push(channel_sender);
        let peer_state = PeerState::new(is_doc, write_public_key, peer_public_keys);
        let hypercore = self.hypercore.clone();
        let mut peer_event_sender_for_task = peer_event_sender.clone();
        let task_span = if is_doc {
            tracing::debug_span!("call_on_doc_peer").or_current()
        } else {
            tracing::debug_span!("call_on_peer").or_current()
        };
        #[cfg(not(target_arch = "wasm32"))]
        task::spawn(async move {
            let _entered = task_span.enter();
            if is_doc {
                on_doc_peer(
                    hypercore,
                    peer_state,
                    channel,
                    channel_receiver,
                    &mut peer_event_sender_for_task,
                )
                .await
                .expect("doc peer connect failed");
            } else {
                on_peer(
                    hypercore,
                    peer_state,
                    channel,
                    channel_receiver,
                    &mut peer_event_sender_for_task,
                )
                .await
                .expect("peer connect failed");
            }
        });
        #[cfg(target_arch = "wasm32")]
        spawn_local(async move {
            let _entered = task_span.enter();
            if is_doc {
                on_doc_peer(
                    hypercore,
                    peer_state,
                    channel,
                    channel_receiver,
                    &mut peer_event_sender_for_task,
                )
                .await
                .expect("doc peer connect failed");
            } else {
                on_peer(
                    hypercore,
                    peer_state,
                    channel,
                    channel_receiver,
                    &mut peer_event_sender_for_task,
                )
                .await
                .expect("peer connect failed");
            }
        });
    }

    async fn notify_listeners(&mut self, message: &Message) -> anyhow::Result<()> {
        let mut closed_indices: Vec<usize> = vec![];
        for i in 0..self.channel_senders.len() {
            if self.channel_senders[i].is_closed() {
                closed_indices.push(i);
            } else {
                let message = message.clone();
                if !self.corked {
                    self.channel_senders[i].send(message).await?;
                } else {
                    self.message_queue.push(message);
                }
            }
        }
        closed_indices.sort();
        closed_indices.reverse();
        for i in closed_indices {
            self.channel_senders.remove(i);
        }
        Ok(())
    }
}

fn generate_nonce(public_key: &[u8; 32], index: u64) -> XNonce {
    let mut nonce = public_key[..16].to_vec();
    nonce.extend(index.to_le_bytes());
    XNonce::clone_from_slice(&nonce)
}
