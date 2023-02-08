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
use std::sync::Arc;
use tracing::{debug, instrument};

#[cfg(feature = "async-std")]
use async_std::sync::Mutex;
#[cfg(all(not(target_arch = "wasm32"), feature = "async-std"))]
use async_std::task;
#[cfg(feature = "tokio")]
use tokio::sync::Mutex;
#[cfg(all(not(target_arch = "wasm32"), feature = "tokio"))]
use tokio::task;
#[cfg(target_arch = "wasm32")]
use wasm_bindgen_futures::spawn_local;

use super::{
    messaging::{
        create_append_local_signal, create_new_peers_created_local_signal,
        create_peer_synced_local_signal,
    },
    on_doc_peer, on_peer, PeerState,
};
use crate::common::{cipher::EntryCipher, entry::Entry, PeerEvent};

#[derive(Debug)]
pub(crate) struct HypercoreWrapper<T>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send,
{
    pub(super) public_key: [u8; 32],
    pub(super) hypercore: Arc<Mutex<Hypercore<T>>>,
    proxy: bool,
    entry_cipher: Option<EntryCipher>,
    channel_senders: Vec<ChannelSender<Message>>,
    corked: bool,
    message_queue: Vec<Message>,
}

#[cfg(not(target_arch = "wasm32"))]
impl HypercoreWrapper<RandomAccessDisk> {
    pub(crate) fn from_disk_hypercore(
        hypercore: Hypercore<RandomAccessDisk>,
        proxy: bool,
        encrypted: bool,
        encryption_key: &Option<Vec<u8>>,
        generate_encryption_key_if_missing: bool,
    ) -> (Self, Option<Vec<u8>>) {
        let public_key = hypercore.key_pair().public.to_bytes();
        let (entry_cipher, key) = prepare_entry_cipher(
            proxy,
            encrypted,
            encryption_key,
            generate_encryption_key_if_missing,
        );
        let wrapper = HypercoreWrapper {
            public_key,
            hypercore: Arc::new(Mutex::new(hypercore)),
            proxy,
            entry_cipher,
            channel_senders: vec![],
            corked: false,
            message_queue: vec![],
        };
        (wrapper, key)
    }
}

impl HypercoreWrapper<RandomAccessMemory> {
    pub(crate) fn from_memory_hypercore(
        hypercore: Hypercore<RandomAccessMemory>,
        proxy: bool,
        encrypted: bool,
        encryption_key: &Option<Vec<u8>>,
        generate_encryption_key_if_missing: bool,
    ) -> (Self, Option<Vec<u8>>) {
        let public_key = hypercore.key_pair().public.to_bytes();
        let (entry_cipher, key) = prepare_entry_cipher(
            proxy,
            encrypted,
            encryption_key,
            generate_encryption_key_if_missing,
        );
        let wrapper = HypercoreWrapper {
            public_key,
            hypercore: Arc::new(Mutex::new(hypercore)),
            proxy,
            entry_cipher,
            channel_senders: vec![],
            corked: false,
            message_queue: vec![],
        };
        (wrapper, key)
    }
}

impl<T> HypercoreWrapper<T>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send + 'static,
{
    pub(crate) async fn append(&mut self, data: &[u8]) -> anyhow::Result<u64> {
        if self.proxy {
            panic!("Can not append to a proxy");
        }
        let outcome = {
            let mut hypercore = self.hypercore.lock().await;

            if let Some(entry_cipher) = &self.entry_cipher {
                let encrypted =
                    entry_cipher.encrypt(&self.public_key, hypercore.info().length, data);
                hypercore.append(&encrypted).await?
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

    pub(crate) async fn contiguous_length(&self) -> u64 {
        let hypercore = self.hypercore.lock().await;
        hypercore.info().contiguous_length
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
        doc_discovery_key: [u8; 32],
        public_keys: Vec<[u8; 32]>,
    ) -> anyhow::Result<()> {
        if self.channel_senders.len() > 0 {
            let message = create_new_peers_created_local_signal(doc_discovery_key, public_keys);
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
        if self.proxy {
            panic!("Can not get entries from a proxy");
        }
        let mut hypercore = self.hypercore.lock().await;
        let mut entries: Vec<Entry> = vec![];
        for i in index..len {
            let data = if let Some(entry_cipher) = &self.entry_cipher {
                let data = hypercore.get(i).await.unwrap().unwrap();
                entry_cipher.decrypt(&self.public_key, i, &data)
            } else {
                hypercore.get(i).await.unwrap().unwrap()
            };
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
        doc_discovery_key: [u8; 32],
        channel: Channel,
        channel_receiver: ChannelReceiver<Message>,
        channel_sender: ChannelSender<Message>,
        write_public_key: Option<[u8; 32]>,
        peer_public_keys: Vec<[u8; 32]>,
        peer_event_sender: &mut UnboundedSender<PeerEvent>,
    ) {
        debug!("Processing channel id={}", channel.id(),);
        self.channel_senders.push(channel_sender);
        let peer_state = PeerState::new(
            is_doc,
            doc_discovery_key,
            write_public_key,
            peer_public_keys,
        );
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

fn prepare_entry_cipher(
    proxy: bool,
    encrypted: bool,
    encryption_key: &Option<Vec<u8>>,
    generate_encryption_key_if_missing: bool,
) -> (Option<EntryCipher>, Option<Vec<u8>>) {
    if !proxy && encrypted {
        if let Some(encryption_key) = encryption_key {
            (Some(EntryCipher::from_encryption_key(encryption_key)), None)
        } else if generate_encryption_key_if_missing {
            let (entry_cipher, key) = EntryCipher::from_generated_key();
            (Some(entry_cipher), Some(key))
        } else {
            (None, None)
        }
    } else {
        (None, None)
    }
}
