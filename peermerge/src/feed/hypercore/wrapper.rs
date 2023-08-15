use compact_encoding::{CompactEncoding, State};
use futures::channel::mpsc::UnboundedSender;
use hypercore_protocol::{
    hypercore::{Hypercore, PartialKeypair},
    Channel, ChannelReceiver, ChannelSender, Message,
};
#[cfg(not(target_arch = "wasm32"))]
use random_access_disk::RandomAccessDisk;
use random_access_memory::RandomAccessMemory;
use random_access_storage::RandomAccess;
use std::fmt::Debug;
use std::sync::Arc;
use tracing::{debug, instrument};

#[cfg(all(not(target_arch = "wasm32"), feature = "async-std"))]
use async_std::task;
#[cfg(all(not(target_arch = "wasm32"), feature = "tokio"))]
use tokio::task;
#[cfg(target_arch = "wasm32")]
use wasm_bindgen_futures::spawn_local;

use super::{
    messaging::{
        create_append_local_signal, create_closed_local_signal, create_feed_synced_local_signal,
        create_feeds_changed_local_signal,
    },
    on_doc_feed, on_feed, PeerState,
};
use crate::{
    common::{
        cipher::EntryCipher,
        entry::{shrink_entries, Entry, ShrunkEntries},
        state::{DocumentFeedInfo, DocumentFeedsState},
        utils::Mutex,
        FeedEvent,
    },
    feed::FeedDiscoveryKey,
    PeerId, PeermergeError,
};

#[derive(Debug)]
pub(crate) struct HypercoreWrapper<T>
where
    T: RandomAccess + Debug + Send,
{
    pub(super) public_key: [u8; 32],
    pub(super) hypercore: Arc<Mutex<Hypercore<T>>>,
    proxy: bool,
    entry_cipher: Option<EntryCipher>,
    channel_senders: Vec<ChannelSender<Message>>,
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
        };
        (wrapper, key)
    }
}

impl<T> HypercoreWrapper<T>
where
    T: RandomAccess + Debug + Send + 'static,
{
    pub(crate) async fn append_batch(
        &mut self,
        data_batch: Vec<Vec<u8>>,
    ) -> Result<u64, PeermergeError> {
        if self.proxy {
            panic!("Can not append to a proxy");
        }
        let outcome = {
            let mut hypercore = self.hypercore.lock().await;
            if let Some(entry_cipher) = &self.entry_cipher {
                let original_length = hypercore.info().length;
                let mut encrypted_data_array: Vec<Vec<u8>> = Vec::with_capacity(data_batch.len());
                for (i, data) in data_batch.iter().enumerate() {
                    encrypted_data_array.push(entry_cipher.encrypt(
                        &self.public_key,
                        original_length + i as u64,
                        data,
                    ))
                }
                hypercore.append_batch(&encrypted_data_array).await?
            } else {
                hypercore.append_batch(&data_batch).await?
            }
        };
        if !self.channel_senders.is_empty() {
            let message = create_append_local_signal(outcome.length);
            self.notify_listeners(&message).await?;
        }
        Ok(outcome.length)
    }

    pub(crate) async fn contiguous_length(&self) -> u64 {
        let hypercore = self.hypercore.lock().await;
        hypercore.info().contiguous_length
    }

    pub(crate) async fn notify_feed_synced(
        &mut self,
        contiguous_length: u64,
    ) -> Result<(), PeermergeError> {
        if !self.channel_senders.is_empty() {
            let message = create_feed_synced_local_signal(contiguous_length);
            self.notify_listeners(&message).await?;
        }
        Ok(())
    }

    pub(crate) async fn notify_feeds_changed(
        &mut self,
        doc_discovery_key: FeedDiscoveryKey,
        replaced_feeds: Vec<DocumentFeedInfo>,
        feeds_to_create: Vec<DocumentFeedInfo>,
    ) -> Result<(), PeermergeError> {
        if !self.channel_senders.is_empty() {
            let message = create_feeds_changed_local_signal(
                doc_discovery_key,
                replaced_feeds,
                feeds_to_create,
            );
            self.notify_listeners(&message).await?;
        }
        Ok(())
    }

    pub(crate) async fn notify_closed(&mut self) -> Result<(), PeermergeError> {
        if !self.channel_senders.is_empty() {
            let message = create_closed_local_signal();
            self.notify_listeners(&message).await?;
        }
        Ok(())
    }

    /// Gets entries. Merges DocParts into InitDoc and InitPeer, which means the number
    /// of entries returned may be less than the given range. Returns the entries and
    /// an offset in the feed that can be used for the first entry (meaning if
    /// [InitDoc, DocPart, DocPart, Change], then it's 2, if [InitDoc, Change] then 0,
    /// if [Change, Change], then 0).
    pub(crate) async fn entries(
        &mut self,
        index: u64,
        len: u64,
    ) -> Result<ShrunkEntries, PeermergeError> {
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
            let entry: Entry = dec_state.decode(&data)?;
            entries.push(entry);
        }
        Ok(shrink_entries(entries))
    }

    pub(crate) async fn key_pair(&self) -> PartialKeypair {
        let hypercore = self.hypercore.lock().await;
        hypercore.key_pair().clone()
    }

    pub(super) fn public_key(&self) -> &[u8; 32] {
        &self.public_key
    }

    #[instrument(level = "debug", skip_all)]
    pub(super) fn on_channel(
        &mut self,
        is_doc: bool,
        peers_state: Option<DocumentFeedsState>,
        peer_id: Option<PeerId>,
        doc_discovery_key: FeedDiscoveryKey,
        channel: Channel,
        channel_receiver: ChannelReceiver<Message>,
        channel_sender: ChannelSender<Message>,
        feed_event_sender: &mut UnboundedSender<FeedEvent>,
    ) {
        debug!("Processing channel id={}", channel.id(),);
        self.channel_senders.push(channel_sender);
        let peer_state = PeerState::new(is_doc, doc_discovery_key, peers_state, peer_id);
        let hypercore = self.hypercore.clone();
        let mut feed_event_sender_for_task = feed_event_sender.clone();
        let task_span = if is_doc {
            tracing::debug_span!("call_on_doc_peer").or_current()
        } else {
            tracing::debug_span!("call_on_peer").or_current()
        };
        #[cfg(not(target_arch = "wasm32"))]
        task::spawn(async move {
            let _entered = task_span.enter();
            if is_doc {
                on_doc_feed(
                    hypercore,
                    peer_state,
                    channel,
                    channel_receiver,
                    &mut feed_event_sender_for_task,
                )
                .await
                .expect("doc peer connect failed");
            } else {
                on_feed(
                    hypercore,
                    peer_state,
                    channel,
                    channel_receiver,
                    &mut feed_event_sender_for_task,
                )
                .await
                .expect("peer connect failed");
            }
        });
        #[cfg(target_arch = "wasm32")]
        spawn_local(async move {
            let _entered = task_span.enter();
            if is_doc {
                on_doc_feed(
                    hypercore,
                    peer_state,
                    channel,
                    channel_receiver,
                    &mut feed_event_sender_for_task,
                )
                .await
                .expect("doc peer connect failed");
            } else {
                on_feed(
                    hypercore,
                    peer_state,
                    channel,
                    channel_receiver,
                    &mut feed_event_sender_for_task,
                )
                .await
                .expect("peer connect failed");
            }
        });
    }

    async fn notify_listeners(&mut self, message: &Message) -> Result<(), PeermergeError> {
        let mut closed_indices: Vec<usize> = vec![];
        for i in 0..self.channel_senders.len() {
            if self.channel_senders[i].is_closed() {
                closed_indices.push(i);
            } else {
                let message = message.clone();
                self.channel_senders[i].send(message).await?;
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
