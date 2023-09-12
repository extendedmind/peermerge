use compact_encoding::{CompactEncoding, State};
use futures::channel::mpsc::UnboundedSender;
use hypercore_protocol::{
    hypercore::{Hypercore, Info, PartialKeypair, SigningKey, VerifyingKey},
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
        create_feed_verification_local_signal, create_feeds_changed_local_signal,
    },
    on_doc_feed, on_feed, PeerState,
};
use crate::{
    common::{
        cipher::{add_signature, verify_data_signature, EntryCipher},
        entry::{shrink_entries, Entry, ShrunkEntries},
        state::{ChildDocumentInfo, DocumentFeedInfo, DocumentFeedsState},
        utils::Mutex,
        FeedEvent,
    },
    feeds::FeedDiscoveryKey,
    AccessType, PeerId, PeermergeError,
};

#[derive(Debug)]
pub(crate) struct HypercoreWrapper<T>
where
    T: RandomAccess + Debug + Send,
{
    pub(super) public_key: [u8; 32],
    pub(super) hypercore: Arc<Mutex<Hypercore<T>>>,
    access_type: AccessType,
    entry_cipher: Option<EntryCipher>,
    channel_senders: Vec<ChannelSender<Message>>,
}

#[cfg(not(target_arch = "wasm32"))]
impl HypercoreWrapper<RandomAccessDisk> {
    pub(crate) fn from_disk_hypercore(
        hypercore: Hypercore<RandomAccessDisk>,
        access_type: AccessType,
        encrypted: bool,
        encryption_key: &Option<Vec<u8>>,
        generate_encryption_key_if_missing: bool,
    ) -> (Self, Option<Vec<u8>>) {
        let public_key = hypercore.key_pair().public.to_bytes();
        let (entry_cipher, key) = prepare_entry_cipher(
            access_type,
            encrypted,
            encryption_key,
            generate_encryption_key_if_missing,
        );
        let wrapper = HypercoreWrapper {
            public_key,
            hypercore: Arc::new(Mutex::new(hypercore)),
            access_type,
            entry_cipher,
            channel_senders: vec![],
        };
        (wrapper, key)
    }
}

impl HypercoreWrapper<RandomAccessMemory> {
    pub(crate) fn from_memory_hypercore(
        hypercore: Hypercore<RandomAccessMemory>,
        access_type: AccessType,
        encrypted: bool,
        encryption_key: &Option<Vec<u8>>,
        generate_encryption_key_if_missing: bool,
    ) -> (Self, Option<Vec<u8>>) {
        let public_key = hypercore.key_pair().public.to_bytes();
        let (entry_cipher, key) = prepare_entry_cipher(
            access_type,
            encrypted,
            encryption_key,
            generate_encryption_key_if_missing,
        );
        let wrapper = HypercoreWrapper {
            public_key,
            hypercore: Arc::new(Mutex::new(hypercore)),
            access_type,
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
        doc_signature_signing_key: &SigningKey,
    ) -> Result<u64, PeermergeError> {
        if self.access_type != AccessType::ReadWrite {
            panic!("Can not append to a proxy or read-only feed");
        }
        let outcome = {
            let mut hypercore = self.hypercore.lock().await;
            let original_length = hypercore.info().length;
            let mut final_data_batch: Vec<Vec<u8>> = if let Some(entry_cipher) = &self.entry_cipher
            {
                let mut encrypted_data_array: Vec<Vec<u8>> = Vec::with_capacity(data_batch.len());
                for (i, data) in data_batch.iter().enumerate() {
                    encrypted_data_array.push(entry_cipher.encrypt(
                        &self.public_key,
                        original_length + i as u64,
                        data,
                    ))
                }
                encrypted_data_array
            } else {
                data_batch
            };
            if original_length == 0 && !final_data_batch.is_empty() {
                // Sign the first entry
                let first_entry = final_data_batch.get_mut(0).unwrap();
                add_signature(first_entry, doc_signature_signing_key);
            }
            hypercore.append_batch(&final_data_batch).await?
        };
        if !self.channel_senders.is_empty() {
            let message = create_append_local_signal(outcome.length);
            self.notify_listeners(&message).await?;
        }
        Ok(outcome.length)
    }

    pub(crate) async fn info(&self) -> Info {
        let hypercore = self.hypercore.lock().await;
        hypercore.info()
    }

    pub(crate) async fn verify_first_entry(
        &self,
        doc_signature_verifying_key: &VerifyingKey,
    ) -> Result<(), PeermergeError> {
        let mut hypercore = self.hypercore.lock().await;
        let first_entry = hypercore.get(0).await?.unwrap();
        verify_data_signature(&first_entry, doc_signature_verifying_key)?;
        Ok(())
    }

    pub(crate) async fn notify_feed_synced(
        &mut self,
        contiguous_length: u64,
        not_created_child_documents: Vec<ChildDocumentInfo>,
    ) -> Result<(), PeermergeError> {
        if !self.channel_senders.is_empty() {
            let message =
                create_feed_synced_local_signal(contiguous_length, not_created_child_documents);
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

    pub(crate) async fn notify_feed_verification(
        &mut self,
        doc_discovery_key: &FeedDiscoveryKey,
        feed_discovery_key: &FeedDiscoveryKey,
        verified: bool,
        peer_id: &Option<PeerId>,
    ) -> Result<(), PeermergeError> {
        if !self.channel_senders.is_empty() {
            let message = create_feed_verification_local_signal(
                *doc_discovery_key,
                *feed_discovery_key,
                verified,
                *peer_id,
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
        if self.access_type == AccessType::Proxy {
            panic!("Can not get entries from a proxy");
        }
        let mut hypercore = self.hypercore.lock().await;
        let mut entries: Vec<Entry> = vec![];
        for i in index..len {
            let mut feed_data = hypercore.get(i).await.unwrap().unwrap();
            let data = if i == 0 {
                // The first entry is signed, only decrypt part of the data
                if feed_data.len() <= 64 {
                    return Err(PeermergeError::InvalidOperation {
                        context: "Feed contains too short first entry".to_string(),
                    });
                }
                feed_data.drain(feed_data.len() - 64..feed_data.len());
                feed_data
            } else {
                feed_data
            };
            let plain_data: Vec<u8> = if let Some(entry_cipher) = &self.entry_cipher {
                entry_cipher.decrypt(&self.public_key, i, &data)
            } else {
                data
            };
            let mut dec_state = State::from_buffer(&plain_data);
            let entry: Entry = dec_state.decode(&plain_data)?;
            entries.push(entry);
        }
        Ok(shrink_entries(entries))
    }

    pub(crate) async fn key_pair(&self) -> PartialKeypair {
        let hypercore = self.hypercore.lock().await;
        hypercore.key_pair().clone()
    }

    pub(crate) async fn make_read_only(&mut self) -> Result<bool, PeermergeError> {
        let mut hypercore = self.hypercore.lock().await;
        let result = hypercore.make_read_only().await?;
        self.access_type = AccessType::ReadOnly;
        Ok(result)
    }

    pub(super) fn public_key(&self) -> &[u8; 32] {
        &self.public_key
    }

    #[instrument(level = "debug", skip_all)]
    pub(super) fn on_channel(
        &mut self,
        is_doc: bool,
        peers_state: Option<DocumentFeedsState>,
        child_documents: Vec<ChildDocumentInfo>,
        peer_id: Option<PeerId>,
        doc_discovery_key: FeedDiscoveryKey,
        doc_signature_verifying_key: VerifyingKey,
        channel: Channel,
        channel_receiver: ChannelReceiver<Message>,
        channel_sender: ChannelSender<Message>,
        feed_event_sender: &mut UnboundedSender<FeedEvent>,
    ) {
        debug!("Processing channel id={}", channel.id(),);
        self.channel_senders.push(channel_sender);
        let peer_state = PeerState::new(
            is_doc,
            doc_discovery_key,
            doc_signature_verifying_key,
            peers_state,
            child_documents,
            peer_id,
        );
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
    access_type: AccessType,
    encrypted: bool,
    encryption_key: &Option<Vec<u8>>,
    generate_encryption_key_if_missing: bool,
) -> (Option<EntryCipher>, Option<Vec<u8>>) {
    if access_type != AccessType::Proxy && encrypted {
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
