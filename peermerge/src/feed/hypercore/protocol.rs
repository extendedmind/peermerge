use compact_encoding::{CompactEncoding, State};
use dashmap::DashMap;
use futures::channel::mpsc::UnboundedSender;
use futures::StreamExt;
use hypercore_protocol::{Event, Protocol};
use random_access_storage::RandomAccess;
use std::fmt::Debug;
use std::io::ErrorKind;
use std::sync::Arc;
use tracing::{debug, instrument};

use super::{messaging::NEW_PEERS_CREATED_LOCAL_SIGNAL_NAME, HypercoreWrapper};
use crate::common::keys::discovery_key_from_public_key;
use crate::common::utils::Mutex;
use crate::common::{message::NewPeersCreatedMessage, PeerEvent};
use crate::document::{get_document, get_document_ids, Document};
use crate::{DocumentId, FeedPersistence, PeermergeError, IO};

#[instrument(level = "debug", skip_all, fields(is_initiator = protocol.is_initiator()))]
pub(crate) async fn on_protocol<T, U, V>(
    protocol: &mut Protocol<V>,
    documents: Arc<DashMap<DocumentId, Document<T, U>>>,
    peer_event_sender: &mut UnboundedSender<PeerEvent>,
) -> Result<(), PeermergeError>
where
    T: RandomAccess + Debug + Send + 'static,
    U: FeedPersistence,
    V: IO,
{
    let is_initiator = protocol.is_initiator();

    debug!("Begin listening to protocol events");
    let mut unbound_discovery_keys: Vec<[u8; 32]> = vec![];
    let mut opened_documents: Vec<DocumentId> = vec![];
    while let Some(event) = protocol.next().await {
        debug!("Got protocol event {:?}", event);
        match event {
            Err(err) => {
                if err.kind() == ErrorKind::BrokenPipe {
                    // Ignore broken pipe, can happen when the other end closes shop
                    break;
                }
                return Err(PeermergeError::IO {
                    context: Some("Unexpected protocol error".to_string()),
                    source: err,
                });
            }
            Ok(event) => {
                match event {
                    Event::Handshake(_) => {
                        if is_initiator {
                            // On handshake, we can only open the root hypercores, because
                            // it is not known which of our documents the other side knowns about.
                            for document_id in get_document_ids(&documents).await {
                                let document =
                                    get_document(&documents, &document_id).await.unwrap();
                                let root_hypercore = document.root_feed().await;
                                let root_hypercore = root_hypercore.lock().await;
                                debug!("Event:Handshake: opening root channel");
                                protocol.open(*root_hypercore.public_key()).await?;
                            }
                        }
                    }
                    Event::DiscoveryKey(discovery_key) => {
                        if let Some(hypercore) = get_openeable_hypercore_for_discovery_key(
                            &discovery_key,
                            &documents,
                            &opened_documents,
                        )
                        .await
                        {
                            unbound_discovery_keys.retain(|key| key != &discovery_key);
                            let hypercore = hypercore.lock().await;
                            protocol.open(*hypercore.public_key()).await?;
                        } else {
                            unbound_discovery_keys.push(discovery_key);
                        }
                    }
                    Event::Channel(mut channel) => {
                        debug!("Event:Channel: id={}", channel.id());
                        let discovery_key = channel.discovery_key();
                        if let Some((document, hypercore, is_root)) =
                            get_document_and_openeable_hypercore_for_discovery_key(
                                discovery_key,
                                &documents,
                                &opened_documents,
                            )
                            .await
                        {
                            if is_root {
                                opened_documents.push(*discovery_key);
                                let document =
                                    get_document(&documents, discovery_key).await.unwrap();
                                if is_initiator {
                                    // Now that the root channel is open, we can open channels for the leaf feeds
                                    let leaf_feeds = document.leaf_feeds().await;
                                    for leaf_feed in leaf_feeds {
                                        let leaf_feed = leaf_feed.lock().await;
                                        debug!("Event:Handshake: opening leaf channel");
                                        protocol.open(*leaf_feed.public_key()).await?;
                                    }
                                }
                            }
                            let mut hypercore = hypercore.lock().await;
                            let (write_public_key, peer_public_keys) = document.public_keys().await;
                            let channel_receiver = channel.take_receiver().unwrap();
                            let channel_sender = channel.local_sender();
                            hypercore.on_channel(
                                is_root,
                                document.id(),
                                channel,
                                channel_receiver,
                                channel_sender,
                                write_public_key,
                                peer_public_keys,
                                peer_event_sender,
                            );
                        } else {
                            panic!(
                                "Could not find hypercore with discovery key {discovery_key:02X?}",
                            );
                        }
                    }
                    Event::Close(discovery_key) => {
                        if let Some(index) = opened_documents
                            .iter()
                            .position(|&doc_id| doc_id == discovery_key)
                        {
                            opened_documents.remove(index);
                        }
                        if opened_documents.is_empty() {
                            // When all of the documents' root feeds have been closed, the
                            // protocol can also be closed.
                            break;
                        }
                    }
                    Event::LocalSignal((name, data)) => match name.as_str() {
                        NEW_PEERS_CREATED_LOCAL_SIGNAL_NAME => {
                            let mut dec_state = State::from_buffer(&data);
                            let message: NewPeersCreatedMessage = dec_state.decode(&data)?;
                            let discovery_keys_to_open: Vec<[u8; 32]> = message
                                .public_keys
                                .iter()
                                .map(discovery_key_from_public_key)
                                .filter(|discovery_key| {
                                    if is_initiator {
                                        true
                                    } else {
                                        // Only open protocol to those that have previously
                                        // been announced.
                                        if unbound_discovery_keys.contains(discovery_key) {
                                            unbound_discovery_keys
                                                .retain(|key| key != discovery_key);
                                            true
                                        } else {
                                            false
                                        }
                                    }
                                })
                                .collect();
                            let document = get_document(&documents, &message.doc_discovery_key)
                                .await
                                .unwrap();
                            for discovery_key in discovery_keys_to_open {
                                if let Some(hypercore) = document.leaf_feed(&discovery_key).await {
                                    let hypercore = hypercore.lock().await;
                                    protocol.open(*hypercore.public_key()).await?;
                                } else {
                                    panic!(
                                        "Could not find new hypercore with discovery key {discovery_key:02X?}",
                                    );
                                };
                            }
                        }
                        _ => panic!("Unknown local signal: {name}"),
                    },
                    _ => {}
                }
            }
        }
    }
    debug!("Exiting");
    Ok(())
}

async fn get_openeable_hypercore_for_discovery_key<T, U>(
    discovery_key: &[u8; 32],
    documents: &Arc<DashMap<DocumentId, Document<T, U>>>,
    opened_documents: &Vec<DocumentId>,
) -> Option<Arc<Mutex<HypercoreWrapper<U>>>>
where
    T: RandomAccess + Debug + Send + 'static,
    U: FeedPersistence,
{
    if let Some(document) = get_document(documents, discovery_key).await {
        Some(document.root_feed().await)
    } else {
        for opened_document_id in opened_documents {
            let document = get_document(documents, opened_document_id).await.unwrap();
            let leaf_feed = document.leaf_feed(discovery_key).await;
            if leaf_feed.is_some() {
                return leaf_feed;
            }
        }
        None
    }
}

async fn get_document_and_openeable_hypercore_for_discovery_key<T, U>(
    discovery_key: &[u8; 32],
    documents: &Arc<DashMap<DocumentId, Document<T, U>>>,
    opened_documents: &Vec<DocumentId>,
) -> Option<(Document<T, U>, Arc<Mutex<HypercoreWrapper<U>>>, bool)>
where
    T: RandomAccess + Debug + Send + 'static,
    U: FeedPersistence,
{
    if let Some(document) = get_document(documents, discovery_key).await {
        let root_feed = document.root_feed().await;
        Some((document, root_feed, true))
    } else {
        for opened_document_id in opened_documents {
            let document = get_document(documents, opened_document_id).await.unwrap();
            let leaf_feed = document.leaf_feed(discovery_key).await;
            if leaf_feed.is_some() {
                return leaf_feed.map(|feed| (document.clone(), feed, false));
            }
        }
        None
    }
}
