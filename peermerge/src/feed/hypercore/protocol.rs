use dashmap::DashMap;
use futures::channel::mpsc::UnboundedSender;
use futures::{AsyncRead, AsyncWrite, StreamExt};
use hypercore_protocol::hypercore::compact_encoding::{CompactEncoding, State};
use hypercore_protocol::{Event, Protocol};
use random_access_storage::RandomAccess;
use std::collections::HashMap;
use std::fmt::Debug;
use std::io::ErrorKind;
use std::sync::Arc;
use tracing::{debug, instrument};

#[cfg(feature = "async-std")]
use async_std::sync::Mutex;
#[cfg(feature = "tokio")]
use tokio::sync::Mutex;

use super::{messaging::NEW_PEERS_CREATED_LOCAL_SIGNAL_NAME, HypercoreWrapper};
use crate::common::keys::discovery_key_from_public_key;
use crate::common::{message::NewPeersCreatedMessage, storage::DocStateWrapper, PeerEvent};
use crate::document::Document;
use crate::{DocumentId, FeedPersistence, IO};

#[instrument(
    level = "debug",
    skip(protocol, doc_state, hypercores, peer_event_sender)
)]
pub(crate) async fn on_protocol<T, IO>(
    protocol: &mut Protocol<IO>,
    doc_state: Arc<Mutex<DocStateWrapper<T>>>,
    hypercores: Arc<DashMap<[u8; 32], Arc<Mutex<HypercoreWrapper<T>>>>>,
    peer_event_sender: &mut UnboundedSender<PeerEvent>,
) -> anyhow::Result<()>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send + 'static,
    IO: AsyncWrite + AsyncRead + Send + Unpin + 'static,
{
    let is_initiator = protocol.is_initiator();
    let doc_discovery_key = { doc_state.lock().await.state().doc_discovery_key.clone() };

    debug!("Begin listening to protocol events");
    let mut unbound_discovery_keys: Vec<[u8; 32]> = vec![];
    while let Some(event) = protocol.next().await {
        debug!("Got protocol event {:?}", event);
        match event {
            Err(err) => {
                if err.kind() == ErrorKind::BrokenPipe {
                    // Ignore broken pipe, can happen when the other end closes shop
                    break;
                }
                return Err(anyhow::anyhow!(err));
            }
            Ok(event) => {
                match event {
                    Event::Handshake(_) => {
                        if is_initiator {
                            for hypercore in hypercores.iter() {
                                let hypercore = hypercore.lock().await;
                                debug!("Event:Handshake: opening protocol");
                                protocol.open(hypercore.public_key().clone()).await?;
                            }
                        }
                    }
                    Event::DiscoveryKey(dkey) => {
                        if let Some(hypercore) = hypercores.get(&dkey) {
                            unbound_discovery_keys.retain(|key| key != &dkey);
                            let hypercore = hypercore.lock().await;
                            protocol.open(hypercore.public_key().clone()).await?;
                        } else {
                            unbound_discovery_keys.push(dkey);
                        }
                    }
                    Event::Channel(mut channel) => {
                        debug!("Event:Channel: id={}", channel.id());
                        let discovery_key = channel.discovery_key();
                        if let Some(hypercore) = hypercores.get(discovery_key) {
                            let mut hypercore = hypercore.lock().await;
                            let (write_public_key, peer_public_keys) =
                                public_keys(doc_state.clone()).await;
                            let is_doc_channel = discovery_key == &doc_discovery_key;
                            let channel_receiver = channel.take_receiver().unwrap();
                            let channel_sender = channel.local_sender();
                            hypercore.on_channel(
                                is_doc_channel,
                                doc_discovery_key,
                                channel,
                                channel_receiver,
                                channel_sender,
                                write_public_key,
                                peer_public_keys,
                                peer_event_sender,
                            );
                        } else {
                            panic!(
                                "Could not find hypercore with discovery key {:02X?}",
                                discovery_key
                            );
                        }
                    }
                    Event::Close(_discovery_key) => {
                        // For now, just ignore
                    }
                    Event::LocalSignal((name, data)) => match name.as_str() {
                        NEW_PEERS_CREATED_LOCAL_SIGNAL_NAME => {
                            let mut dec_state = State::from_buffer(&data);
                            let message: NewPeersCreatedMessage = dec_state.decode(&data);
                            let discovery_keys_to_open: Vec<[u8; 32]> = message
                                .public_keys
                                .iter()
                                .map(|public_key| discovery_key_from_public_key(public_key))
                                .filter(|discovery_key| {
                                    if is_initiator {
                                        true
                                    } else {
                                        // Only open protocol to those that have previously
                                        // been announced.
                                        if unbound_discovery_keys.contains(&discovery_key) {
                                            unbound_discovery_keys
                                                .retain(|key| key != discovery_key);
                                            true
                                        } else {
                                            false
                                        }
                                    }
                                })
                                .collect();
                            for discovery_key in discovery_keys_to_open {
                                if let Some(hypercore) = hypercores.get(&discovery_key) {
                                    let hypercore = hypercore.lock().await;
                                    protocol.open(hypercore.public_key().clone()).await?;
                                } else {
                                    panic!(
                                        "Could not find new hypercore with discovery key {:02X?}",
                                        discovery_key
                                    );
                                };
                            }
                        }
                        _ => panic!("Unknown local signal {}", name),
                    },
                    _ => {}
                }
            }
        }
    }
    debug!("Exiting");
    Ok(())
}

#[instrument(level = "debug", skip_all, fields(is_initiator = protocol.is_initiator()))]
pub(crate) async fn on_protocol_new<T, U, V>(
    protocol: &mut Protocol<V>,
    documents: Arc<DashMap<DocumentId, Document<T, U>>>,
    peer_event_sender: &mut UnboundedSender<PeerEvent>,
) -> anyhow::Result<()>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send + 'static,
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
                return Err(anyhow::anyhow!(err));
            }
            Ok(event) => {
                match event {
                    Event::Handshake(_) => {
                        if is_initiator {
                            // On handshake, we can only open the root hypercores, because
                            // it is not known which of our documents the other side knowns about.
                            for document in documents.iter() {
                                let root_hypercore = document.root_feed();
                                let root_hypercore = root_hypercore.lock().await;
                                debug!("Event:Handshake: opening root channel");
                                protocol.open(root_hypercore.public_key().clone()).await?;
                            }
                        }
                    }
                    Event::DiscoveryKey(discovery_key) => {
                        if let Some(hypercore) = get_openeable_hypercore_for_discovery_key(
                            &discovery_key,
                            &documents,
                            &opened_documents,
                        ) {
                            unbound_discovery_keys.retain(|key| key != &discovery_key);
                            let hypercore = hypercore.lock().await;
                            protocol.open(hypercore.public_key().clone()).await?;
                        } else {
                            unbound_discovery_keys.push(discovery_key);
                        }
                    }
                    Event::Channel(mut channel) => {
                        debug!("Event:Channel: id={}", channel.id());
                        let discovery_key = channel.discovery_key();
                        if let Some((document, hypercore, is_root)) =
                            get_document_and_openeable_hypercore_for_discovery_key(
                                &discovery_key,
                                &documents,
                                &opened_documents,
                            )
                        {
                            if is_root {
                                opened_documents.push(discovery_key.clone());
                                let document = documents.get(discovery_key).unwrap();
                                if is_initiator {
                                    // Now that the root channel is open, we can open channels for the leaf feeds
                                    let leaf_feeds = document.leaf_feeds();
                                    for leaf_feed in leaf_feeds {
                                        let leaf_feed = leaf_feed.lock().await;
                                        debug!("Event:Handshake: opening leaf channel");
                                        protocol.open(leaf_feed.public_key().clone()).await?;
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
                                "Could not find hypercore with discovery key {:02X?}",
                                discovery_key
                            );
                        }
                    }
                    Event::Close(_discovery_key) => {
                        // For now, just ignore
                    }
                    Event::LocalSignal((name, data)) => match name.as_str() {
                        NEW_PEERS_CREATED_LOCAL_SIGNAL_NAME => {
                            let mut dec_state = State::from_buffer(&data);
                            let message: NewPeersCreatedMessage = dec_state.decode(&data);
                            let discovery_keys_to_open: Vec<[u8; 32]> = message
                                .public_keys
                                .iter()
                                .map(|public_key| discovery_key_from_public_key(public_key))
                                .filter(|discovery_key| {
                                    if is_initiator {
                                        true
                                    } else {
                                        // Only open protocol to those that have previously
                                        // been announced.
                                        if unbound_discovery_keys.contains(&discovery_key) {
                                            unbound_discovery_keys
                                                .retain(|key| key != discovery_key);
                                            true
                                        } else {
                                            false
                                        }
                                    }
                                })
                                .collect();
                            let document = documents.get(&message.doc_discovery_key).unwrap();
                            for discovery_key in discovery_keys_to_open {
                                if let Some(hypercore) = document.leaf_feed(&discovery_key) {
                                    let hypercore = hypercore.lock().await;
                                    protocol.open(hypercore.public_key().clone()).await?;
                                } else {
                                    panic!(
                                        "Could not find new hypercore with discovery key {:02X?}",
                                        discovery_key
                                    );
                                };
                            }
                        }
                        _ => panic!("Unknown local signal {}", name),
                    },
                    _ => {}
                }
            }
        }
    }
    debug!("Exiting");
    Ok(())
}

fn get_openeable_hypercore_for_discovery_key<T, U>(
    discovery_key: &[u8; 32],
    documents: &Arc<DashMap<DocumentId, Document<T, U>>>,
    opened_documents: &Vec<DocumentId>,
) -> Option<Arc<Mutex<HypercoreWrapper<U>>>>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send + 'static,
    U: FeedPersistence,
{
    if let Some(document) = documents.get(discovery_key) {
        Some(document.root_feed())
    } else {
        for opened_document_id in opened_documents {
            let document = documents.get(opened_document_id).unwrap();
            let leaf_feed = document.leaf_feed(discovery_key);
            if leaf_feed.is_some() {
                return leaf_feed;
            }
        }
        None
    }
}

fn get_document_and_openeable_hypercore_for_discovery_key<T, U>(
    discovery_key: &[u8; 32],
    documents: &Arc<DashMap<DocumentId, Document<T, U>>>,
    opened_documents: &Vec<DocumentId>,
) -> Option<(Document<T, U>, Arc<Mutex<HypercoreWrapper<U>>>, bool)>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send + 'static,
    U: FeedPersistence,
{
    if let Some(multi) = documents.get(discovery_key) {
        Some((multi.value().clone(), multi.value().root_feed(), true))
    } else {
        for opened_document_id in opened_documents {
            let document = documents.get(opened_document_id).unwrap();
            let leaf_feed = document.leaf_feed(discovery_key);
            if leaf_feed.is_some() {
                return leaf_feed.map(|feed| (document.clone(), feed, false));
            }
        }
        None
    }
}

async fn public_keys<T>(
    doc_state: Arc<Mutex<DocStateWrapper<T>>>,
) -> (Option<[u8; 32]>, Vec<[u8; 32]>)
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send + 'static,
{
    let state = doc_state.lock().await;
    let state = state.state();
    let peer_public_keys: Vec<[u8; 32]> = state
        .peers
        .iter()
        .map(|peer| peer.public_key.clone())
        .collect();
    (state.write_public_key, peer_public_keys)
}
