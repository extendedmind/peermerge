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

use super::{
    messaging::{FEEDS_CHANGED_LOCAL_SIGNAL_NAME, FEED_VERIFICATION_LOCAL_SIGNAL_NAME},
    HypercoreWrapper,
};
use crate::common::state::DocumentFeedsState;
use crate::common::utils::Mutex;
use crate::common::{message::FeedVerificationMessage, state::ChildDocumentInfo};
use crate::common::{message::FeedsChangedMessage, FeedEvent};
use crate::document::{get_document, get_document_by_discovery_key, get_document_ids, Document};
use crate::{common::keys::discovery_key_from_public_key, feeds::FeedDiscoveryKey};
use crate::{DocumentId, FeedPersistence, PeerId, PeermergeError, IO};

#[instrument(level = "debug", skip_all, fields(is_initiator = protocol.is_initiator()))]
pub(crate) async fn on_protocol<T, U, V>(
    protocol: &mut Protocol<V>,
    documents: Arc<DashMap<DocumentId, Document<T, U>>>,
    feed_event_sender: &mut UnboundedSender<FeedEvent>,
) -> Result<(), PeermergeError>
where
    T: RandomAccess + Debug + Send + 'static,
    U: FeedPersistence,
    V: IO,
{
    let is_initiator = protocol.is_initiator();

    debug!("Begin listening to protocol events");
    // Stores discovery keys that have been received only via hypercore's
    // Event::DiscoveryKey, but which haven't yet been broadcasted and thus
    // hypercores not yet created either.
    let mut unbound_discovery_keys: Vec<FeedDiscoveryKey> = vec![];
    // Stores discovery keys have been broadcasted and hypercores created but
    // which are not yet opened because doc feed has not been verified yet.
    let mut discovery_keys_to_open: Vec<FeedDiscoveryKey> = vec![];
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
                            // On handshake, we can only open the doc hypercores, because
                            // it is not known which of our documents the other side knowns about.
                            for document_id in get_document_ids(&documents).await {
                                let document =
                                    get_document(&documents, &document_id).await.unwrap();
                                let doc_hypercore = document.doc_feed().await;
                                let doc_hypercore = doc_hypercore.lock().await;
                                debug!("Event:Handshake: opening doc channel");
                                protocol.open(*doc_hypercore.public_key()).await?;
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
                        if let Some((document, hypercore, is_doc)) =
                            get_document_and_openeable_hypercore_for_discovery_key(
                                discovery_key,
                                &documents,
                                &opened_documents,
                            )
                            .await
                        {
                            if is_doc {
                                opened_documents.push(*discovery_key);
                                if document.doc_feed_verified().await {
                                    if is_initiator {
                                        // Now that the doc channel is open, we can open channels for the write feed and peer feeds
                                        let active_feeds = document.active_feeds().await;
                                        for active_feed in active_feeds {
                                            let active_feed = active_feed.lock().await;
                                            debug!("Event:Channel: opening active feed");
                                            protocol.open(*active_feed.public_key()).await?;
                                        }
                                    }
                                } else {
                                    // Doc feed is not verified, need to wait for the others,
                                    // tolerate unverified.
                                    let active_peer_feeds_discovery_keys =
                                        document.active_feeds_discovery_keys().await;
                                    discovery_keys_to_open.extend(active_peer_feeds_discovery_keys);
                                }
                            }
                            let (feeds_state, child_documents, peer_id): (
                                Option<DocumentFeedsState>,
                                Vec<ChildDocumentInfo>,
                                Option<PeerId>,
                            ) = if is_doc {
                                let (feeds_state, child_documents) =
                                    document.feeds_state_and_child_documents().await;
                                (Some(feeds_state), child_documents, None)
                            } else {
                                (
                                    None,
                                    vec![],
                                    Some(document.peer_id_from_discovery_key(discovery_key).await),
                                )
                            };

                            let mut hypercore = hypercore.lock().await;
                            let channel_receiver = channel.take_receiver().unwrap();
                            let channel_sender = channel.local_sender();
                            hypercore.on_channel(
                                is_doc,
                                feeds_state,
                                child_documents,
                                peer_id,
                                document.doc_discovery_key(),
                                document.doc_signature_verifying_key(),
                                channel,
                                channel_receiver,
                                channel_sender,
                                feed_event_sender,
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
                            // When all of the documents' doc feeds have been closed, the
                            // protocol can also be closed.
                            break;
                        }
                    }
                    Event::LocalSignal((name, data)) => match name.as_str() {
                        FEEDS_CHANGED_LOCAL_SIGNAL_NAME => {
                            let mut dec_state = State::from_buffer(&data);
                            let message: FeedsChangedMessage = dec_state.decode(&data)?;
                            let document = get_document_by_discovery_key(
                                &documents,
                                &message.doc_discovery_key,
                            )
                            .await
                            .unwrap();

                            let new_discovery_keys_to_open: Vec<[u8; 32]> = message
                                .feeds_to_create
                                .iter()
                                .map(|peer| discovery_key_from_public_key(&peer.public_key))
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
                            discovery_keys_to_open.extend(new_discovery_keys_to_open);
                            if document.doc_feed_verified().await {
                                for discovery_key in discovery_keys_to_open
                                    .drain(..discovery_keys_to_open.len())
                                    .into_iter()
                                    .collect::<Vec<FeedDiscoveryKey>>()
                                {
                                    if let Some(hypercore) =
                                        document.active_feed(&discovery_key).await
                                    {
                                        let hypercore = hypercore.lock().await;
                                        protocol.open(*hypercore.public_key()).await?;
                                    } else {
                                        panic!(
                                            "Could not find new hypercore with discovery key {discovery_key:02X?}",
                                        );
                                    };
                                }
                            }
                        }
                        FEED_VERIFICATION_LOCAL_SIGNAL_NAME => {
                            let mut dec_state = State::from_buffer(&data);
                            let message: FeedVerificationMessage = dec_state.decode(&data)?;
                            if message.verified && message.peer_id.is_none() {
                                // The doc peer was verified, only now ready to start listening
                                // to all the other feeds
                                let document = get_document_by_discovery_key(
                                    &documents,
                                    &message.doc_discovery_key,
                                )
                                .await
                                .unwrap();
                                for discovery_key in discovery_keys_to_open
                                    .drain(..discovery_keys_to_open.len())
                                    .into_iter()
                                    .collect::<Vec<FeedDiscoveryKey>>()
                                {
                                    if let Some(hypercore) =
                                        document.active_feed(&discovery_key).await
                                    {
                                        let hypercore = hypercore.lock().await;
                                        protocol.open(*hypercore.public_key()).await?;
                                    } else {
                                        panic!(
                                            "Could not find new hypercore with discovery key {discovery_key:02X?}",
                                        );
                                    };
                                }
                            } else {
                                unimplemented!("TODO: Invalid feed deletion");
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
    if let Some(document) = get_document_by_discovery_key(documents, discovery_key).await {
        Some(document.doc_feed().await)
    } else {
        for opened_document_id in opened_documents {
            let document = get_document_by_discovery_key(documents, opened_document_id)
                .await
                .unwrap();
            let active_feed = document.active_feed(discovery_key).await;
            if active_feed.is_some() {
                return active_feed;
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
    if let Some(document) = get_document_by_discovery_key(documents, discovery_key).await {
        let doc_feed = document.doc_feed().await;
        Some((document, doc_feed, true))
    } else {
        for opened_document_id in opened_documents {
            let document = get_document_by_discovery_key(documents, opened_document_id)
                .await
                .unwrap();
            let active_feed = document.active_feed(discovery_key).await;
            if active_feed.is_some() {
                return active_feed.map(|feed| (document.clone(), feed, false));
            }
        }
        None
    }
}
