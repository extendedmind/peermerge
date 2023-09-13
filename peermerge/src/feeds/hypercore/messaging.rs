use compact_encoding::{CompactEncoding, State};
use hypercore_protocol::{
    hypercore::{Hypercore, RequestBlock, RequestUpgrade},
    schema::*,
    Channel, Message,
};
use random_access_storage::RandomAccess;
use std::fmt::Debug;
use std::sync::Arc;
use tracing::{debug, instrument, warn};

use super::PeerState;
use crate::{
    common::{
        cipher::verify_data_signature,
        keys::discovery_key_from_public_key,
        message::{
            BroadcastMessage, FeedSyncedMessage, FeedVerificationMessage, FeedsChangedMessage,
        },
        state::{ChildDocumentInfo, DocumentFeedInfo, DocumentFeedsState},
        utils::Mutex,
        FeedEvent,
        FeedEventContent::*,
    },
    FeedDiscoveryKey, PeerId, PeermergeError,
};

// Messages sent over-the-wire
const PEERMERGE_BROADCAST_MSG: &str = "peermerge/v1/broadcast";

// Local signals
const APPEND_LOCAL_SIGNAL_NAME: &str = "append";
const FEED_SYNCED_LOCAL_SIGNAL_NAME: &str = "feed_synced";
pub(super) const FEEDS_CHANGED_LOCAL_SIGNAL_NAME: &str = "feeds_changed";
pub(super) const FEED_VERIFICATION_LOCAL_SIGNAL_NAME: &str = "feed_verification";
pub(super) const CHILD_DOCUMENT_CREATED_LOCAL_SIGNAL_NAME: &str = "child_document_created";
const CLOSED_LOCAL_SIGNAL_NAME: &str = "closed";

pub(super) fn create_broadcast_message(
    feeds_state: &DocumentFeedsState,
    child_documents: &[ChildDocumentInfo],
    active_feeds_to_include: &Vec<DocumentFeedInfo>,
    inactive_feeds: Option<Vec<DocumentFeedInfo>>,
) -> Message {
    // Take all verified feeds from the store
    let mut all_active_feeds: Vec<DocumentFeedInfo> = feeds_state
        .active_peer_feeds(false)
        .into_iter()
        .map(|(_, feed)| feed)
        .collect();

    // Then include also feeds given as parameter, i.e. feeds not-yet-verified
    // but just now created.
    for active_feed_to_include in active_feeds_to_include {
        if !all_active_feeds.contains(active_feed_to_include) {
            all_active_feeds.push(active_feed_to_include.clone());
        }
    }

    let broadcast_message: BroadcastMessage = BroadcastMessage {
        write_feed: feeds_state.write_feed.clone(),
        active_feeds: all_active_feeds,
        inactive_feeds,
        child_documents: child_documents.to_vec(),
    };
    let mut enc_state = State::new();
    enc_state
        .preencode(&broadcast_message)
        .expect("Pre-encoding broadcast message should not fail");
    let mut buffer = enc_state.create_buffer();
    enc_state
        .encode(&broadcast_message, &mut buffer)
        .expect("Encoding broadcast message should not fail");
    Message::Extension(Extension {
        name: PEERMERGE_BROADCAST_MSG.to_string(),
        message: buffer.to_vec(),
    })
}

pub(super) fn create_append_local_signal(length: u64) -> Message {
    let mut enc_state = State::new();
    enc_state
        .preencode(&length)
        .expect("Pre-encoding append local signal should not fail");
    let mut buffer = enc_state.create_buffer();
    enc_state
        .encode(&length, &mut buffer)
        .expect("Encoding append local signal should not fail");
    Message::LocalSignal((APPEND_LOCAL_SIGNAL_NAME.to_string(), buffer.to_vec()))
}

pub(super) fn create_feed_synced_local_signal(
    contiguous_length: u64,
    not_created_child_documents: Vec<ChildDocumentInfo>,
) -> Message {
    let message = FeedSyncedMessage::new(contiguous_length, not_created_child_documents);
    let mut enc_state = State::new();
    enc_state
        .preencode(&message)
        .expect("Pre-encoding peer synced local signal should not fail");
    let mut buffer = enc_state.create_buffer();
    enc_state
        .encode(&message, &mut buffer)
        .expect("Encoding peer synced local signal should not fail");
    Message::LocalSignal((FEED_SYNCED_LOCAL_SIGNAL_NAME.to_string(), buffer.to_vec()))
}

pub(super) fn create_feeds_changed_local_signal(
    doc_discovery_key: FeedDiscoveryKey,
    replaced_feeds: Vec<DocumentFeedInfo>,
    feeds_to_create: Vec<DocumentFeedInfo>,
) -> Message {
    let message = FeedsChangedMessage {
        doc_discovery_key,
        replaced_feeds,
        feeds_to_create,
    };
    let mut enc_state = State::new();
    enc_state
        .preencode(&message)
        .expect("Pre-encoding feeds changed local signal should not fail");
    let mut buffer = enc_state.create_buffer();
    enc_state
        .encode(&message, &mut buffer)
        .expect("Encoding feeds changed local signal should not fail");
    Message::LocalSignal((FEEDS_CHANGED_LOCAL_SIGNAL_NAME.to_string(), buffer.to_vec()))
}

pub(super) fn create_feed_verification_local_signal(
    doc_discovery_key: FeedDiscoveryKey,
    feed_discovery_key: FeedDiscoveryKey,
    verified: bool,
    peer_id: Option<PeerId>,
) -> Message {
    let message = FeedVerificationMessage {
        doc_discovery_key,
        feed_discovery_key,
        verified,
        peer_id,
    };
    let mut enc_state = State::new();
    enc_state
        .preencode(&message)
        .expect("Pre-encoding feed verification local signal should not fail");
    let mut buffer = enc_state.create_buffer();
    enc_state
        .encode(&message, &mut buffer)
        .expect("Encoding feed verification local signal should not fail");
    Message::LocalSignal((
        FEED_VERIFICATION_LOCAL_SIGNAL_NAME.to_string(),
        buffer.to_vec(),
    ))
}

pub(super) fn create_child_document_created_local_signal(
    child_document_info: ChildDocumentInfo,
) -> Message {
    let mut enc_state = State::new();
    enc_state
        .preencode(&child_document_info)
        .expect("Pre-encoding child document created local signal should not fail");
    let mut buffer = enc_state.create_buffer();
    enc_state
        .encode(&child_document_info, &mut buffer)
        .expect("Encoding child document created  local signal should not fail");
    Message::LocalSignal((
        CHILD_DOCUMENT_CREATED_LOCAL_SIGNAL_NAME.to_string(),
        buffer.to_vec(),
    ))
}

pub(super) fn create_closed_local_signal() -> Message {
    Message::LocalSignal((CLOSED_LOCAL_SIGNAL_NAME.to_string(), vec![]))
}

pub(super) async fn create_initial_synchronize<T>(
    hypercore: &mut Arc<Mutex<Hypercore<T>>>,
    peer_state: &mut PeerState,
) -> Vec<Message>
where
    T: RandomAccess + Debug + Send,
{
    // There are no new feeds, start sync
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

    if info.contiguous_length > 0 && peer_state.contiguous_range_sent < info.contiguous_length {
        let range_msg = Range {
            drop: false,
            start: 0,
            length: info.contiguous_length,
        };
        peer_state.contiguous_range_sent = info.contiguous_length;
        vec![Message::Synchronize(sync_msg), Message::Range(range_msg)]
    } else {
        vec![Message::Synchronize(sync_msg)]
    }
}

#[instrument(level = "debug", skip(hypercore, peer_state, channel))]
pub(super) async fn on_message<T>(
    hypercore: &mut Arc<Mutex<Hypercore<T>>>,
    peer_state: &mut PeerState,
    channel: &mut Channel,
    message: Message,
) -> Result<Vec<FeedEvent>, PeermergeError>
where
    T: RandomAccess + Debug + Send,
{
    debug!("Message on channel={}", channel.id(),);

    match message {
        Message::Synchronize(message) => {
            let length_changed = message.length != peer_state.remote_length;
            let info = {
                let hypercore = hypercore.lock().await;
                hypercore.info()
            };
            let same_fork = message.fork == info.fork;

            peer_state.remote_fork = message.fork;
            peer_state.remote_length = message.length;
            peer_state.remote_can_upgrade = message.can_upgrade;
            peer_state.remote_uploading = message.uploading;
            peer_state.remote_downloading = message.downloading;
            peer_state.length_acked = if same_fork { message.remote_length } else { 0 };

            let mut messages = vec![];

            if length_changed {
                // When the peer says it got a new length, we need to send another sync back that acknowledges the received sync
                let msg = Synchronize {
                    fork: info.fork,
                    length: info.length,
                    remote_length: peer_state.remote_length,
                    can_upgrade: peer_state.can_upgrade,
                    uploading: true,
                    downloading: true,
                };
                messages.push(Message::Synchronize(msg));
            }

            if peer_state.remote_length > info.length
                && peer_state.length_acked == info.length
                && length_changed
            {
                // Let's request an upgrade when the peer has said they have a new length, and
                // they've acked our length. NB: We can't know if the peer actually has any
                // blocks from this message: those are only possible to know from the Ranges they
                // send. Hence no block.
                let mut request = Request {
                    id: 0,
                    fork: info.fork,
                    hash: None,
                    block: None,
                    seek: None,
                    upgrade: Some(RequestUpgrade {
                        start: info.length,
                        length: peer_state.remote_length - info.length,
                    }),
                };
                peer_state.inflight.add(&mut request);
                messages.push(Message::Request(request));
            }

            channel.send_batch(&messages).await?;
        }
        Message::Request(message) => {
            let (info, proof) = {
                let mut hypercore = hypercore.lock().await;
                let proof = hypercore
                    .create_proof(message.block, message.hash, message.seek, message.upgrade)
                    .await?;
                (hypercore.info(), proof)
            };
            if let Some(proof) = proof {
                let msg = Data {
                    request: message.id,
                    fork: info.fork,
                    hash: proof.hash,
                    block: proof.block,
                    seek: proof.seek,
                    upgrade: proof.upgrade,
                };
                channel.send(Message::Data(msg)).await?;
            } else {
                panic!("Could not create proof from request id {:?}", message.id);
            }
        }
        Message::Data(message) => {
            let mut events: Vec<FeedEvent> = vec![];
            let (old_info, applied, new_info, request) = {
                let mut hypercore = hypercore.lock().await;
                let old_info = hypercore.info();
                let proof = message.clone().into_proof();
                let applied = hypercore.verify_and_apply_proof(&proof).await?;
                let new_info = hypercore.info();
                peer_state.inflight.remove(message.request);
                let is_fully_synced_doc_feed =
                    peer_state.is_doc && new_info.contiguous_length == new_info.length;
                if new_info.contiguous_length == 1 || is_fully_synced_doc_feed {
                    // Need to verify the signature of the feed immediately
                    // to close connection early if the feed offered is bogus.
                    // Also re-verify when doc feed is fully synced as only then
                    // is the successful verification sent to make sure everyone
                    // has the full doc feed before continuing with other feeds.
                    let first_entry = hypercore.get(0).await?.unwrap();
                    let discovery_key = *channel.discovery_key();
                    match verify_data_signature(
                        &first_entry,
                        &peer_state.doc_signature_verifying_key,
                    ) {
                        Ok(()) => {
                            if !peer_state.is_doc || is_fully_synced_doc_feed {
                                events.push(FeedEvent::new(
                                    peer_state.doc_discovery_key,
                                    FeedVerified {
                                        discovery_key,
                                        verified: true,
                                        peer_id: peer_state.peer_id,
                                    },
                                ));
                            }
                        }
                        Err(err) => {
                            warn!("Remote feed verification failed, {err}");
                            events.push(FeedEvent::new(
                                peer_state.doc_discovery_key,
                                FeedVerified {
                                    discovery_key,
                                    verified: true,
                                    peer_id: peer_state.peer_id,
                                },
                            ));
                            let channel_id = channel.id();
                            events.push(FeedEvent::new(
                                peer_state.doc_discovery_key,
                                FeedDisconnected {
                                    channel: channel_id as u64,
                                },
                            ));
                            // Return and disallow all other messages from coming
                            // through with disconnect without close.
                            return Ok(events);
                        }
                    }
                }

                let request = next_request(&mut hypercore, peer_state).await?;
                if new_info.contiguous_length == new_info.length {
                    peer_state.synced_contiguous_length = new_info.contiguous_length;
                    events.push(FeedEvent::new(
                        peer_state.doc_discovery_key,
                        FeedSynced {
                            peer_id: peer_state.peer_id,
                            discovery_key: *channel.discovery_key(),
                            contiguous_length: new_info.contiguous_length,
                        },
                    ));
                }
                (old_info, applied, new_info, request)
            };

            assert!(applied, "Could not apply proof");

            let mut messages: Vec<Message> = vec![];
            if let Some(upgrade) = &message.upgrade {
                let new_length = upgrade.length;

                let remote_length = if new_info.fork == peer_state.remote_fork {
                    peer_state.remote_length
                } else {
                    0
                };

                messages.push(Message::Synchronize(Synchronize {
                    fork: new_info.fork,
                    length: new_length,
                    remote_length,
                    can_upgrade: false,
                    uploading: true,
                    downloading: true,
                }));
            }
            if message.block.is_some() {
                // Send Range if the number of items changed for the contiguous length
                // only.
                if old_info.contiguous_length < new_info.contiguous_length
                    && peer_state.contiguous_range_sent < new_info.contiguous_length
                {
                    peer_state.contiguous_range_sent = new_info.contiguous_length;
                    messages.push(Message::Range(Range {
                        drop: false,
                        start: 0,
                        length: new_info.contiguous_length,
                    }));
                }
            }
            if let Some(request) = request {
                messages.push(Message::Request(request));
            }
            if !channel.closed() {
                channel.send_batch(&messages).await?;
            } else {
                events.push(FeedEvent::new(
                    peer_state.doc_discovery_key,
                    FeedDisconnected {
                        channel: channel.id() as u64,
                    },
                ));
            }
            return Ok(events);
        }
        Message::Range(message) => {
            let mut hypercore = hypercore.lock().await;
            let info = hypercore.info();
            if message.start == 0 {
                peer_state.remote_contiguous_length = message.length;
                if message.length > peer_state.remote_length {
                    peer_state.remote_length = message.length;
                }
                if info.contiguous_length == peer_state.remote_contiguous_length {
                    if peer_state.notified_remote_synced_contiguous_length
                        < peer_state.remote_contiguous_length
                    {
                        // The peer has advertised that they now have what we have
                        let event = FeedEvent::new(
                            peer_state.doc_discovery_key,
                            RemoteFeedSynced {
                                peer_id: peer_state.peer_id,
                                discovery_key: *channel.discovery_key(),
                                contiguous_length: peer_state.remote_contiguous_length,
                            },
                        );
                        peer_state.notified_remote_synced_contiguous_length =
                            peer_state.remote_contiguous_length;
                        return Ok(vec![event]);
                    }
                } else if info.contiguous_length < peer_state.remote_contiguous_length {
                    // If the other side advertises more than we have, we need to request the rest
                    // of the blocks.
                    if let Some(request) = next_request(&mut hypercore, peer_state).await? {
                        channel.send(Message::Request(request)).await?;
                    }
                } else {
                    // We have more than the peer, just ignore
                }
            } else {
                // TODO: For now, let's just ignore messages that don't indicate
                // a contiguous length. When we add support for deleting entries
                // from the hypercore, this needs proper bitfield support.
            }
        }
        Message::Extension(message) => match message.name.as_str() {
            PEERMERGE_BROADCAST_MSG => {
                assert!(
                    peer_state.is_doc,
                    "Only doc feed should ever get broadcast messages"
                );
                let mut feed_events: Vec<FeedEvent> = vec![];
                let feeds_state = peer_state.feeds_state.as_mut().unwrap();
                let mut dec_state = State::from_buffer(&message.message);
                let broadcast_message: BroadcastMessage = dec_state.decode(&message.message)?;
                if !broadcast_message.child_documents.is_empty() {
                    // Check if there are brand new documents broadcasted
                    let new_child_documents: Vec<ChildDocumentInfo> = broadcast_message
                        .child_documents
                        .into_iter()
                        .filter(|document| !peer_state.child_documents.contains(document))
                        .collect();
                    if !new_child_documents.is_empty() {
                        for new_child_document in &new_child_documents {
                            new_child_document.verify(&peer_state.doc_signature_verifying_key)?;
                        }
                        feed_events.push(FeedEvent::new(
                            peer_state.doc_discovery_key,
                            NewChildDocumentsBroadcasted {
                                new_child_documents,
                            },
                        ));
                    }
                }
                let compare_result = feeds_state.compare_broadcasted_feeds(
                    broadcast_message.write_feed,
                    broadcast_message.active_feeds,
                    broadcast_message.inactive_feeds,
                )?;
                if compare_result.wait_for_rebroadcast {
                    debug!("Expecting a re-broadcast with more information");
                }
                if compare_result.stored_active_feeds_found {
                    debug!("Remote has all of our stored active feeds");
                }
                if !compare_result.inactive_feeds_to_rebroadcast.is_empty() {
                    // Immedeately re-broadcast for the other end to get all the info
                    // that we know we should send.
                    let message = create_broadcast_message(
                        feeds_state,
                        &peer_state.child_documents,
                        &peer_state.broadcast_new_feeds,
                        Some(compare_result.inactive_feeds_to_rebroadcast),
                    );
                    channel.send(message).await?;
                }

                if !compare_result.new_feeds.is_empty() {
                    // Verify all new feeds broadcasted here before sending them
                    // forward. This prevents rogue peers from overwhelming the
                    // disk/memory storage.
                    for new_feed in &compare_result.new_feeds {
                        new_feed.verify(&peer_state.doc_signature_verifying_key)?;
                    }

                    // We can send the (possibly partial) new feeds immediately
                    // because (possible) duplicates from a re-broadcast will be
                    // removed at merge_new_feeds.
                    peer_state
                        .broadcast_new_feeds
                        .extend(compare_result.new_feeds.clone());
                    // New remote feeds found
                    feed_events.push(FeedEvent::new(
                        peer_state.doc_discovery_key,
                        NewFeedsBroadcasted {
                            new_feeds: compare_result.new_feeds,
                        },
                    ));
                }
                return Ok(feed_events);
            }
            _ => {
                panic!("Received unexpected extension message {message:?}");
            }
        },
        Message::LocalSignal((name, data)) => match name.as_str() {
            APPEND_LOCAL_SIGNAL_NAME => {
                let mut dec_state = State::from_buffer(&data);
                let length: u64 = dec_state.decode(&data)?;
                let info = {
                    let hypercore = hypercore.lock().await;
                    hypercore.info()
                };

                if info.contiguous_length >= length
                    && peer_state.contiguous_range_sent < info.contiguous_length
                {
                    let range_msg = Range {
                        drop: false,
                        start: 0,
                        length: info.contiguous_length,
                    };
                    peer_state.contiguous_range_sent = info.contiguous_length;
                    channel.send(Message::Range(range_msg)).await?;
                }
            }
            FEED_SYNCED_LOCAL_SIGNAL_NAME => {
                let mut dec_state = State::from_buffer(&data);
                let message: FeedSyncedMessage = dec_state.decode(&data)?;
                if message.contiguous_length > peer_state.synced_contiguous_length
                    && peer_state.contiguous_range_sent < message.contiguous_length
                {
                    // Let's send the actual contiguous length we have now, which may have
                    // changed since creating the signal.
                    let contiguous_length = {
                        let hypercore = hypercore.lock().await;
                        hypercore.info().contiguous_length
                    };
                    peer_state.synced_contiguous_length = contiguous_length;
                    let range_msg = Range {
                        drop: false,
                        start: 0,
                        length: contiguous_length,
                    };
                    peer_state.contiguous_range_sent = contiguous_length;
                    channel.send(Message::Range(range_msg)).await?;
                }
                if !message.not_created_child_documents.is_empty() {
                    // Let's just try again to create the documents now that we have more info.
                    return Ok(vec![FeedEvent::new(
                        peer_state.doc_discovery_key,
                        NewChildDocumentsBroadcasted {
                            new_child_documents: message.not_created_child_documents,
                        },
                    )]);
                }
            }
            FEEDS_CHANGED_LOCAL_SIGNAL_NAME => {
                assert!(
                    peer_state.is_doc,
                    "Only doc feed should ever get feeds changed messages"
                );
                let feeds_state = peer_state.feeds_state.as_mut().unwrap();
                let mut dec_state = State::from_buffer(&data);
                let feeds_changed_message: FeedsChangedMessage = dec_state.decode(&data)?;

                // Set replaced feeds and feeds to create to the feeds state
                feeds_state.set_replaced_feeds_and_feeds_to_create(
                    feeds_changed_message.replaced_feeds,
                    feeds_changed_message.feeds_to_create,
                );

                // Transmit this event forward to the protocol
                channel
                    .signal_local_protocol(FEEDS_CHANGED_LOCAL_SIGNAL_NAME, data)
                    .await?;

                // Create new broadcast message
                let messages = vec![create_broadcast_message(
                    feeds_state,
                    &peer_state.child_documents,
                    &peer_state.broadcast_new_feeds,
                    None,
                )];
                channel.send_batch(&messages).await?;
            }
            FEED_VERIFICATION_LOCAL_SIGNAL_NAME => {
                assert!(
                    peer_state.is_doc,
                    "Only doc feed should ever get feed verification messages"
                );
                let feeds_state = peer_state.feeds_state.as_mut().unwrap();
                let mut dec_state = State::from_buffer(&data);
                let message: FeedVerificationMessage = dec_state.decode(&data)?;
                if feeds_state.verify_feed(&message.feed_discovery_key, &message.peer_id)
                    && !channel.closed()
                {
                    if message.peer_id.is_none() || !message.verified {
                        // Transmit doc peer verification and failures forward to the protocol
                        channel
                            .signal_local_protocol(FEED_VERIFICATION_LOCAL_SIGNAL_NAME, data)
                            .await?;
                    }
                    if message.peer_id.is_some()
                        && !peer_state.broadcast_new_feeds.iter().any(|feed| {
                            discovery_key_from_public_key(&feed.public_key)
                                == message.feed_discovery_key
                        })
                    {
                        // Verification changed for one of the peer feeds, but not the ones that it
                        // itself sent (and which were already part of a previous broadcast) need to
                        // re-broadcast.
                        let message = create_broadcast_message(
                            feeds_state,
                            &peer_state.child_documents,
                            &peer_state.broadcast_new_feeds,
                            None,
                        );
                        channel.send(message).await?;
                    }
                }
            }
            CHILD_DOCUMENT_CREATED_LOCAL_SIGNAL_NAME => {
                assert!(
                    peer_state.is_doc,
                    "Only doc feed should ever get child document created messages"
                );
                let mut dec_state = State::from_buffer(&data);
                let child_document_info: ChildDocumentInfo = dec_state.decode(&data)?;
                peer_state.child_documents.push(child_document_info);
                let feeds_state = peer_state.feeds_state.as_ref().unwrap();
                let message = create_broadcast_message(
                    feeds_state,
                    &peer_state.child_documents,
                    &peer_state.broadcast_new_feeds,
                    None,
                );
                channel.send(message).await?;

                // Transmit this event forward to the protocol so that the new document can
                // be advertised
                channel
                    .signal_local_protocol(CHILD_DOCUMENT_CREATED_LOCAL_SIGNAL_NAME, data)
                    .await?;
            }
            CLOSED_LOCAL_SIGNAL_NAME => {
                assert!(
                    peer_state.is_doc,
                    "Only doc feed should ever get closed messages"
                );
                channel.close().await?;
            }
            _ => {
                panic!("Received unexpected local signal: {name}");
            }
        },
        Message::Close(message) => {
            return Ok(vec![FeedEvent::new(
                peer_state.doc_discovery_key,
                FeedDisconnected {
                    channel: message.channel,
                },
            )]);
        }
        _ => {
            panic!("Received unexpected message: {message:?}");
        }
    };
    Ok(vec![])
}

/// Return the next request that should be sent based on peer state.
async fn next_request<T>(
    hypercore: &mut Hypercore<T>,
    peer_state: &mut PeerState,
) -> Result<Option<Request>, PeermergeError>
where
    T: RandomAccess + Debug + Send,
{
    let info = hypercore.info();

    // Create an upgrade if needed
    let upgrade: Option<RequestUpgrade> = if peer_state.remote_length > info.length {
        // Check if there's already an inflight upgrade
        if let Some(upgrade) = peer_state.inflight.get_highest_upgrade() {
            if peer_state.remote_length > upgrade.start + upgrade.length {
                Some(RequestUpgrade {
                    start: upgrade.start,
                    length: peer_state.remote_length - upgrade.start,
                })
            } else {
                None
            }
        } else {
            Some(RequestUpgrade {
                start: info.length,
                length: peer_state.remote_length - info.length,
            })
        }
    } else {
        None
    };

    // Add blocks to block index queue if there's a new contiguous length
    let block: Option<RequestBlock> =
        if peer_state.remote_contiguous_length > info.contiguous_length {
            // Check if there's already an inflight block request
            let request_index: u64 = if let Some(block) = peer_state.inflight.get_highest_block() {
                block.index + 1
            } else {
                info.contiguous_length
            };
            // Don't ask for more blocks than the peer has, but also we can't ask for a block that we haven't
            // got an upgrade response for. With applied upgrade, the info.length increases, so we make
            // sure the index is below the length.
            if peer_state.remote_contiguous_length > request_index && info.length > request_index {
                let nodes = hypercore.missing_nodes(request_index * 2).await?;
                Some(RequestBlock {
                    index: request_index,
                    nodes,
                })
            } else {
                None
            }
        } else {
            None
        };

    if block.is_some() || upgrade.is_some() {
        let mut request = Request {
            id: 0, // This changes in the next step
            fork: info.fork,
            hash: None,
            block,
            seek: None,
            upgrade,
        };
        peer_state.inflight.add(&mut request);
        Ok(Some(request))
    } else {
        Ok(None)
    }
}
