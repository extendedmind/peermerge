use compact_encoding::{CompactEncoding, State};
use hypercore_protocol::{
    hypercore::{Hypercore, RequestBlock, RequestUpgrade},
    schema::*,
    Channel, Message,
};
use random_access_storage::RandomAccess;
use std::fmt::Debug;
use std::sync::Arc;
use tracing::{debug, instrument};

use super::PeerState;
use crate::{
    common::{
        message::{BroadcastMessage, PeerSyncedMessage, PeersChangedMessage},
        state::{DocumentPeer, DocumentPeersState},
        utils::Mutex,
        FeedEvent,
        FeedEventContent::*,
    },
    feed::FeedDiscoveryKey,
    PeermergeError,
};

// Messages sent over-the-wire
const PEERMERGE_BROADCAST_MSG: &str = "peermerge/v1/broadcast";

// Local signals
const APPEND_LOCAL_SIGNAL_NAME: &str = "append";
const PEER_SYNCED_LOCAL_SIGNAL_NAME: &str = "peer_synced";
pub(super) const PEERS_CHANGED_LOCAL_SIGNAL_NAME: &str = "peers_changed";
const CLOSED_LOCAL_SIGNAL_NAME: &str = "closed";

pub(super) fn create_broadcast_message(peers_state: &DocumentPeersState) -> Message {
    let broadcast_message: BroadcastMessage = BroadcastMessage {
        write_peer: peers_state.write_peer.clone(),
        peers: peers_state.peers.clone(),
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

pub(super) fn create_peer_synced_local_signal(contiguous_length: u64) -> Message {
    let message = PeerSyncedMessage::new(contiguous_length);
    let mut enc_state = State::new();
    enc_state
        .preencode(&message)
        .expect("Pre-encoding peer synced local signal should not fail");
    let mut buffer = enc_state.create_buffer();
    enc_state
        .encode(&message, &mut buffer)
        .expect("Encoding peer synced local signal should not fail");
    Message::LocalSignal((PEER_SYNCED_LOCAL_SIGNAL_NAME.to_string(), buffer.to_vec()))
}

pub(super) fn create_peers_changed_local_signal(
    doc_discovery_key: FeedDiscoveryKey,
    incoming_peers: Vec<DocumentPeer>,
    replaced_peers: Vec<DocumentPeer>,
    peers_to_create: Vec<DocumentPeer>,
) -> Message {
    let message = PeersChangedMessage {
        doc_discovery_key,
        incoming_peers,
        replaced_peers,
        peers_to_create,
    };
    let mut enc_state = State::new();
    enc_state
        .preencode(&message)
        .expect("Pre-encoding new peers created local signal should not fail");
    let mut buffer = enc_state.create_buffer();
    enc_state
        .encode(&message, &mut buffer)
        .expect("Encoding new peers created local signal should not fail");
    Message::LocalSignal((PEERS_CHANGED_LOCAL_SIGNAL_NAME.to_string(), buffer.to_vec()))
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
    // There are no new peers, start sync
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

    peer_state.sync_sent = true;
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
) -> Result<Option<FeedEvent>, PeermergeError>
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
            return Ok(None);
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
            let (old_info, applied, new_info, request, peer_synced) = {
                let mut hypercore = hypercore.lock().await;
                let old_info = hypercore.info();
                let proof = message.clone().into_proof();
                let applied = hypercore.verify_and_apply_proof(&proof).await?;
                let new_info = hypercore.info();
                peer_state.inflight.remove(message.request);
                let request = next_request(&mut hypercore, peer_state).await?;
                let peer_synced: Option<FeedEvent> =
                    if new_info.contiguous_length == new_info.length {
                        peer_state.synced_contiguous_length = new_info.contiguous_length;
                        Some(FeedEvent::new(
                            peer_state.doc_discovery_key,
                            FeedSynced((
                                peer_state.peer_id,
                                *channel.discovery_key(),
                                new_info.contiguous_length,
                            )),
                        ))
                    } else {
                        None
                    };
                (old_info, applied, new_info, request, peer_synced)
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
            channel.send_batch(&messages).await?;
            return Ok(peer_synced);
        }
        Message::Range(message) => {
            let event = {
                let mut hypercore = hypercore.lock().await;
                let info = hypercore.info();
                let event: Option<FeedEvent> = if message.start == 0 {
                    peer_state.remote_contiguous_length = message.length;
                    if message.length > peer_state.remote_length {
                        peer_state.remote_length = message.length;
                    }
                    if info.contiguous_length == peer_state.remote_contiguous_length {
                        if peer_state.notified_remote_synced_contiguous_length
                            < peer_state.remote_contiguous_length
                        {
                            // The peer has advertised that they now have what we have
                            let event = Some(FeedEvent::new(
                                peer_state.doc_discovery_key,
                                RemoteFeedSynced((
                                    peer_state.peer_id,
                                    *channel.discovery_key(),
                                    peer_state.remote_contiguous_length,
                                )),
                            ));
                            peer_state.notified_remote_synced_contiguous_length =
                                peer_state.remote_contiguous_length;
                            event
                        } else {
                            None
                        }
                    } else if info.contiguous_length < peer_state.remote_contiguous_length {
                        // If the other side advertises more than we have, we need to request the rest
                        // of the blocks.
                        if let Some(request) = next_request(&mut hypercore, peer_state).await? {
                            channel.send(Message::Request(request)).await?;
                        }
                        None
                    } else {
                        // We have more than the peer, just ignore
                        None
                    }
                } else {
                    // TODO: For now, let's just ignore messages that don't indicate
                    // a contiguous length. When we add support for deleting entries
                    // from the hypercore, this needs proper bitfield support.
                    None
                };
                event
            };
            return Ok(event);
        }
        Message::Extension(message) => match message.name.as_str() {
            PEERMERGE_BROADCAST_MSG => {
                assert!(
                    peer_state.is_doc,
                    "Only doc peer should ever get broadcast messages"
                );
                let peers_state = peer_state.peers_state.as_mut().unwrap();
                let mut dec_state = State::from_buffer(&message.message);
                let broadcast_message: BroadcastMessage = dec_state.decode(&message.message)?;
                let new_remote_peers = peers_state
                    .filter_new_peers(&broadcast_message.write_peer, &broadcast_message.peers);

                if new_remote_peers.is_empty()
                    && peers_state
                        .peers_match(&broadcast_message.write_peer, &broadcast_message.peers)
                {
                    // Don't re-initialize if this has already been synced, meaning this is a
                    // broadcast that notifies a new third party peer after the initial handshake.
                    if !peer_state.sync_sent {
                        let messages = create_initial_synchronize(hypercore, peer_state).await;
                        channel.send_batch(&messages).await?;
                    }
                } else if !new_remote_peers.is_empty() {
                    // New peers found, return a peer event
                    return Ok(Some(FeedEvent::new(
                        peer_state.doc_discovery_key,
                        NewFeedsBroadcasted(new_remote_peers),
                    )));
                }
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
            PEER_SYNCED_LOCAL_SIGNAL_NAME => {
                let mut dec_state = State::from_buffer(&data);
                let message: PeerSyncedMessage = dec_state.decode(&data)?;
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
            }
            PEERS_CHANGED_LOCAL_SIGNAL_NAME => {
                assert!(
                    peer_state.is_doc,
                    "Only doc feed should ever get new peers created messages"
                );
                let peers_state = peer_state.peers_state.as_mut().unwrap();
                let mut dec_state = State::from_buffer(&data);
                let new_peers_message: PeersChangedMessage = dec_state.decode(&data)?;

                // Merge new peers to the peer state
                peers_state.merge_incoming_peers(&new_peers_message.incoming_peers);

                // Transmit this event forward to the protocol
                channel
                    .signal_local_protocol(PEERS_CHANGED_LOCAL_SIGNAL_NAME, data)
                    .await?;

                // Create new broadcast message
                let mut messages = vec![create_broadcast_message(peers_state)];

                // If sync has not been started, start it now
                if !peer_state.sync_sent {
                    messages.extend(create_initial_synchronize(hypercore, peer_state).await);
                }
                channel.send_batch(&messages).await?;
            }

            CLOSED_LOCAL_SIGNAL_NAME => {
                assert!(
                    peer_state.is_doc,
                    "Only doc feed should ever get closed message"
                );
                channel.close().await?;
            }
            _ => {
                panic!("Received unexpected local signal: {name}");
            }
        },
        Message::Close(message) => {
            return Ok(Some(FeedEvent::new(
                peer_state.doc_discovery_key,
                FeedDisconnected(message.channel),
            )));
        }
        _ => {
            panic!("Received unexpected message: {message:?}");
        }
    };
    Ok(None)
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
