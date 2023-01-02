use anyhow::Result;
use async_std::sync::{Arc, Mutex};
use hypercore_protocol::{
    hypercore::{
        compact_encoding::{CompactEncoding, State},
        Hypercore, RequestBlock, RequestUpgrade,
    },
    schema::*,
    Channel, Message,
};
use random_access_storage::RandomAccess;
use std::fmt::Debug;
use tracing::{debug, instrument};

use super::PeerState;
use crate::common::{
    message::{BroadcastMessage, NewPeersCreatedMessage},
    PeerEvent,
};

// Messages sent over-the-wire
const HYPERMERGE_BROADCAST_MSG: &str = "hypermerge/v1/broadcast";

// Local signals
const APPEND_LOCAL_SIGNAL_NAME: &str = "append";
const PEER_SYNCED_LOCAL_SIGNAL_NAME: &str = "peer_synced";
pub(super) const NEW_PEERS_CREATED_LOCAL_SIGNAL_NAME: &str = "new_peers_created";

pub(super) fn create_broadcast_message(peer_state: &PeerState) -> Message {
    let broadcast_message: BroadcastMessage = BroadcastMessage {
        write_public_key: peer_state.write_public_key.clone(),
        peer_public_keys: peer_state.peer_public_keys.clone(),
    };
    let mut enc_state = State::new();
    enc_state.preencode(&broadcast_message);
    let mut buffer = enc_state.create_buffer();
    enc_state.encode(&broadcast_message, &mut buffer);
    Message::Extension(Extension {
        name: HYPERMERGE_BROADCAST_MSG.to_string(),
        message: buffer.to_vec(),
    })
}

pub(super) fn create_append_local_signal(length: u64) -> Message {
    let mut enc_state = State::new();
    enc_state.preencode(&length);
    let mut buffer = enc_state.create_buffer();
    enc_state.encode(&length, &mut buffer);
    Message::LocalSignal((APPEND_LOCAL_SIGNAL_NAME.to_string(), buffer.to_vec()))
}

pub(super) fn create_peer_synced_local_signal(contiguous_length: u64) -> Message {
    let mut enc_state = State::new();
    enc_state.preencode(&contiguous_length);
    let mut buffer = enc_state.create_buffer();
    enc_state.encode(&contiguous_length, &mut buffer);
    Message::LocalSignal((PEER_SYNCED_LOCAL_SIGNAL_NAME.to_string(), buffer.to_vec()))
}

pub(super) fn create_new_peers_created_local_signal(public_keys: Vec<[u8; 32]>) -> Message {
    let message = NewPeersCreatedMessage { public_keys };
    let mut enc_state = State::new();
    enc_state.preencode(&message);
    let mut buffer = enc_state.create_buffer();
    enc_state.encode(&message, &mut buffer);
    Message::LocalSignal((
        NEW_PEERS_CREATED_LOCAL_SIGNAL_NAME.to_string(),
        buffer.to_vec(),
    ))
}

pub(super) async fn create_initial_synchronize<T>(
    hypercore: &mut Arc<Mutex<Hypercore<T>>>,
    peer_state: &mut PeerState,
) -> Vec<Message>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send,
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
    if info.contiguous_length > 0 {
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
) -> Result<Option<PeerEvent>>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send,
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
                panic!("Could not create proof from {:?}", message.id);
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
                let peer_synced: Option<PeerEvent> =
                    if new_info.contiguous_length == new_info.length {
                        peer_state.synced_contiguous_length = new_info.contiguous_length;
                        Some(PeerEvent::PeerSynced((
                            *channel.discovery_key(),
                            peer_state.synced_contiguous_length,
                        )))
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
            if let Some(block) = &message.block {
                // Send Range if the number of items changed, both for the single and
                // for the contiguous length
                if old_info.length < new_info.length {
                    messages.push(Message::Range(Range {
                        drop: false,
                        start: block.index,
                        length: 1,
                    }));
                }
                if old_info.contiguous_length < new_info.contiguous_length
                    && peer_state.contiguous_range_sent < new_info.contiguous_length
                {
                    messages.push(Message::Range(Range {
                        drop: false,
                        start: 0,
                        length: new_info.contiguous_length,
                    }));
                    peer_state.contiguous_range_sent = new_info.contiguous_length;
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
                let event: Option<PeerEvent> = if message.start == 0 {
                    if info.contiguous_length == message.length
                        && peer_state.remote_length == message.length
                    {
                        if peer_state.notified_remote_synced_contiguous_length < message.length {
                            // The peer has advertised that they now have what we have
                            let event = Some(PeerEvent::RemotePeerSynced((
                                *channel.discovery_key(),
                                message.length,
                            )));
                            peer_state.notified_remote_synced_contiguous_length = message.length;
                            event
                        } else {
                            None
                        }
                    } else {
                        // If the other side advertises more than we have, and more than the peer length,
                        // we have recorded, then we need to request the rest of the blocks.
                        if info.length < message.length && peer_state.remote_length < message.length
                        {
                            peer_state.remote_length = message.length;
                            if let Some(request) = next_request(&mut hypercore, peer_state).await? {
                                channel.send(Message::Request(request)).await?;
                            }
                        }
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
            HYPERMERGE_BROADCAST_MSG => {
                assert!(
                    peer_state.is_doc,
                    "Only doc peer should ever get broadcast messages"
                );
                let mut dec_state = State::from_buffer(&message.message);
                let broadcast_message: BroadcastMessage = dec_state.decode(&message.message);
                let new_remote_public_keys = peer_state.filter_new_peer_public_keys(
                    &broadcast_message.write_public_key,
                    &broadcast_message.peer_public_keys,
                );

                if new_remote_public_keys.is_empty()
                    && peer_state.peer_public_keys_match(
                        &broadcast_message.write_public_key,
                        &broadcast_message.peer_public_keys,
                    )
                {
                    // Don't re-initialize if this has already been synced, meaning this is a
                    // broadcast that notifies a new third party peer after the initial handshake.
                    if !peer_state.sync_sent {
                        let messages = create_initial_synchronize(hypercore, peer_state).await;
                        channel.send_batch(&messages).await?;
                    }
                } else if !new_remote_public_keys.is_empty() {
                    // New peers found, return a peer event
                    return Ok(Some(PeerEvent::NewPeersBroadcasted(new_remote_public_keys)));
                }
            }
            _ => {
                panic!("Received unexpected extension message {:?}", message);
            }
        },
        Message::LocalSignal((name, data)) => match name.as_str() {
            APPEND_LOCAL_SIGNAL_NAME => {
                let mut dec_state = State::from_buffer(&data);
                let length: u64 = dec_state.decode(&data);
                let info = {
                    let hypercore = hypercore.lock().await;
                    hypercore.info()
                };

                if info.contiguous_length >= length && peer_state.contiguous_range_sent < length {
                    let range_msg = Range {
                        drop: false,
                        start: 0,
                        length,
                    };
                    peer_state.contiguous_range_sent = length;
                    channel.send(Message::Range(range_msg)).await?;
                }
            }
            PEER_SYNCED_LOCAL_SIGNAL_NAME => {
                let mut dec_state = State::from_buffer(&data);
                let contiguous_length: u64 = dec_state.decode(&data);
                if contiguous_length > peer_state.synced_contiguous_length
                    && peer_state.contiguous_range_sent < contiguous_length
                {
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
            NEW_PEERS_CREATED_LOCAL_SIGNAL_NAME => {
                assert!(
                    peer_state.is_doc,
                    "Only doc peer should ever get new peers created messages"
                );
                let mut dec_state = State::from_buffer(&data);
                let new_peers: NewPeersCreatedMessage = dec_state.decode(&data);

                // Save new peers to the peer state
                peer_state.peer_public_keys.extend(new_peers.public_keys);

                // Transmit this event forward to the protocol
                channel
                    .signal_local_protocol(NEW_PEERS_CREATED_LOCAL_SIGNAL_NAME, data)
                    .await?;

                // Create new broadcast message
                let mut messages = vec![create_broadcast_message(&peer_state)];

                // If sync has not been started, start it now
                if !peer_state.sync_sent {
                    messages.extend(create_initial_synchronize(hypercore, peer_state).await);
                }
                channel.send_batch(&messages).await?;
            }
            _ => {
                panic!("Received unexpected local signal {}", name);
            }
        },
        Message::Close(message) => {
            return Ok(Some(PeerEvent::PeerDisconnected(message.channel)));
        }
        _ => {
            panic!("Received unexpected message {:?}", message);
        }
    };
    Ok(None)
}

/// Return the next request that should be sent based on peer state.
async fn next_request<T>(
    hypercore: &mut Hypercore<T>,
    peer_state: &mut PeerState,
) -> anyhow::Result<Option<Request>>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send,
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

    // Add blocks to block index queue if there's a new length
    let block: Option<RequestBlock> = if peer_state.remote_length > info.contiguous_length {
        // Check if there's already an inflight block request
        let request_index: u64 = if let Some(block) = peer_state.inflight.get_highest_block() {
            block.index + 1
        } else {
            info.contiguous_length
        };
        if peer_state.remote_length > request_index {
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
