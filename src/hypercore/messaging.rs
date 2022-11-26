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

use super::PeerState;
use crate::common::{message::AdvertiseMessage, PeerEvent};

const HYPERMERGE_ADVERTISE_MSG: &str = "hypermerge/v1/advertise";

pub(super) fn create_advertise_message(peer_state: &PeerState) -> Message {
    let advertise_message: AdvertiseMessage = AdvertiseMessage {
        public_keys: peer_state.public_keys.clone(),
    };
    let mut enc_state = State::new();
    enc_state.preencode(&advertise_message);
    let mut buffer = enc_state.create_buffer();
    enc_state.encode(&advertise_message, &mut buffer);
    Message::Extension(Extension {
        name: HYPERMERGE_ADVERTISE_MSG.to_string(),
        message: buffer.to_vec(),
    })
}

pub(super) async fn on_message<T>(
    hypercore: &mut Arc<Mutex<Hypercore<T>>>,
    peer_state: &mut PeerState,
    channel: &mut Channel,
    message: Message,
    is_initiator: bool,
) -> Result<Option<PeerEvent>>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send,
{
    println!("on_message({}): GOT MESSAGE {:?}", is_initiator, message);
    match message {
        Message::Synchronize(message) => {
            let length_changed = message.length != peer_state.remote_length;
            let first_sync = !peer_state.remote_synced;
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
            peer_state.remote_synced = true;

            peer_state.length_acked = if same_fork { message.remote_length } else { 0 };

            let mut messages = vec![];

            if first_sync {
                // Need to send another sync back that acknowledges the received sync
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
                let msg = Request {
                    id: 1,
                    fork: info.fork,
                    hash: None,
                    block: None,
                    seek: None,
                    upgrade: Some(RequestUpgrade {
                        start: info.length,
                        length: peer_state.remote_length - info.length,
                    }),
                };
                messages.push(Message::Request(msg));
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
                panic!("Could not create proof from {:?}", message.id);
            }
        }
        Message::Data(message) => {
            let (old_info, applied, new_info, request_block, synced) = {
                let mut hypercore = hypercore.lock().await;
                let old_info = hypercore.info();
                let proof = message.clone().into_proof();
                let applied = hypercore.verify_and_apply_proof(&proof).await?;
                let new_info = hypercore.info();
                let request_block: Option<RequestBlock> = if let Some(upgrade) = &message.upgrade {
                    // When getting the initial upgrade, send a request for the first missing block
                    if old_info.length < upgrade.length {
                        let request_index = old_info.length;
                        let nodes = hypercore.missing_nodes(request_index * 2).await?;
                        Some(RequestBlock {
                            index: request_index,
                            nodes,
                        })
                    } else {
                        None
                    }
                } else if let Some(block) = &message.block {
                    // When receiving a block, ask for the next, if there are still some missing
                    if block.index < peer_state.remote_length - 1 {
                        let request_index = block.index + 1;
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
                let synced = new_info.contiguous_length == new_info.length;
                (old_info, applied, new_info, request_block, synced)
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
                if old_info.contiguous_length < new_info.contiguous_length {
                    messages.push(Message::Range(Range {
                        drop: false,
                        start: 0,
                        length: new_info.contiguous_length,
                    }));
                }
            }
            if let Some(request_block) = request_block {
                messages.push(Message::Request(Request {
                    id: request_block.index + 1,
                    fork: new_info.fork,
                    hash: None,
                    block: Some(request_block),
                    seek: None,
                    upgrade: None,
                }));
            }
            channel.send_batch(&messages).await.unwrap();
        }
        Message::Range(message) => {
            let info = {
                let hypercore = hypercore.lock().await;
                hypercore.info()
            };
            if message.start == 0 && message.length == info.contiguous_length {
                // TODO: Ready with this core
                return Ok(None);
            }
        }
        Message::Extension(message) => match message.name.as_str() {
            HYPERMERGE_ADVERTISE_MSG => {
                let mut dec_state = State::from_buffer(&message.message);
                let advertise_message: AdvertiseMessage = dec_state.decode(&message.message);
                let new_peers =
                    peer_state.filter_new_peer_public_keys(&advertise_message.public_keys);

                if new_peers.is_empty() {
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

                    if info.contiguous_length > 0 {
                        let range_msg = Range {
                            drop: false,
                            start: 0,
                            length: info.contiguous_length,
                        };
                        channel
                            .send_batch(&[
                                Message::Synchronize(sync_msg),
                                Message::Range(range_msg),
                            ])
                            .await?;
                    } else {
                        channel.send(Message::Synchronize(sync_msg)).await?;
                    }
                }
                if !new_peers.is_empty() {
                    // New peers found, return a peer event
                    return Ok(Some(PeerEvent::NewPeersAdvertised(new_peers)));
                }
            }
            _ => {
                panic!("Received unexpected extension message {:?}", message);
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
