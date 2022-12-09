use async_channel::Sender;
use async_std::sync::{Arc, Mutex};
use dashmap::DashMap;
use futures_lite::{AsyncRead, AsyncWrite, StreamExt};
use hypercore_protocol::hypercore::compact_encoding::{CompactEncoding, State};
use hypercore_protocol::{Event, Protocol};
use random_access_storage::RandomAccess;
use std::io::ErrorKind;
use std::{fmt::Debug, time::Duration};

use super::HypercoreWrapper;
use crate::common::message::CloseMessage;
use crate::common::{storage::DocStateWrapper, PeerEvent};

pub(crate) async fn on_protocol<T, IO>(
    protocol: &mut Protocol<IO>,
    doc_state: Arc<Mutex<DocStateWrapper<T>>>,
    hypercores: Arc<DashMap<[u8; 32], Arc<Mutex<HypercoreWrapper<T>>>>>,
    peer_event_sender: &mut Sender<PeerEvent>,
    is_initiator: bool,
) -> anyhow::Result<()>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send + 'static,
    IO: AsyncWrite + AsyncRead + Send + Unpin + 'static,
{
    let is_initiator = protocol.is_initiator();

    println!(
        "on_protocol({}): Begin listening to protocol events",
        is_initiator
    );
    let mut hypercore_count = 0;
    while let Some(event) = protocol.next().await {
        println!(
            "on_protocol({}): Got protocol event {:?}",
            is_initiator, event,
        );
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
                            hypercore_count = hypercores.len();
                            for hypercore in hypercores.iter() {
                                let hypercore = hypercore.lock().await;
                                println!(
                                    "on_protocol({}): Event:Handshake: opening protocol",
                                    is_initiator,
                                );
                                protocol.open(hypercore.public_key().clone()).await?;
                            }
                        }
                    }
                    Event::DiscoveryKey(dkey) => {
                        if let Some(hypercore) = hypercores.get(&dkey) {
                            let hypercore = hypercore.lock().await;
                            protocol.open(hypercore.public_key().clone()).await?;
                        }
                    }
                    Event::Channel(channel) => {
                        println!(
                            "on_protocol({}): Event:Channel: id={}",
                            is_initiator,
                            channel.id()
                        );
                        if let Some(hypercore) = hypercores.get(channel.discovery_key()) {
                            let mut hypercore = hypercore.lock().await;
                            let (public_key, peer_public_keys) =
                                public_keys(doc_state.clone()).await;
                            println!(
                                "on_protocol({}): read {} + {} public keys from store",
                                is_initiator,
                                public_key.map(|_| 1).unwrap_or(0),
                                peer_public_keys.len()
                            );
                            hypercore.on_channel(
                                channel,
                                public_key,
                                peer_public_keys,
                                peer_event_sender,
                                is_initiator,
                            );
                        }
                    }
                    Event::Close((_, close_message)) => {
                        // When a channel is closed, reopen all. Reopening is needed during initial
                        // advertising of public keys.
                        if is_initiator {
                            if !close_message.is_empty() {
                                let mut dec_state = State::from_buffer(&close_message);
                                let close_message: CloseMessage = dec_state.decode(&close_message);
                                while hypercores.len()
                                    < hypercore_count + close_message.new_peer_public_keys.len()
                                {
                                    // We need to wait here to give time for the new hypercores to be
                                    // created, and only after that reopen
                                    async_std::task::sleep(Duration::from_millis(10)).await;
                                }
                                hypercore_count = hypercores.len();
                            }
                            for entry in hypercores.iter() {
                                let hypercore = entry.value().lock().await;
                                println!(
                                    "on_protocol({}): Event:Close: re-opening protocol",
                                    is_initiator,
                                );
                                protocol.open(hypercore.public_key().clone()).await?;
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    }
    println!("on_protocol({}): returning", is_initiator);
    Ok(())
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
