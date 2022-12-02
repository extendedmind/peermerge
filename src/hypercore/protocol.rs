use async_channel::Sender;
use async_std::sync::{Arc, Mutex};
use dashmap::DashMap;
use futures_lite::{AsyncRead, AsyncWrite, StreamExt};
use hypercore_protocol::{Event, Protocol};
use random_access_storage::RandomAccess;
use std::fmt::Debug;
use std::io::ErrorKind;

use super::HypercoreWrapper;
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
                            for hypercore in hypercores.iter() {
                                let hypercore = hypercore.lock().await;
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
                            println!(
                                "on_protocol({}): reading public keys from store",
                                is_initiator
                            );
                            let (public_key, peer_public_keys) =
                                public_keys(doc_state.clone()).await;
                            hypercore.on_channel(
                                channel,
                                public_key,
                                peer_public_keys,
                                peer_event_sender,
                                is_initiator,
                            );
                        }
                    }
                    Event::Close(_) => {
                        // When a channel is closed, reopen all. Reopening is needed during initial
                        // advertising of public keys.
                        if is_initiator {
                            for entry in hypercores.iter() {
                                let hypercore = entry.value().lock().await;
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
    (state.public_key, peer_public_keys)
}
