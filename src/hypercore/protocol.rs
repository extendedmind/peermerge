use async_channel::Sender;
use async_std::sync::{Arc, Mutex};
use dashmap::DashMap;
use futures_lite::{AsyncRead, AsyncWrite, StreamExt};
use hypercore_protocol::{Event, Protocol};
use random_access_storage::RandomAccess;
use std::fmt::Debug;
use std::io::ErrorKind;
use tracing::{debug, instrument};

use super::HypercoreWrapper;
use crate::common::{storage::DocStateWrapper, PeerEvent};

#[instrument(
    level = "debug",
    skip(protocol, doc_state, hypercores, peer_event_sender)
)]
pub(crate) async fn on_protocol<T, IO>(
    protocol: &mut Protocol<IO>,
    doc_state: Arc<Mutex<DocStateWrapper<T>>>,
    hypercores: Arc<DashMap<[u8; 32], Arc<Mutex<HypercoreWrapper<T>>>>>,
    peer_event_sender: &mut Sender<PeerEvent>,
) -> anyhow::Result<()>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send + 'static,
    IO: AsyncWrite + AsyncRead + Send + Unpin + 'static,
{
    let is_initiator = protocol.is_initiator();
    let doc_discovery_key = { doc_state.lock().await.state().doc_discovery_key.clone() };

    debug!("Begin listening to protocol events");
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
                            let hypercore = hypercore.lock().await;
                            protocol.open(hypercore.public_key().clone()).await?;
                        }
                    }
                    Event::Channel(channel) => {
                        debug!("Event:Channel: id={}", channel.id());
                        let discovery_key = channel.discovery_key();
                        let new_peers_created_message_receiver = {
                            let doc_hypercore = hypercores.get(&doc_discovery_key).unwrap();
                            let mut doc_hypercore = doc_hypercore.lock().await;
                            doc_hypercore.listen_for_new_peers_created()
                        };
                        if let Some(hypercore) = hypercores.get(discovery_key) {
                            let mut hypercore = hypercore.lock().await;
                            let (public_key, peer_public_keys) =
                                public_keys(doc_state.clone()).await;
                            hypercore.on_channel(
                                channel,
                                public_key,
                                peer_public_keys,
                                peer_event_sender,
                                new_peers_created_message_receiver,
                            );
                        } else {
                            panic!(
                                "Could not find hypercore with discovery key {:02X?}",
                                discovery_key
                            );
                        }
                    }
                    Event::Close(discovery_key) => {
                        // When a channel is closed, reopen all if the channel is the doc channel. Reopening is
                        // needed during initial advertising of public keys.
                        if is_initiator {
                            if discovery_key == doc_discovery_key {
                                for entry in hypercores.iter() {
                                    let hypercore = entry.value().lock().await;
                                    debug!("Event:Close: re-opening protocol");
                                    protocol.open(hypercore.public_key().clone()).await?;
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    }
    debug!("Exiting");
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
