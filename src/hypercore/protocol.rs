use async_channel::Sender;
use async_std::sync::{Arc, Mutex};
use dashmap::DashMap;
use futures_lite::{AsyncRead, AsyncWrite, StreamExt};
use hypercore_protocol::hypercore::compact_encoding::{CompactEncoding, State};
use hypercore_protocol::{Event, Protocol};
use random_access_storage::RandomAccess;
use std::fmt::Debug;
use std::io::ErrorKind;
use tracing::{debug, instrument};

use super::{
    discovery_key_from_public_key, messaging::HYPERMERGE_INTERNAL_NEW_PEERS_CREATED_MSG,
    HypercoreWrapper,
};
use crate::common::{message::NewPeersCreatedMessage, storage::DocStateWrapper, PeerEvent};

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
                    Event::Channel(channel) => {
                        debug!("Event:Channel: id={}", channel.id());
                        let discovery_key = channel.discovery_key();
                        if let Some(hypercore) = hypercores.get(discovery_key) {
                            let mut hypercore = hypercore.lock().await;
                            let (write_public_key, peer_public_keys) =
                                public_keys(doc_state.clone()).await;
                            let is_doc_channel = discovery_key == &doc_discovery_key;
                            hypercore.on_channel(
                                is_doc_channel,
                                channel,
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
                        HYPERMERGE_INTERNAL_NEW_PEERS_CREATED_MSG => {
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
