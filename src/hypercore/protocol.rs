use async_channel::{unbounded, Receiver, Sender};
use async_std::sync::{Arc, Mutex};
#[cfg(not(target_arch = "wasm32"))]
use async_std::task;
use futures_lite::{AsyncRead, AsyncWrite, StreamExt};
use hypercore_protocol::{Event, Protocol};
use random_access_storage::RandomAccess;
use std::{collections::HashMap, fmt::Debug};
#[cfg(target_arch = "wasm32")]
use wasm_bindgen_futures::spawn_local;

use super::common::PeerEvent;
use crate::common::SynchronizeEvent;
use crate::store::HypercoreStore;

pub(crate) async fn on_protocol<T, IO>(
    protocol: &mut Protocol<IO>,
    hypercore_store: &mut HypercoreStore<T>,
    sync_event_sender: &mut Sender<SynchronizeEvent>,
) -> anyhow::Result<()>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send + 'static,
    IO: AsyncWrite + AsyncRead + Send + Unpin + 'static,
{
    let is_initiator = protocol.is_initiator();
    let (mut peer_event_sender, peer_event_receiver): (Sender<PeerEvent>, Receiver<PeerEvent>) =
        unbounded();

    let sync_event_sender = sync_event_sender.clone();
    #[cfg(not(target_arch = "wasm32"))]
    task::spawn(async move {
        on_peer_event(peer_event_receiver, sync_event_sender).await;
    });
    #[cfg(target_arch = "wasm32")]
    spawn_local(async move {
        on_peer_event(peer_event_receiver, sync_event_sender).await;
    });

    while let Some(event) = protocol.next().await {
        let event = event?;
        match event {
            Event::Handshake(_) => {
                if is_initiator {
                    for hypercore in hypercore_store.hypercores.values() {
                        let hypercore = hypercore.lock().await;
                        protocol.open(hypercore.key().clone()).await?;
                    }
                }
            }
            Event::DiscoveryKey(dkey) => {
                if let Some(hypercore) = hypercore_store.hypercores.get(&dkey) {
                    let hypercore = hypercore.lock().await;
                    protocol.open(hypercore.key().clone()).await?;
                }
            }
            Event::Channel(channel) => {
                if let Some(hypercore) = hypercore_store.hypercores.get(channel.discovery_key()) {
                    let hypercore = hypercore.lock().await;
                    hypercore.on_channel(
                        channel,
                        hypercore_store.peer_public_keys(),
                        &mut peer_event_sender,
                    );
                }
            }
            Event::Close(_dkey) => {}
            _ => {}
        }
    }

    Ok(())
}

async fn on_peer_event(
    mut peer_event_receiver: Receiver<PeerEvent>,
    mut sync_event_sender: Sender<SynchronizeEvent>,
) {
    while let Some(event) = peer_event_receiver.next().await {}
}
