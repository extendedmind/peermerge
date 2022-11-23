use async_std::sync::{Arc, Mutex};
use futures_lite::{AsyncRead, AsyncWrite, StreamExt};
use hypercore_protocol::{Event, Protocol};
use random_access_storage::RandomAccess;
use std::{collections::HashMap, fmt::Debug};

use super::HypercoreWrapper;

pub async fn on_protocol<T, IO>(
    protocol: &mut Protocol<IO>,
    hypercore_store: &HashMap<[u8; 32], Arc<Mutex<HypercoreWrapper<T>>>>,
) -> anyhow::Result<()>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send + 'static,
    IO: AsyncWrite + AsyncRead + Send + Unpin + 'static,
{
    let is_initiator = protocol.is_initiator();
    while let Some(event) = protocol.next().await {
        let event = event?;
        match event {
            Event::Handshake(_) => {
                if is_initiator {
                    for hypercore in hypercore_store.values() {
                        let hypercore = hypercore.lock().await;
                        protocol.open(hypercore.key().clone()).await?;
                    }
                }
            }
            Event::DiscoveryKey(dkey) => {
                if let Some(hypercore) = hypercore_store.get(&dkey) {
                    let hypercore = hypercore.lock().await;
                    protocol.open(hypercore.key().clone()).await?;
                }
            }
            Event::Channel(channel) => {
                if let Some(hypercore) = hypercore_store.get(channel.discovery_key()) {
                    let hypercore = hypercore.lock().await;
                    hypercore.on_peer(channel);
                }
            }
            Event::Close(_dkey) => {}
            _ => {}
        }
    }

    Ok(())
}
