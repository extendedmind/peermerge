use async_channel::Sender;
use futures_lite::{AsyncRead, AsyncWrite, StreamExt};
use hypercore_protocol::{Event, Protocol};
use random_access_storage::RandomAccess;
use std::fmt::Debug;
use std::io::ErrorKind;

use crate::common::{PeerEvent, SynchronizeEvent};
use crate::store::HypercoreStore;

pub(crate) async fn on_protocol<T, IO>(
    protocol: &mut Protocol<IO>,
    hypercore_store: &mut HypercoreStore<T>,
    peer_event_sender: &mut Sender<PeerEvent>,
    sync_event_sender: &mut Sender<SynchronizeEvent>,
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
                            for hypercore in hypercore_store.hypercores().lock().await.values() {
                                let hypercore = hypercore.lock().await;
                                protocol.open(hypercore.key().clone()).await?;
                            }
                        }
                    }
                    Event::DiscoveryKey(dkey) => {
                        if let Some(hypercore) =
                            hypercore_store.hypercores().lock().await.get(&dkey)
                        {
                            let hypercore = hypercore.lock().await;
                            protocol.open(hypercore.key().clone()).await?;
                        }
                    }
                    Event::Channel(channel) => {
                        println!(
                            "on_protocol({}): Event:Channel: id={}",
                            is_initiator,
                            channel.id()
                        );
                        if let Some(hypercore) = hypercore_store
                            .hypercores()
                            .lock()
                            .await
                            .get(channel.discovery_key())
                        {
                            let hypercore = hypercore.lock().await;
                            println!(
                                "on_protocol({}): reading public keys from store",
                                is_initiator
                            );
                            hypercore.on_channel(
                                channel,
                                hypercore_store.public_keys().await,
                                peer_event_sender,
                                is_initiator,
                            );
                        }
                    }
                    Event::Close(_) => {
                        // When a channel is closed, reopen all. Reopening is needed during initial
                        // advertising of public keys.
                        if is_initiator {
                            for hypercore in hypercore_store.hypercores().lock().await.values() {
                                let hypercore = hypercore.lock().await;
                                protocol.open(hypercore.key().clone()).await?;
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
