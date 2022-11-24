use anyhow::Result;
use async_channel::Sender;
use async_std::sync::{Arc, Mutex};
use futures_lite::stream::StreamExt;
use hypercore_protocol::{hypercore::Hypercore, Channel};
use random_access_storage::RandomAccess;
use std::fmt::Debug;

use super::{messaging::create_advertise_message, on_message, PeerEvent, PeerState};

pub(super) async fn on_peer<T>(
    mut hypercore: Arc<Mutex<Hypercore<T>>>,
    mut peer_state: PeerState,
    mut channel: Channel,
    peer_event_sender: &mut Sender<PeerEvent>,
) -> Result<()>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send + 'static,
{
    // Immediately advertise peers to the other end
    let message = create_advertise_message(&peer_state);
    channel.send(message).await.unwrap();

    // Start listening on incoming messages
    while let Some(message) = channel.next().await {
        let event = on_message(&mut hypercore, &mut peer_state, &mut channel, message).await?;
        if let Some(event) = event {
            peer_event_sender.send(event.clone()).await?;
            if let PeerEvent::NewPeersAdvertised(_) = event {
                // When there are new peers advertised, let's just close the channel, write
                // the new peers to state and reopen.
                channel.close().await?;
            }
        }
    }
    Ok(())
}
