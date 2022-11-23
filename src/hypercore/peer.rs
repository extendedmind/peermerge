use anyhow::Result;
use async_channel::Sender;
use async_std::sync::{Arc, Mutex};
use futures_lite::stream::StreamExt;
use hypercore_protocol::{hypercore::Hypercore, schema::*, Channel, Message};
use random_access_storage::RandomAccess;
use std::fmt::Debug;

use super::{on_message, PeerEvent, PeerState};

pub(super) async fn on_peer<T>(
    mut hypercore: Arc<Mutex<Hypercore<T>>>,
    mut peer_state: PeerState,
    mut channel: Channel,
    peer_event_sender: &mut Sender<PeerEvent>,
) -> Result<()>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send + 'static,
{
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
            .send_batch(&[Message::Synchronize(sync_msg), Message::Range(range_msg)])
            .await?;
    } else {
        channel.send(Message::Synchronize(sync_msg)).await?;
    }
    while let Some(message) = channel.next().await {
        let event = on_message(&mut hypercore, &mut peer_state, &mut channel, message).await?;
        if let Some(event) = event {
            peer_event_sender.send(event.clone()).await?;
            if let PeerEvent::PeersAdvertised(_) = event {
                // When there are new peers advertised, let's just close the channel, write
                // the new peers to state and reopen.
                channel.close().await?;
            }
        }
    }
    Ok(())
}
