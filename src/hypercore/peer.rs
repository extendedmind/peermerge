use async_std::sync::{Arc, Mutex};
use futures_lite::stream::StreamExt;
use hypercore_protocol::{hypercore::Hypercore, schema::*, Channel, Message};
use random_access_storage::RandomAccess;
use std::fmt::Debug;

use super::{on_message, PeerState};

pub(super) async fn on_peer<T>(
    mut hypercore: Arc<Mutex<Hypercore<T>>>,
    mut peer_state: PeerState,
    mut channel: Channel,
) where
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
            .await
            .unwrap();
    } else {
        channel.send(Message::Synchronize(sync_msg)).await.unwrap();
    }
    while let Some(message) = channel.next().await {
        let ready = on_message(&mut hypercore, &mut peer_state, &mut channel, message)
            .await
            .expect("on_message should return Ok");
        if ready {
            channel.close().await.expect("Should be able to close");
            break;
        }
    }
}
