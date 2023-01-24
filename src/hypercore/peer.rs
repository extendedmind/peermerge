use anyhow::Result;
use futures::channel::mpsc::UnboundedSender;
use futures::stream::StreamExt;
use hypercore_protocol::{hypercore::Hypercore, Channel, ChannelReceiver, Message};
use random_access_storage::RandomAccess;
use std::fmt::Debug;
use std::sync::Arc;
use tracing::{debug, instrument};

#[cfg(feature = "async-std")]
use async_std::sync::Mutex;
#[cfg(feature = "tokio")]
use tokio::sync::Mutex;

use super::{
    messaging::{create_broadcast_message, create_initial_synchronize},
    on_message, PeerState,
};
use crate::common::PeerEvent;

#[instrument(level = "debug", skip_all)]
pub(super) async fn on_peer<T>(
    mut hypercore: Arc<Mutex<Hypercore<T>>>,
    mut peer_state: PeerState,
    mut channel: Channel,
    mut channel_receiver: ChannelReceiver<Message>,
    peer_event_sender: &mut UnboundedSender<PeerEvent>,
) -> Result<()>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send + 'static,
{
    // Immediately send an initial synchronize
    let messages = create_initial_synchronize(&mut hypercore, &mut peer_state).await;
    channel.send_batch(&messages).await?;

    // Start listening on incoming messages or internal messages
    debug!("Start listening on channel messages");
    while let Some(message) = channel_receiver.next().await {
        let exit = process_message(
            message,
            &mut hypercore,
            &mut peer_state,
            &mut channel,
            peer_event_sender,
        )
        .await?;
        if exit {
            break;
        }
    }
    debug!("Exiting");
    Ok(())
}

#[instrument(level = "debug", skip_all)]
pub(super) async fn on_doc_peer<T>(
    mut hypercore: Arc<Mutex<Hypercore<T>>>,
    mut peer_state: PeerState,
    mut channel: Channel,
    mut channel_receiver: ChannelReceiver<Message>,
    peer_event_sender: &mut UnboundedSender<PeerEvent>,
) -> Result<()>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send + 'static,
{
    // Immediately broadcast hypercores to the other end on the doc peer
    let message = create_broadcast_message(&peer_state);
    channel.send(message).await?;

    // Start listening on incoming messages or internal messages
    debug!("Start listening on doc channel messages");
    while let Some(message) = channel_receiver.next().await {
        let exit = process_message(
            message,
            &mut hypercore,
            &mut peer_state,
            &mut channel,
            peer_event_sender,
        )
        .await?;
        if exit {
            break;
        }
    }
    debug!("Exiting");
    Ok(())
}

#[instrument(level = "debug", skip_all)]
async fn process_message<T>(
    message: Message,
    hypercore: &mut Arc<Mutex<Hypercore<T>>>,
    peer_state: &mut PeerState,
    channel: &mut Channel,
    peer_event_sender: &mut UnboundedSender<PeerEvent>,
) -> Result<bool>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send + 'static,
{
    let event = on_message(hypercore, peer_state, channel, message).await?;
    if let Some(event) = event {
        peer_event_sender.unbounded_send(event.clone())?;
        if let PeerEvent::PeerDisconnected(_) = event {
            return Ok(true);
        }
    }
    Ok(false)
}
