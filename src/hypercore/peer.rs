use anyhow::Result;
use async_channel::{Receiver, Sender};
use async_std::sync::{Arc, Mutex};
use futures_lite::stream::StreamExt;
use hypercore_protocol::{
    hypercore::{
        compact_encoding::{CompactEncoding, State},
        Hypercore,
    },
    Channel, Message,
};
use random_access_storage::RandomAccess;
use std::fmt::Debug;

use super::{messaging::create_broadcast_message, on_message, PeerState};
use crate::common::{message::CloseMessage, PeerEvent};

pub(super) async fn on_peer<T>(
    mut hypercore: Arc<Mutex<Hypercore<T>>>,
    mut peer_state: PeerState,
    mut channel: Channel,
    mut internal_message_receiver: Receiver<Message>,
    peer_event_sender: &mut Sender<PeerEvent>,
    is_initiator: bool,
) -> Result<()>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send + 'static,
{
    // Immediately broadcast hypercores to the other end
    let message = create_broadcast_message(&peer_state);
    channel.send(message).await.unwrap();

    // Start listening on incoming messages or internal messages
    while let Some(message) =
        futures_lite::stream::race(&mut channel, &mut internal_message_receiver)
            .next()
            .await
    {
        let event = on_message(
            &mut hypercore,
            &mut peer_state,
            &mut channel,
            message,
            is_initiator,
        )
        .await?;
        if let Some(event) = event {
            peer_event_sender.send(event.clone()).await?;
            if let PeerEvent::NewPeersAdvertised(ref new_peer_public_keys) = event {
                // When there are new peers advertised, let's tell about them via the close
                // message to be able to wait for the right number of hypercores to end up
                // in the store.
                println!("on_peer({}): closing id={}", is_initiator, channel.id());
                let close_message = CloseMessage::new(new_peer_public_keys.clone());
                let mut enc_state = State::new();
                enc_state.preencode(&close_message);
                let mut buffer = enc_state.create_buffer();
                enc_state.encode(&close_message, &mut buffer);
                channel.close_with_data(buffer.to_vec()).await?;
                println!(
                    "on_peer({}): close success id={}",
                    is_initiator,
                    channel.id()
                );
            }
            if let PeerEvent::PeerDisconnected(_) = event {
                break;
            }
        }
    }
    println!("on_peer({}): exiting", is_initiator,);

    Ok(())
}
