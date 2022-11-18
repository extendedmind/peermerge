use async_std::prelude::*;
use async_std::task::{self, JoinHandle};
use futures_lite::io::{AsyncRead, AsyncWrite};
use hypercore_protocol::{Channel, DiscoveryKey, Duplex, Event, Protocol, ProtocolBuilder};
use std::io;

pub type MemoryProtocol = Protocol<Duplex<sluice::pipe::PipeReader, sluice::pipe::PipeWriter>>;
pub async fn create_pair_memory() -> io::Result<(MemoryProtocol, MemoryProtocol)> {
    let (ar, bw) = sluice::pipe::pipe();
    let (br, aw) = sluice::pipe::pipe();

    let responder = ProtocolBuilder::new(false);
    let initiator = ProtocolBuilder::new(true);
    let responder = responder.connect_rw(ar, aw);
    let initiator = initiator.connect_rw(br, bw);
    Ok((responder, initiator))
}

pub fn next_event<IO>(
    mut proto: Protocol<IO>,
) -> impl Future<Output = (Protocol<IO>, io::Result<Event>)>
where
    IO: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let task = task::spawn(async move {
        let e1 = proto.next().await;
        let e1 = e1.unwrap();
        (proto, e1)
    });
    task
}

pub fn event_discovery_key(event: Event) -> DiscoveryKey {
    if let Event::DiscoveryKey(dkey) = event {
        dkey
    } else {
        panic!("Expected discovery key event");
    }
}

pub fn event_channel(event: Event) -> Channel {
    if let Event::Channel(channel) = event {
        channel
    } else {
        panic!("Expected channel event");
    }
}

/// Drive a protocol stream until the first channel arrives.
pub fn drive_until_channel<IO>(
    mut proto: Protocol<IO>,
) -> JoinHandle<io::Result<(Protocol<IO>, Channel)>>
where
    IO: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    task::spawn(async move {
        while let Some(event) = proto.next().await {
            let event = event?;
            match event {
                Event::Channel(channel) => return Ok((proto, channel)),
                _ => {}
            }
        }
        Err(io::Error::new(
            io::ErrorKind::Interrupted,
            "Protocol closed before a channel was opened",
        ))
    })
}
