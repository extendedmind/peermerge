use futures::channel::mpsc::UnboundedSender;
use peermerge::{
    FeedDiskPersistence, FeedMemoryPersistence, Peermerge, PeermergeError, ProtocolBuilder,
    StateEvent,
};
use random_access_disk::RandomAccessDisk;
use random_access_memory::RandomAccessMemory;
use tracing::instrument;

#[cfg(feature = "tokio")]
use async_compat::CompatExt;
#[cfg(feature = "async-std")]
use async_std::{
    net::{TcpListener, TcpStream},
    task,
};
#[cfg(feature = "async-std")]
use futures::StreamExt;
#[cfg(feature = "tokio")]
use tokio::{
    net::{TcpListener, TcpStream},
    task,
};

//////////////////////////////////////////////////////
//
// Memory

#[cfg(feature = "async-std")]
#[instrument(skip_all, fields(peer_name = peermerge.peer_name(), host = host, port = port))]
pub async fn connect_tcp_server_memory(
    peermerge: Peermerge<RandomAccessMemory, FeedMemoryPersistence>,
    host: &str,
    port: u16,
    state_event_sender: &mut UnboundedSender<StateEvent>,
) -> Result<(), PeermergeError> {
    let listener = TcpListener::bind(&format!("{}:{}", host, port)).await?;
    let mut incoming = listener.incoming();
    while let Some(Ok(stream)) = incoming.next().await {
        let mut peermerge_for_task = peermerge.clone();
        let mut state_event_sender_for_task = state_event_sender.clone();
        task::spawn(async move {
            let mut protocol = ProtocolBuilder::new(false).connect(stream);
            peermerge_for_task
                .connect_protocol_memory(&mut protocol, &mut state_event_sender_for_task)
                .await
                .expect("Should return ok");
        });
    }
    Ok(())
}

#[cfg(feature = "tokio")]
#[instrument(skip_all, fields(peer_name = peermerge.peer_name(), host = host, port = port))]
pub async fn connect_tcp_server_memory(
    peermerge: Peermerge<RandomAccessMemory, FeedMemoryPersistence>,
    host: &str,
    port: u16,
    state_event_sender: &mut UnboundedSender<StateEvent>,
) -> Result<(), PeermergeError> {
    let listener = TcpListener::bind(&format!("{}:{}", host, port)).await?;

    while let Ok((stream, _peer_address)) = listener.accept().await {
        let mut peermerge_for_task = peermerge.clone();
        let mut state_event_sender_for_task = state_event_sender.clone();
        task::spawn(async move {
            let mut protocol = ProtocolBuilder::new(false).connect(stream.compat());
            peermerge_for_task
                .connect_protocol_memory(&mut protocol, &mut state_event_sender_for_task)
                .await
                .expect("Should return ok");
        });
    }
    Ok(())
}

#[cfg(feature = "async-std")]
#[instrument(skip_all, fields(peer_name = peermerge.peer_name(), host = host, port = port))]
pub async fn connect_tcp_client_memory(
    mut peermerge: Peermerge<RandomAccessMemory, FeedMemoryPersistence>,
    host: &str,
    port: u16,
    state_event_sender: &mut UnboundedSender<StateEvent>,
) -> Result<(), PeermergeError> {
    let stream = TcpStream::connect(&format!("{}:{}", host, port)).await?;
    let mut protocol = ProtocolBuilder::new(true).connect(stream);
    peermerge
        .connect_protocol_memory(&mut protocol, state_event_sender)
        .await
        .expect("Should return ok");
    Ok(())
}

#[cfg(feature = "tokio")]
#[instrument(skip_all, fields(peer_name = peermerge.peer_name(), host = host, port = port))]
pub async fn connect_tcp_client_memory(
    mut peermerge: Peermerge<RandomAccessMemory, FeedMemoryPersistence>,
    host: &str,
    port: u16,
    state_event_sender: &mut UnboundedSender<StateEvent>,
) -> Result<(), PeermergeError> {
    let stream = TcpStream::connect(&format!("{}:{}", host, port)).await?;
    let mut protocol = ProtocolBuilder::new(true).connect(stream.compat());
    peermerge
        .connect_protocol_memory(&mut protocol, state_event_sender)
        .await
        .expect("Should return ok");
    Ok(())
}

//////////////////////////////////////////////////////
//
// Disk

#[cfg(feature = "async-std")]
#[instrument(skip_all, fields(peer_name = peermerge.peer_name(), host = host, port = port))]
pub async fn connect_tcp_server_disk(
    peermerge: Peermerge<RandomAccessDisk, FeedDiskPersistence>,
    host: &str,
    port: u16,
    state_event_sender: &mut UnboundedSender<StateEvent>,
) -> Result<(), PeermergeError> {
    let listener = TcpListener::bind(&format!("{}:{}", host, port)).await?;
    let mut incoming = listener.incoming();
    while let Some(Ok(stream)) = incoming.next().await {
        let mut peermerge_for_task = peermerge.clone();
        let mut state_event_sender_for_task = state_event_sender.clone();
        task::spawn(async move {
            let mut protocol = ProtocolBuilder::new(false).connect(stream);
            peermerge_for_task
                .connect_protocol_disk(&mut protocol, &mut state_event_sender_for_task)
                .await
                .expect("Should return ok");
        });
    }
    Ok(())
}

#[cfg(feature = "tokio")]
#[instrument(skip_all, fields(peer_name = peermerge.peer_name(), host = host, port = port))]
pub async fn connect_tcp_server_disk(
    peermerge: Peermerge<RandomAccessDisk, FeedDiskPersistence>,
    host: &str,
    port: u16,
    state_event_sender: &mut UnboundedSender<StateEvent>,
) -> Result<(), PeermergeError> {
    let listener = TcpListener::bind(&format!("{}:{}", host, port)).await?;

    while let Ok((stream, _peer_address)) = listener.accept().await {
        let mut peermerge_for_task = peermerge.clone();
        let mut state_event_sender_for_task = state_event_sender.clone();
        task::spawn(async move {
            let mut protocol = ProtocolBuilder::new(false).connect(stream.compat());
            peermerge_for_task
                .connect_protocol_disk(&mut protocol, &mut state_event_sender_for_task)
                .await
                .expect("Should return ok");
        });
    }
    Ok(())
}

#[cfg(feature = "async-std")]
#[instrument(skip_all, fields(peer_name = peermerge.peer_name(), host = host, port = port))]
pub async fn connect_tcp_client_disk(
    mut peermerge: Peermerge<RandomAccessDisk, FeedDiskPersistence>,
    host: &str,
    port: u16,
    state_event_sender: &mut UnboundedSender<StateEvent>,
) -> Result<(), PeermergeError> {
    let stream = TcpStream::connect(&format!("{}:{}", host, port)).await?;
    let mut protocol = ProtocolBuilder::new(true).connect(stream);
    peermerge
        .connect_protocol_disk(&mut protocol, state_event_sender)
        .await
        .expect("Should return ok");
    Ok(())
}

#[cfg(feature = "tokio")]
#[instrument(skip_all, fields(peer_name = peermerge.peer_name(), host = host, port = port))]
pub async fn connect_tcp_client_disk(
    mut peermerge: Peermerge<RandomAccessDisk, FeedDiskPersistence>,
    host: &str,
    port: u16,
    state_event_sender: &mut UnboundedSender<StateEvent>,
) -> Result<(), PeermergeError> {
    let stream = TcpStream::connect(&format!("{}:{}", host, port)).await?;
    let mut protocol = ProtocolBuilder::new(true).connect(stream.compat());
    peermerge
        .connect_protocol_disk(&mut protocol, state_event_sender)
        .await
        .expect("Should return ok");
    Ok(())
}
