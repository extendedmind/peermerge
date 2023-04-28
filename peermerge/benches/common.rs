use std::fmt::Debug;

use futures::channel::mpsc::{
    channel, unbounded, Receiver, Sender, UnboundedReceiver, UnboundedSender,
};
use futures::stream::StreamExt;
use hypercore_protocol::{Duplex, Protocol, ProtocolBuilder};
use peermerge::{
    DocumentId, FeedDiskPersistence, FeedMemoryPersistence, FeedPersistence, NameDescription,
    Peermerge, RandomAccess, StateEvent, StateEventContent::*, ROOT,
};
use random_access_disk::RandomAccessDisk;
use random_access_memory::RandomAccessMemory;
use tempfile::Builder as TempfileBuilder;

#[cfg(feature = "async-std")]
use async_std::task;
#[cfg(feature = "tokio")]
use tokio::task;

pub async fn setup_peermerge_mesh_memory(
    peers: usize,
    encrypted: bool,
) -> (Vec<Sender<u64>>, UnboundedReceiver<StateEvent>) {
    let creator_name = "p1";
    let (state_event_sender, mut state_event_receiver): (
        UnboundedSender<StateEvent>,
        UnboundedReceiver<StateEvent>,
    ) = unbounded();
    let mut peermerge_creator: Peermerge<RandomAccessMemory, FeedMemoryPersistence> =
        Peermerge::new_memory(
            NameDescription::new(creator_name),
            Some(state_event_sender.clone()),
        )
        .await;
    let doc_info = peermerge_creator
        .create_new_document_memory(
            NameDescription::new("bench"),
            vec![("version", 1)],
            encrypted,
        )
        .await
        .unwrap();
    let encryption_key = peermerge_creator.encryption_key(&doc_info.id()).await;
    peermerge_creator.watch(&doc_info.id(), vec![ROOT]).await;

    let mut senders = Vec::with_capacity(peers);
    let doc_url = peermerge_creator.doc_url(&doc_info.id()).await;

    for i in 1..peers {
        let (proto_responder, proto_initiator) = create_pair_memory().await;
        let peermerge_creator_for_task = peermerge_creator.clone();
        task::spawn(async move {
            connect_memory(peermerge_creator_for_task, proto_responder).await;
        });

        let peer_name = format!("p{}", i + 1);
        let mut peermerge_peer = Peermerge::new_memory(
            NameDescription::new(&peer_name),
            Some(state_event_sender.clone()),
        )
        .await;
        let _doc_info = peermerge_peer
            .attach_writer_document_memory(&doc_url, &encryption_key)
            .await
            .unwrap();
        peermerge_peer.watch(&doc_info.id(), vec![ROOT]).await;

        let peermerge_peer_for_task = peermerge_peer.clone();
        let task_span = tracing::debug_span!("call_connect").or_current();
        task::spawn(async move {
            let _entered = task_span.enter();
            connect_memory(peermerge_peer_for_task, proto_initiator).await;
        });

        let append_index_sender = append_and_process_events(
            i,
            peer_name,
            peermerge_peer,
            doc_info.id(),
            &mut state_event_receiver,
        )
        .await;
        senders.push(append_index_sender);
    }

    let append_index_sender =
        append_value_in_task(creator_name.to_string(), peermerge_creator, doc_info.id());
    senders.push(append_index_sender);

    (senders, state_event_receiver)
}

pub async fn setup_peermerge_mesh_disk(
    peers: usize,
    encrypted: bool,
) -> (Vec<Sender<u64>>, UnboundedReceiver<StateEvent>) {
    let creator_name = "p1";
    let creator_dir = TempfileBuilder::new()
        .prefix(&format!(
            "{}_{}",
            creator_name,
            if encrypted { "encrypted" } else { "plain" }
        ))
        .tempdir()
        .unwrap()
        .into_path();
    let (state_event_sender, mut state_event_receiver): (
        UnboundedSender<StateEvent>,
        UnboundedReceiver<StateEvent>,
    ) = unbounded();
    let mut peermerge_creator: Peermerge<RandomAccessDisk, FeedDiskPersistence> =
        Peermerge::create_new_disk(
            NameDescription::new(creator_name),
            Some(state_event_sender.clone()),
            &creator_dir,
        )
        .await;
    let doc_info = peermerge_creator
        .create_new_document_disk(
            NameDescription::new("bench"),
            vec![("version", 1)],
            encrypted,
        )
        .await
        .unwrap();
    let encryption_key = peermerge_creator.encryption_key(&doc_info.id()).await;
    peermerge_creator.watch(&doc_info.id(), vec![ROOT]).await;

    let mut senders = Vec::with_capacity(peers);
    let doc_url = peermerge_creator.doc_url(&doc_info.id()).await;

    for i in 1..peers {
        let (proto_responder, proto_initiator) = create_pair_memory().await;
        let peermerge_creator_for_task = peermerge_creator.clone();
        task::spawn(async move {
            connect_disk(peermerge_creator_for_task, proto_responder).await;
        });

        let peer_name = format!("p{}", i + 1);
        let peer_dir = TempfileBuilder::new()
            .prefix(&format!(
                "{}_{}",
                peer_name,
                if encrypted { "encrypted" } else { "plain" }
            ))
            .tempdir()
            .unwrap()
            .into_path();
        let mut peermerge_peer = Peermerge::create_new_disk(
            NameDescription::new(&peer_name),
            Some(state_event_sender.clone()),
            &peer_dir,
        )
        .await;
        let doc_info = peermerge_peer
            .attach_writer_document_disk(&doc_url, &encryption_key)
            .await
            .unwrap();
        peermerge_peer.watch(&doc_info.id(), vec![ROOT]).await;

        let peermerge_peer_for_task = peermerge_peer.clone();
        let task_span = tracing::debug_span!("call_connect").or_current();
        task::spawn(async move {
            let _entered = task_span.enter();
            connect_disk(peermerge_peer_for_task, proto_initiator).await;
        });

        let append_index_sender = append_and_process_events(
            i,
            peer_name,
            peermerge_peer,
            doc_info.id(),
            &mut state_event_receiver,
        )
        .await;
        senders.push(append_index_sender);
    }

    let append_index_sender =
        append_value_in_task(creator_name.to_string(), peermerge_creator, doc_info.id());
    senders.push(append_index_sender);

    (senders, state_event_receiver)
}

type MemoryProtocol = Protocol<Duplex<sluice::pipe::PipeReader, sluice::pipe::PipeWriter>>;
async fn create_pair_memory() -> (MemoryProtocol, MemoryProtocol) {
    let (ar, bw) = sluice::pipe::pipe();
    let (br, aw) = sluice::pipe::pipe();

    let responder = ProtocolBuilder::new(false);
    let initiator = ProtocolBuilder::new(true);
    let responder = responder.connect_rw(ar, aw);
    let initiator = initiator.connect_rw(br, bw);
    (responder, initiator)
}

async fn connect_memory(
    mut peermerge: Peermerge<RandomAccessMemory, FeedMemoryPersistence>,
    mut protocol: MemoryProtocol,
) {
    peermerge
        .connect_protocol_memory(&mut protocol)
        .await
        .expect("connect_protocol_memory should not throw error");
}

async fn connect_disk(
    mut peermerge: Peermerge<RandomAccessDisk, FeedDiskPersistence>,
    mut protocol: MemoryProtocol,
) {
    peermerge
        .connect_protocol_disk(&mut protocol)
        .await
        .expect("connect_protocol_disk should not throw error");
}

async fn append_and_process_events<T, U>(
    i: usize,
    peer_name: String,
    peermerge_peer: Peermerge<T, U>,
    doc_id: DocumentId,
    state_event_receiver: &mut UnboundedReceiver<StateEvent>,
) -> Sender<u64>
where
    T: RandomAccess + Debug + Send + 'static,
    U: FeedPersistence,
{
    let append_index_sender = append_value_in_task(peer_name, peermerge_peer, doc_id);

    // TODO: Check what these should be for peers > 3
    let mut sync_remaining = i * 2;
    let mut remote_sync_remaining = if i == 1 { 3 } else { 5 };
    let mut document_initialized_remaining = if i == 1 { 2 } else { 1 };

    while let Some(event) = state_event_receiver.next().await {
        match event.content {
            RemotePeerSynced(..) => {
                remote_sync_remaining -= 1;
            }
            PeerSynced(..) => {
                sync_remaining -= 1;
            }
            DocumentChanged(..) => {
                // Ignore
            }
            DocumentInitialized(..) => {
                document_initialized_remaining -= 1;
            }
            Reattached(_) => {
                panic!("Should not get reattached");
            }
        }

        if sync_remaining == 0 && remote_sync_remaining == 0 && document_initialized_remaining == 0
        {
            break;
        }
    }
    append_index_sender
}

fn append_value_in_task<T, U>(
    peer_name: String,
    peermerge_peer: Peermerge<T, U>,
    doc_id: DocumentId,
) -> Sender<u64>
where
    T: RandomAccess + Debug + Send + 'static,
    U: FeedPersistence,
{
    let (append_index_sender, append_index_receiver): (Sender<u64>, Receiver<u64>) = channel(1);
    let task_span = tracing::debug_span!("call_append_value").or_current();
    task::spawn(async move {
        let _entered = task_span.enter();
        append_value(&peer_name, peermerge_peer, doc_id, append_index_receiver).await;
    });
    append_index_sender
}

async fn append_value<T, U>(
    peer_name: &str,
    mut peermerge: Peermerge<T, U>,
    doc_id: DocumentId,
    mut append_index_receiver: Receiver<u64>,
) where
    T: RandomAccess + Debug + Send + 'static,
    U: FeedPersistence,
{
    while let Some(i) = append_index_receiver.next().await {
        peermerge
            .put_scalar(&doc_id, ROOT, format!("{peer_name}_{i}"), i)
            .await
            .unwrap();
    }
}
