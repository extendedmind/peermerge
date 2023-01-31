use futures::channel::mpsc::{
    channel, unbounded, Receiver, Sender, UnboundedReceiver, UnboundedSender,
};
use futures::stream::StreamExt;
use hypercore_protocol::{Duplex, Protocol, ProtocolBuilder};
use peermerge::{Peermerge, StateEvent, ROOT};
use random_access_memory::RandomAccessMemory;

#[cfg(feature = "async-std")]
use async_std::task;
#[cfg(feature = "tokio")]
use tokio::task;

pub async fn setup_peermerge_mesh(
    peers: usize,
    encrypted: bool,
) -> (Vec<Sender<u64>>, UnboundedReceiver<StateEvent>) {
    let mut peermerge_creator: Peermerge<RandomAccessMemory> =
        Peermerge::create_new_memory("p1", vec![("version", 1)], encrypted).await;
    let encryption_key = peermerge_creator.encryption_key();
    peermerge_creator.watch(vec![ROOT]).await;
    let (state_event_sender, mut state_event_receiver): (
        UnboundedSender<StateEvent>,
        UnboundedReceiver<StateEvent>,
    ) = unbounded();
    let mut senders = Vec::with_capacity(peers);
    let doc_url = peermerge_creator.doc_url();

    for i in 1..peers {
        let (proto_responder, proto_initiator) = create_pair_memory().await;
        let peermerge_creator_for_task = peermerge_creator.clone();
        let state_event_sender_for_task = state_event_sender.clone();
        task::spawn(async move {
            connect(
                peermerge_creator_for_task,
                proto_responder,
                state_event_sender_for_task,
            )
            .await;
        });

        let peer_name = format!("p{}", i + 1);
        let mut peermerge_peer =
            Peermerge::attach_write_peer_memory(&peer_name, &doc_url, &encryption_key).await;
        peermerge_peer.watch(vec![ROOT]).await;

        let peermerge_peer_for_task = peermerge_peer.clone();
        let state_event_sender_for_task = state_event_sender.clone();
        let task_span = tracing::debug_span!("call_connect").or_current();
        task::spawn(async move {
            let _entered = task_span.enter();
            connect(
                peermerge_peer_for_task,
                proto_initiator,
                state_event_sender_for_task,
            )
            .await;
        });

        let (append_index_sender, append_index_receiver): (Sender<u64>, Receiver<u64>) = channel(1);
        let task_span = tracing::debug_span!("call_append_value").or_current();
        task::spawn(async move {
            let _entered = task_span.enter();
            append_value(&peer_name, peermerge_peer, append_index_receiver).await;
        });

        // TODO: Check what these should be for peers > 3
        let mut sync_remaining = i * 2;
        let mut remote_sync_remaining = i * 2;

        while let Some(event) = state_event_receiver.next().await {
            match event {
                StateEvent::RemotePeerSynced(..) => {
                    remote_sync_remaining -= 1;
                }
                StateEvent::PeerSynced(..) => {
                    sync_remaining -= 1;
                }
                StateEvent::DocumentChanged(..) => {
                    // Ignore
                }
            }
            if sync_remaining == 0 && remote_sync_remaining == 0 {
                break;
            }
        }
        senders.push(append_index_sender);
    }

    let (append_index_sender, append_index_receiver): (Sender<u64>, Receiver<u64>) = channel(1);
    let task_span = tracing::debug_span!("call_append_value").or_current();
    task::spawn(async move {
        let _entered = task_span.enter();
        append_value("p1", peermerge_creator, append_index_receiver).await;
    });
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

async fn connect(
    mut peermerge: Peermerge<RandomAccessMemory>,
    mut protocol: MemoryProtocol,
    mut state_event_sender: UnboundedSender<StateEvent>,
) {
    peermerge
        .connect_protocol_memory(&mut protocol, &mut state_event_sender)
        .await
        .expect("connect_protocol_memory should not throw error");
}

async fn append_value(
    peer_name: &str,
    mut peermerge: Peermerge<RandomAccessMemory>,
    mut append_index_receiver: Receiver<u64>,
) {
    while let Some(i) = append_index_receiver.next().await {
        peermerge
            .put_scalar(ROOT, format!("{}_{}", peer_name, i), i)
            .await
            .unwrap();
    }
}
