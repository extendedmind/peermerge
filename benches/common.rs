use futures::channel::mpsc::{
    channel, unbounded, Receiver, Sender, UnboundedReceiver, UnboundedSender,
};
use futures::select;
use futures::stream::StreamExt;
use hypercore_protocol::{Duplex, Protocol, ProtocolBuilder};
use hypermerge::{Hypermerge, StateEvent, ROOT};
use random_access_memory::RandomAccessMemory;

pub async fn setup_hypermerge_mesh(
    peers: usize,
) -> (Vec<Sender<u64>>, Vec<UnboundedReceiver<StateEvent>>) {
    let mut hypermerge_creator: Hypermerge<RandomAccessMemory> =
        Hypermerge::create_doc_memory("p1", vec![("version", 1)]).await;
    hypermerge_creator.watch(vec![ROOT]).await;
    let (creator_state_event_sender, mut creator_state_event_receiver): (
        UnboundedSender<StateEvent>,
        UnboundedReceiver<StateEvent>,
    ) = unbounded();
    let mut receivers = Vec::with_capacity(peers);
    let mut senders = Vec::with_capacity(peers);
    let doc_url = hypermerge_creator.doc_url();

    for i in 1..peers {
        let (proto_responder, proto_initiator) = create_pair_memory().await;
        let hypermerge_creator_for_task = hypermerge_creator.clone();
        let creator_state_event_sender_for_task = creator_state_event_sender.clone();
        async_std::task::spawn(async move {
            connect(
                hypermerge_creator_for_task,
                proto_responder,
                creator_state_event_sender_for_task,
            )
            .await;
        });

        let peer_name = format!("p{}", i + 1);
        let mut hypermerge_peer = Hypermerge::register_doc_memory(&peer_name, &doc_url).await;
        hypermerge_peer.watch(vec![ROOT]).await;
        let (peer_state_event_sender, mut peer_state_event_receiver): (
            UnboundedSender<StateEvent>,
            UnboundedReceiver<StateEvent>,
        ) = unbounded();

        let hypermerge_peer_for_task = hypermerge_peer.clone();
        async_std::task::spawn(async move {
            connect(
                hypermerge_peer_for_task,
                proto_initiator,
                peer_state_event_sender,
            )
            .await;
        });

        let (append_index_sender, append_index_receiver): (Sender<u64>, Receiver<u64>) = channel(1);
        async_std::task::spawn(async move {
            append_value(&peer_name, hypermerge_peer, append_index_receiver).await;
        });

        // TODO: Check what these should be for peers > 3
        let mut sync_remaining = i + 1;
        let mut remote_sync_remaining = if i == 1 { 2 } else { 5 };

        while sync_remaining > 0 || remote_sync_remaining > 0 {
            select!(
                event = creator_state_event_receiver.next() => {
                    println!("CREATOR RES {:?}", event);
                    handle_event(&event.unwrap(), &mut sync_remaining, &mut remote_sync_remaining);
                },
                event = peer_state_event_receiver.next() => {
                    println!("PEER {} RES {:?}", i, event);
                    handle_event(&event.unwrap(), &mut sync_remaining, &mut remote_sync_remaining);
                }
            );
        }
        println!("------------- FINISHED WITH PEER {}", i);
        senders.push(append_index_sender);
        receivers.push(peer_state_event_receiver);
    }

    let (append_index_sender, append_index_receiver): (Sender<u64>, Receiver<u64>) = channel(1);
    async_std::task::spawn(async move {
        append_value("p1", hypermerge_creator, append_index_receiver).await;
    });
    senders.push(append_index_sender);
    receivers.push(creator_state_event_receiver);

    (senders, receivers)
}

fn handle_event(event: &StateEvent, sync_remaining: &mut usize, remote_sync_remaining: &mut usize) {
    match event {
        StateEvent::RemotePeerSynced(..) => {
            *remote_sync_remaining -= 1;
        }
        StateEvent::PeerSynced(..) => {
            *sync_remaining -= 1;
        }
        _ => {}
    }
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
    mut hypermerge: Hypermerge<RandomAccessMemory>,
    mut protocol: MemoryProtocol,
    mut state_event_sender: UnboundedSender<StateEvent>,
) {
    hypermerge
        .connect_protocol_memory(&mut protocol, &mut state_event_sender)
        .await
        .expect("connect_protocol_memory should not throw error");
}

async fn append_value(
    peer_name: &str,
    mut hypermerge: Hypermerge<RandomAccessMemory>,
    mut append_index_receiver: Receiver<u64>,
) {
    while let Some(i) = append_index_receiver.next().await {
        hypermerge
            .put_scalar(ROOT, format!("{}_{}", peer_name, i), i)
            .await
            .unwrap();
    }
}
