use async_channel::{bounded, unbounded, Receiver, Sender};
use futures_lite::stream::StreamExt;
use hypercore_protocol::{Duplex, Protocol, ProtocolBuilder};
use hypermerge::{Hypermerge, StateEvent, SynchronizeEvent, ROOT};
use random_access_memory::RandomAccessMemory;

pub async fn setup_hypermerge_mesh(peers: usize) -> (Vec<Sender<u64>>, Vec<Receiver<StateEvent>>) {
    let mut hypermerge_creator: Hypermerge<RandomAccessMemory> =
        Hypermerge::create_doc_memory("p1", vec![("version", 1)]).await;
    hypermerge_creator.watch(vec![ROOT]).await;
    let (creator_state_event_sender, mut creator_state_event_receiver): (
        Sender<StateEvent>,
        Receiver<StateEvent>,
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
            Sender<StateEvent>,
            Receiver<StateEvent>,
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

        let (append_index_sender, append_index_receiver): (Sender<u64>, Receiver<u64>) = bounded(1);
        async_std::task::spawn(async move {
            append_value(&peer_name, hypermerge_peer, append_index_receiver).await;
        });

        // TODO: Check what these should be for peers > 3
        let mut sync_remaining = i + 1;
        let mut remote_sync_remaining = if i == 1 { 2 } else { 5 };
        let mut events = futures_lite::stream::race(
            &mut creator_state_event_receiver,
            &mut peer_state_event_receiver,
        );
        while let Some(event) = events.next().await {
            println!("GOT EVENT {:?}", event);
            match event {
                StateEvent::RemotePeerSynced(..) => {
                    remote_sync_remaining -= 1;
                    if sync_remaining == 0 && remote_sync_remaining == 0 {
                        break;
                    }
                }
                StateEvent::PeerSynced(..) => {
                    sync_remaining -= 1;
                    if sync_remaining == 0 && remote_sync_remaining == 0 {
                        break;
                    }
                }
                _ => {}
            }
        }
        println!("------------- FINISHED WITH PEER {}", i);
        senders.push(append_index_sender);
        receivers.push(peer_state_event_receiver);
    }

    let (append_index_sender, append_index_receiver): (Sender<u64>, Receiver<u64>) = bounded(1);
    async_std::task::spawn(async move {
        append_value("p1", hypermerge_creator, append_index_receiver).await;
    });
    senders.push(append_index_sender);
    receivers.push(creator_state_event_receiver);

    (senders, receivers)
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
    state_event_sender: Sender<StateEvent>,
) {
    let (mut sync_event_sender, mut sync_event_receiver): (
        Sender<SynchronizeEvent>,
        Receiver<SynchronizeEvent>,
    ) = unbounded();
    let mut hypermerge_for_task = hypermerge.clone();
    async_std::task::spawn(async move {
        hypermerge_for_task
            .connect_document(state_event_sender, &mut sync_event_receiver)
            .await
            .expect("connect_document should not throw error");
    });
    hypermerge
        .connect_protocol_memory(&mut protocol, &mut sync_event_sender)
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
