use futures::{
    channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender},
    future::join_all,
    StreamExt,
};
use peermerge::{
    automerge::{transaction::Transactable, ROOT},
    AccessType, AttachDocumentDiskOptionsBuilder, AttachDocumentMemoryOptionsBuilder,
    CreateNewDocumentDiskOptionsBuilder, CreateNewDocumentMemoryOptionsBuilder, DocumentId,
    FeedDiskPersistence, FeedMemoryPersistence, FeedPersistence, NameDescription, PeerId,
    Peermerge, PeermergeDiskOptionsBuilder, PeermergeMemoryOptionsBuilder, StateEvent,
    StateEventContent::*,
};
use random_access_disk::RandomAccessDisk;
use random_access_memory::RandomAccessMemory;
use random_access_storage::RandomAccess;
use std::{collections::HashMap, fmt::Debug};
use tempfile::Builder;
use test_log::test;
use tokio::time::{sleep, Duration};

#[cfg(feature = "async-std")]
use async_std::{sync::Mutex, task, test as async_test};
#[cfg(feature = "tokio")]
use tokio::{task, test as async_test};

pub mod common;
use common::*;

/// Test three main documents, each with two peers, and two shared documents,
/// one between all of them, an one between the first and second. All mediated
/// by a proxy.
#[test(async_test)]
async fn scale_three_main_documents_two_shared() -> anyhow::Result<()> {
    // Document 1: memory peers

    let mut peermerge_creator_1 = Peermerge::new_memory(
        PeermergeMemoryOptionsBuilder::default()
            .default_peer_header(NameDescription::new("creator_1"))
            .build()?,
    )
    .await?;
    let mut peermerge_joiner_1 = Peermerge::new_memory(
        PeermergeMemoryOptionsBuilder::default()
            .default_peer_header(NameDescription::new("joiner_1"))
            .build()?,
    )
    .await?;
    let creator_1_state_event_receiver =
        create_state_events_channel(&mut peermerge_creator_1).await?;
    let joiner_1_state_event_receiver =
        create_state_events_channel(&mut peermerge_joiner_1).await?;
    let (document_1_id, document_1_proxy_doc_url) = create_main_document_memory(
        "document_1",
        &mut peermerge_creator_1,
        &mut peermerge_joiner_1,
    )
    .await?;

    // Document 2: disk peers

    let creator_2_dir = Builder::new()
        .prefix("scale_three_main_documents_two_shared_creator_2")
        .tempdir()
        .unwrap()
        .into_path();
    let mut peermerge_creator_2 = Peermerge::new_disk(
        PeermergeDiskOptionsBuilder::default()
            .default_peer_header(NameDescription::new("creator_2"))
            .data_root_dir(creator_2_dir.clone())
            .build()?,
    )
    .await?;
    let joiner_2_dir = Builder::new()
        .prefix("scale_three_main_documents_two_shared_joiner_2")
        .tempdir()
        .unwrap()
        .into_path();

    let mut peermerge_joiner_2 = Peermerge::new_disk(
        PeermergeDiskOptionsBuilder::default()
            .default_peer_header(NameDescription::new("joiner_2"))
            .data_root_dir(joiner_2_dir.clone())
            .build()?,
    )
    .await?;
    let creator_2_state_event_receiver =
        create_state_events_channel(&mut peermerge_creator_2).await?;
    let joiner_2_state_event_receiver =
        create_state_events_channel(&mut peermerge_joiner_2).await?;
    let (document_2_id, document_2_proxy_doc_url) = create_main_document_disk(
        "document_2",
        &mut peermerge_creator_2,
        &mut peermerge_joiner_2,
    )
    .await?;

    // Document 3: memory peers

    let mut peermerge_creator_3 = Peermerge::new_memory(
        PeermergeMemoryOptionsBuilder::default()
            .default_peer_header(NameDescription::new("creator_3"))
            .build()?,
    )
    .await?;
    let mut peermerge_joiner_3 = Peermerge::new_memory(
        PeermergeMemoryOptionsBuilder::default()
            .default_peer_header(NameDescription::new("joiner_3"))
            .build()?,
    )
    .await?;
    let creator_3_state_event_receiver =
        create_state_events_channel(&mut peermerge_creator_3).await?;
    let joiner_3_state_event_receiver =
        create_state_events_channel(&mut peermerge_joiner_3).await?;
    let (document_3_id, document_3_proxy_doc_url) = create_main_document_memory(
        "document_3",
        &mut peermerge_creator_3,
        &mut peermerge_joiner_3,
    )
    .await?;

    // Shared document A: between all of the parties

    let (shared_doc_info_a, _) = peermerge_joiner_1
        .create_new_document_memory(
            CreateNewDocumentMemoryOptionsBuilder::default()
                .document_type("scale_shared".to_string())
                .document_header(NameDescription::new("a"))
                .parent_id(document_1_id)
                .build()?,
            |tx| tx.put(ROOT, "shared_counter", 0),
        )
        .await?;
    let shared_doc_id_a = shared_doc_info_a.id();
    assert!(shared_doc_info_a.static_info.child);
    let document_secret_a = peermerge_joiner_1
        .document_secret(&shared_doc_id_a)
        .await?
        .unwrap();
    let sharing_info_a = peermerge_joiner_1.sharing_info(&shared_doc_id_a).await?;
    let shared_doc_info_attach_a = peermerge_creator_2
        .attach_document_disk(
            AttachDocumentDiskOptionsBuilder::default()
                .document_url(sharing_info_a.doc_url.clone())
                .document_secret(document_secret_a.clone())
                .parent_id(document_2_id)
                .parent_header(NameDescription::new("document_2_override"))
                .build()?,
        )
        .await?;
    assert!(shared_doc_info_attach_a.static_info.child);
    let shared_doc_info_attach_a = peermerge_joiner_3
        .attach_document_memory(
            AttachDocumentMemoryOptionsBuilder::default()
                .document_url(sharing_info_a.doc_url)
                .document_secret(document_secret_a)
                .parent_id(document_3_id)
                .parent_header(NameDescription::new("document_3_override"))
                .build()?,
        )
        .await?;
    assert!(shared_doc_info_attach_a.static_info.child);

    // Now connect the peers and wait for syncs, including new documents

    connect_peers_memory(&mut peermerge_creator_1, &mut peermerge_joiner_1).await?;
    connect_peers_disk(&mut peermerge_creator_2, &mut peermerge_joiner_2).await?;
    connect_peers_memory(&mut peermerge_creator_3, &mut peermerge_joiner_3).await?;

    wait_for_peers_synced(
        vec![
            (
                peermerge_creator_1.peer_id(),
                peermerge_joiner_1.peer_id(),
                creator_1_state_event_receiver,
                document_1_id,
                shared_doc_id_a,
            ),
            (
                peermerge_creator_2.peer_id(),
                peermerge_joiner_2.peer_id(),
                creator_2_state_event_receiver,
                document_2_id,
                shared_doc_id_a,
            ),
            (
                peermerge_creator_3.peer_id(),
                peermerge_joiner_3.peer_id(),
                creator_3_state_event_receiver,
                document_3_id,
                shared_doc_id_a,
            ),
        ],
        vec![
            (
                peermerge_joiner_1.peer_id(),
                peermerge_creator_1.peer_id(),
                joiner_1_state_event_receiver,
                document_1_id,
                shared_doc_id_a,
            ),
            (
                peermerge_joiner_2.peer_id(),
                peermerge_creator_2.peer_id(),
                joiner_2_state_event_receiver,
                document_2_id,
                shared_doc_id_a,
            ),
            (
                peermerge_joiner_3.peer_id(),
                peermerge_creator_3.peer_id(),
                joiner_3_state_event_receiver,
                document_3_id,
                shared_doc_id_a,
            ),
        ],
    )
    .await?;

    // Shared document B: between first and second peer

    let (shared_doc_info_b, _) = peermerge_creator_2
        .create_new_document_disk(
            CreateNewDocumentDiskOptionsBuilder::default()
                .document_type("scale_shared".to_string())
                .document_header(NameDescription::new("b"))
                .parent_id(document_2_id)
                .build()?,
            |tx| tx.put(ROOT, "shared_counter", 0),
        )
        .await?;
    let shared_doc_id_b = shared_doc_info_b.id();
    assert!(shared_doc_info_b.static_info.child);
    let document_secret_b = peermerge_creator_2
        .document_secret(&shared_doc_id_b)
        .await?
        .unwrap();
    let sharing_info_b = peermerge_creator_2.sharing_info(&shared_doc_id_b).await?;
    let shared_doc_info_attach_b = peermerge_creator_1
        .attach_document_memory(
            AttachDocumentMemoryOptionsBuilder::default()
                .document_url(sharing_info_b.doc_url)
                .document_secret(document_secret_b)
                .parent_id(document_1_id)
                .parent_header(NameDescription::new("document_1_override"))
                .build()?,
        )
        .await?;
    assert!(shared_doc_info_attach_b.static_info.child);

    println!("### SLEEP START...");
    sleep(Duration::from_millis(10000)).await;
    println!("### ...SLEEP END");

    // Use a proxy to sync peers of different documents together, asserting that the right document is
    // found in every peer that should have it. NB: The proxy doesn't need to know about child
    // documents, they are synced automatically.

    let mut peermerge_proxy = Peermerge::new_memory(
        PeermergeMemoryOptionsBuilder::default()
            .default_peer_header(NameDescription::new("proxy"))
            .build()?,
    )
    .await?;
    let proxy_info = peermerge_proxy
        .attach_document_memory(
            AttachDocumentMemoryOptionsBuilder::default()
                .document_url(document_1_proxy_doc_url)
                .build()?,
        )
        .await?;
    assert_eq!(proxy_info.access_type, AccessType::Proxy);
    let proxy_info = peermerge_proxy
        .attach_document_memory(
            AttachDocumentMemoryOptionsBuilder::default()
                .document_url(document_2_proxy_doc_url)
                .build()?,
        )
        .await?;
    assert_eq!(proxy_info.access_type, AccessType::Proxy);
    let proxy_info = peermerge_proxy
        .attach_document_memory(
            AttachDocumentMemoryOptionsBuilder::default()
                .document_url(document_3_proxy_doc_url)
                .build()?,
        )
        .await?;
    assert_eq!(proxy_info.access_type, AccessType::Proxy);

    let proxy_state_event_receiver = create_state_events_channel(&mut peermerge_proxy).await?;
    let creator_1_state_event_receiver =
        create_state_events_channel(&mut peermerge_creator_1).await?;
    let joiner_1_state_event_receiver =
        create_state_events_channel(&mut peermerge_joiner_1).await?;
    let creator_2_state_event_receiver =
        create_state_events_channel(&mut peermerge_creator_2).await?;
    let joiner_2_state_event_receiver =
        create_state_events_channel(&mut peermerge_joiner_2).await?;
    let creator_3_state_event_receiver =
        create_state_events_channel(&mut peermerge_creator_3).await?;
    let joiner_3_state_event_receiver =
        create_state_events_channel(&mut peermerge_joiner_3).await?;

    // Only one of the peers per document is connected, the mutual sync between the peers
    // from before will sync to the other peer not connected to the proxy.
    connect_peers_memory(&mut peermerge_proxy, &mut peermerge_creator_1).await?;
    connect_peers_disk_to_memory(&mut peermerge_proxy, &mut peermerge_joiner_2).await?;
    connect_peers_memory(&mut peermerge_proxy, &mut peermerge_creator_3).await?;

    wait_for_proxy_mediated_increments(
        proxy_state_event_receiver,
        vec![
            creator_3_state_event_receiver,
            joiner_3_state_event_receiver,
        ],
        vec![
            creator_1_state_event_receiver,
            joiner_1_state_event_receiver,
            creator_2_state_event_receiver,
            joiner_2_state_event_receiver,
        ],
    )
    .await?;

    Ok(())
}

async fn create_state_events_channel<T, U>(
    peermerge: &mut Peermerge<T, U>,
) -> anyhow::Result<UnboundedReceiver<StateEvent>>
where
    T: RandomAccess + Debug + Send + 'static,
    U: FeedPersistence,
{
    let (state_event_sender, state_event_receiver): (
        UnboundedSender<StateEvent>,
        UnboundedReceiver<StateEvent>,
    ) = unbounded();
    peermerge
        .set_state_event_sender(Some(state_event_sender))
        .await?;
    Ok(state_event_receiver)
}

async fn create_main_document_memory(
    document_name: &str,
    peermerge_creator: &mut Peermerge<RandomAccessMemory, FeedMemoryPersistence>,
    peermerge_joiner: &mut Peermerge<RandomAccessMemory, FeedMemoryPersistence>,
) -> anyhow::Result<(DocumentId, String)> {
    let (creator_doc_info, _) = peermerge_creator
        .create_new_document_memory(
            CreateNewDocumentMemoryOptionsBuilder::default()
                .document_type("scale".to_string())
                .document_header(NameDescription::new(document_name))
                .build()?,
            |tx| tx.put(ROOT, "counter", 0),
        )
        .await?;
    let document_id = creator_doc_info.id();
    let document_secret = peermerge_creator
        .document_secret(&creator_doc_info.id())
        .await?
        .unwrap();
    let sharing_info = peermerge_creator.sharing_info(&document_id).await?;
    peermerge_joiner
        .attach_document_memory(
            AttachDocumentMemoryOptionsBuilder::default()
                .document_url(sharing_info.doc_url)
                .document_secret(document_secret)
                .build()?,
        )
        .await?;
    Ok((document_id, sharing_info.proxy_doc_url))
}

async fn create_main_document_disk(
    document_name: &str,
    peermerge_creator: &mut Peermerge<RandomAccessDisk, FeedDiskPersistence>,
    peermerge_joiner: &mut Peermerge<RandomAccessDisk, FeedDiskPersistence>,
) -> anyhow::Result<(DocumentId, String)> {
    let (creator_doc_info, _) = peermerge_creator
        .create_new_document_disk(
            CreateNewDocumentDiskOptionsBuilder::default()
                .document_type("scale".to_string())
                .document_header(NameDescription::new(document_name))
                .build()?,
            |tx| tx.put(ROOT, "main", document_name),
        )
        .await?;
    let document_id = creator_doc_info.id();
    let document_secret = peermerge_creator
        .document_secret(&creator_doc_info.id())
        .await?
        .unwrap();
    let sharing_info = peermerge_creator.sharing_info(&document_id).await?;
    peermerge_joiner
        .attach_document_disk(
            AttachDocumentDiskOptionsBuilder::default()
                .document_url(sharing_info.doc_url)
                .document_secret(document_secret)
                .build()?,
        )
        .await?;
    Ok((document_id, sharing_info.proxy_doc_url))
}

async fn connect_peers_memory(
    responder: &mut Peermerge<RandomAccessMemory, FeedMemoryPersistence>,
    initiator: &mut Peermerge<RandomAccessMemory, FeedMemoryPersistence>,
) -> anyhow::Result<()> {
    let (mut proto_responder, mut proto_initiator) = create_pair_memory().await;
    let mut responder_for_task = responder.clone();
    task::spawn(async move {
        responder_for_task
            .connect_protocol_memory(&mut proto_responder)
            .await
            .unwrap();
    });

    let mut initiator_for_task = initiator.clone();
    task::spawn(async move {
        initiator_for_task
            .connect_protocol_memory(&mut proto_initiator)
            .await
            .unwrap();
    });
    Ok(())
}

async fn connect_peers_disk(
    responder: &mut Peermerge<RandomAccessDisk, FeedDiskPersistence>,
    initiator: &mut Peermerge<RandomAccessDisk, FeedDiskPersistence>,
) -> anyhow::Result<()> {
    let (mut proto_responder, mut proto_initiator) = create_pair_memory().await;
    let mut responder_for_task = responder.clone();
    task::spawn(async move {
        responder_for_task
            .connect_protocol_disk(&mut proto_responder)
            .await
            .unwrap();
    });

    let mut initiator_for_task = initiator.clone();
    task::spawn(async move {
        initiator_for_task
            .connect_protocol_disk(&mut proto_initiator)
            .await
            .unwrap();
    });
    Ok(())
}

async fn connect_peers_disk_to_memory(
    responder: &mut Peermerge<RandomAccessMemory, FeedMemoryPersistence>,
    initiator: &mut Peermerge<RandomAccessDisk, FeedDiskPersistence>,
) -> anyhow::Result<()> {
    let (mut proto_responder, mut proto_initiator) = create_pair_memory().await;
    let mut responder_for_task = responder.clone();
    task::spawn(async move {
        responder_for_task
            .connect_protocol_memory(&mut proto_responder)
            .await
            .unwrap();
    });

    let mut initiator_for_task = initiator.clone();
    task::spawn(async move {
        initiator_for_task
            .connect_protocol_disk(&mut proto_initiator)
            .await
            .unwrap();
    });
    Ok(())
}

async fn wait_for_peers_synced(
    creators: Vec<(
        PeerId,
        PeerId,
        UnboundedReceiver<StateEvent>,
        DocumentId,
        DocumentId,
    )>,
    joiners: Vec<(
        PeerId,
        PeerId,
        UnboundedReceiver<StateEvent>,
        DocumentId,
        DocumentId,
    )>,
) -> anyhow::Result<()> {
    let mut processes = vec![];
    for (creator_peer_id, joiner_peer_id, creator_state_event_receiver, doc_id, child_doc_id) in
        creators
    {
        processes.push(task::spawn(async move {
            process_until_synced(
                creator_state_event_receiver,
                creator_peer_id,
                joiner_peer_id,
                doc_id,
                child_doc_id,
            )
            .await;
        }));
    }

    for (joiner_peer_id, creator_peer_id, joiner_state_event_receiver, doc_id, child_doc_id) in
        joiners
    {
        processes.push(task::spawn(async move {
            process_until_synced(
                joiner_state_event_receiver,
                joiner_peer_id,
                creator_peer_id,
                doc_id,
                child_doc_id,
            )
            .await;
        }));
    }

    join_all(processes).await;
    Ok(())
}

async fn process_until_synced(
    mut state_event_receiver: UnboundedReceiver<StateEvent>,
    local_peer_id: PeerId,
    remote_peer_id: PeerId,
    document_id: DocumentId,
    child_document_id: DocumentId,
) {
    let mut peer_synced: bool = false;
    let mut remote_peer_synced: bool = false;
    let mut documents_synced: usize = 0;
    while let Some(event) = state_event_receiver.next().await {
        match event.content {
            PeerSynced {
                peer_id,
                contiguous_length,
                ..
            } => {
                if peer_id == remote_peer_id && contiguous_length > 0 {
                    peer_synced = true;
                }
            }
            RemotePeerSynced {
                peer_id,
                contiguous_length,
                ..
            } => {
                if peer_id == local_peer_id && contiguous_length > 0 {
                    remote_peer_synced = true;
                }
            }
            DocumentInitialized {
                child,
                parent_document_ids,
                ..
            } => {
                println!("### {child}");
                if child
                    && child_document_id == event.document_id
                    && parent_document_ids == vec![document_id]
                {
                    documents_synced += 1;
                } else if !child
                    && document_id == event.document_id
                    && parent_document_ids.is_empty()
                {
                    documents_synced += 1;
                } else {
                    panic!("Unexpected DocumentInitialized");
                }
            }
            _ => {}
        }
        if peer_synced && remote_peer_synced && documents_synced == 2 {
            break;
        }
    }
}

async fn wait_for_proxy_mediated_increments(
    proxy_state_event_receiver: UnboundedReceiver<StateEvent>,
    two_document_state_event_receivers: Vec<UnboundedReceiver<StateEvent>>,
    three_document_state_event_receivers: Vec<UnboundedReceiver<StateEvent>>,
) -> anyhow::Result<()> {
    let proxy_process = task::spawn(async move {
        process_proxy_state_events(proxy_state_event_receiver)
            .await
            .unwrap();
    });
    let mut processes = vec![proxy_process];
    for state_event_receiver in two_document_state_event_receivers {
        processes.push(task::spawn(async move {
            process_two_documents_state_events(state_event_receiver)
                .await
                .unwrap();
        }));
    }
    for state_event_receiver in three_document_state_event_receivers {
        processes.push(task::spawn(async move {
            process_three_documents_state_events(state_event_receiver)
                .await
                .unwrap();
        }));
    }
    join_all(processes).await;
    Ok(())
}

async fn process_proxy_state_events(
    mut proxy_state_event_receiver: UnboundedReceiver<StateEvent>,
) -> anyhow::Result<()> {
    let mut peer_syncs: HashMap<PeerId, u64> = HashMap::new();
    while let Some(event) = proxy_state_event_receiver.next().await {
        if let PeerSynced {
            peer_id,
            contiguous_length,
            ..
        } = event.content
        {
            peer_syncs.insert(peer_id, contiguous_length);
            // TODO: Break when there is the right number of these
        }
    }
    Ok(())
}

async fn process_two_documents_state_events(
    mut state_event_receiver: UnboundedReceiver<StateEvent>,
) -> anyhow::Result<()> {
    while let Some(event) = state_event_receiver.next().await {
        match event.content {
            PeerSynced { .. } => {
                // TODO
            }
            RemotePeerSynced { .. } => {
                // TODO
            }
            DocumentInitialized { new_document, .. } => {
                println!("### 2 DOCS, {new_document}");
            }
            _ => {}
        }
    }
    Ok(())
}

async fn process_three_documents_state_events(
    mut state_event_receiver: UnboundedReceiver<StateEvent>,
) -> anyhow::Result<()> {
    while let Some(event) = state_event_receiver.next().await {
        match event.content {
            PeerSynced { .. } => {
                // TODO
            }
            RemotePeerSynced { .. } => {
                // TODO
            }
            DocumentInitialized { new_document, .. } => {
                println!("### 3 DOCS, {new_document}");
            }
            _ => {}
        }
    }
    Ok(())
}
