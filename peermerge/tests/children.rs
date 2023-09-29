use futures::{
    channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender},
    future::join_all,
    StreamExt,
};
use peermerge::{
    automerge::{transaction::Transactable, ScalarValue, ROOT},
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

#[cfg(feature = "async-std")]
use async_std::{sync::Mutex, task, test as async_test};
#[cfg(feature = "tokio")]
use tokio::{task, test as async_test};

pub mod common;
use common::*;

const MEMORY_MAX_WRITE_FEED_LENGTH: u64 = 16;
const DISK_MAX_WRITE_FEED_LENGTH: u64 = 8;

/// Test three main documents, each with two peers, and two shared documents,
/// one between all of them, an one between the first and second. All mediated
/// by a proxy.
#[test(async_test)]
async fn children_three_main_documents_two_shared() -> anyhow::Result<()> {
    // Document 1: memory peers

    let mut peermerge_creator_1 = Peermerge::new_memory(
        PeermergeMemoryOptionsBuilder::default()
            .default_peer_header(NameDescription::new("creator_1"))
            .max_write_feed_length(MEMORY_MAX_WRITE_FEED_LENGTH)
            .build()?,
    )
    .await?;
    let mut peermerge_joiner_1 = Peermerge::new_memory(
        PeermergeMemoryOptionsBuilder::default()
            .default_peer_header(NameDescription::new("joiner_1"))
            .max_write_feed_length(MEMORY_MAX_WRITE_FEED_LENGTH)
            .build()?,
    )
    .await?;
    let creator_1_state_event_receiver =
        create_state_events_channel(&mut peermerge_creator_1).await?;
    let joiner_1_state_event_receiver =
        create_state_events_channel(&mut peermerge_joiner_1).await?;
    let (document_1_id, document_1_proxy_doc_url) = create_main_document_memory(
        "document_1",
        "value_1",
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
            .max_write_feed_length(DISK_MAX_WRITE_FEED_LENGTH)
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
            .max_write_feed_length(DISK_MAX_WRITE_FEED_LENGTH)
            .build()?,
    )
    .await?;
    let creator_2_state_event_receiver =
        create_state_events_channel(&mut peermerge_creator_2).await?;
    let joiner_2_state_event_receiver =
        create_state_events_channel(&mut peermerge_joiner_2).await?;
    let (document_2_id, document_2_proxy_doc_url) = create_main_document_disk(
        "document_2",
        "value_2",
        &mut peermerge_creator_2,
        &mut peermerge_joiner_2,
    )
    .await?;

    // Document 3: memory peers
    // NB: The creator has small max entry size, big meta and user doc values, to test
    // splitting of values.
    let mut peermerge_creator_3 = Peermerge::new_memory(
        PeermergeMemoryOptionsBuilder::default()
            .default_peer_header(NameDescription::new_with_description(
                "creator_3",
                &generate_string(1, 1500),
            ))
            .max_entry_data_size_bytes(1024)
            .max_write_feed_length(MEMORY_MAX_WRITE_FEED_LENGTH)
            .build()?,
    )
    .await?;

    let mut peermerge_joiner_3 = Peermerge::new_memory(
        PeermergeMemoryOptionsBuilder::default()
            .default_peer_header(NameDescription::new("joiner_3"))
            .max_write_feed_length(MEMORY_MAX_WRITE_FEED_LENGTH)
            .build()?,
    )
    .await?;
    let creator_3_state_event_receiver =
        create_state_events_channel(&mut peermerge_creator_3).await?;
    let joiner_3_state_event_receiver =
        create_state_events_channel(&mut peermerge_joiner_3).await?;
    let (document_3_id, document_3_proxy_doc_url) = create_main_document_memory(
        &("document_3_".to_string() + &generate_string(2, 1500)),
        &generate_string(3, 1500),
        &mut peermerge_creator_3,
        &mut peermerge_joiner_3,
    )
    .await?;

    // Create an assortment of changes, big and small, to verify that changes split into many pieces
    // are recreated later in the joiner.
    peermerge_creator_3
        .transact_mut(&document_3_id, |doc| doc.put(ROOT, "test", "initial"), None)
        .await?;
    peermerge_creator_3
        .transact_mut(
            &document_3_id,
            |doc| doc.put(ROOT, "test", generate_string(4, 3500)),
            None,
        )
        .await?;
    let document_3_final_change_id: Vec<u8> = vec![1];
    peermerge_creator_3
        .transact_mut(
            &document_3_id,
            |doc| doc.put(ROOT, "test", "final"),
            Some(document_3_final_change_id.clone()),
        )
        .await?;

    // Use a proxy to sync peers of different documents together, asserting that the right document is
    // found in every peer that should have it. NB: The proxy doesn't need to know about child
    // documents, they are synced automatically.

    let mut peermerge_proxy = Peermerge::new_memory(
        PeermergeMemoryOptionsBuilder::default()
            .default_peer_header(NameDescription::new("proxy"))
            .build()?,
    )
    .await?;
    let proxy_state_event_receiver = create_state_events_channel(&mut peermerge_proxy).await?;

    // Shared document A: between all of the parties

    let (shared_doc_info_a, _) = peermerge_joiner_2
        .create_new_document_disk(
            CreateNewDocumentDiskOptionsBuilder::default()
                .document_type("scale_shared".to_string())
                .document_header(NameDescription::new("a"))
                .parent_id(document_2_id)
                .build()?,
            |tx| tx.put(ROOT, "shared_counter", ScalarValue::Counter(0.into())),
            None,
        )
        .await?;
    let shared_doc_id_a = shared_doc_info_a.id();
    assert!(shared_doc_info_a.static_info.child);
    let document_secret_a = peermerge_joiner_2
        .document_secret(&shared_doc_id_a)
        .await?
        .unwrap();
    let sharing_info_a = peermerge_joiner_2.sharing_info(&shared_doc_id_a).await?;
    let shared_doc_info_attach_a = peermerge_creator_1
        .attach_document_memory(
            AttachDocumentMemoryOptionsBuilder::default()
                .document_url(sharing_info_a.read_write_doc_url.clone())
                .document_secret(document_secret_a.clone())
                .parent_id(document_1_id)
                .parent_header(NameDescription::new("document_1_override"))
                .build()?,
        )
        .await?;
    assert!(shared_doc_info_attach_a.static_info.child);
    let shared_doc_info_attach_a = peermerge_joiner_3
        .attach_document_memory(
            AttachDocumentMemoryOptionsBuilder::default()
                .document_url(sharing_info_a.read_write_doc_url)
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

    let three_doc_receivers = wait_for_peers_synced(
        vec![
            (
                peermerge_creator_1.clone(),
                peermerge_joiner_1.peer_id(),
                creator_1_state_event_receiver,
                document_1_id,
                ("main".to_string(), "value_1".to_string(), None),
                None,
            ),
            (
                peermerge_creator_3.clone(),
                peermerge_joiner_3.peer_id(),
                creator_3_state_event_receiver,
                document_3_id,
                (
                    "test".to_string(),
                    "final".to_string(),
                    Some(document_3_final_change_id),
                ),
                None,
            ),
        ],
        vec![
            (
                peermerge_joiner_1.clone(),
                peermerge_creator_1.peer_id(),
                joiner_1_state_event_receiver,
                document_1_id,
                ("main".to_string(), "value_1".to_string(), None),
                None,
            ),
            (
                peermerge_joiner_3.clone(),
                peermerge_creator_3.peer_id(),
                joiner_3_state_event_receiver,
                document_3_id,
                ("test".to_string(), "final".to_string(), None),
                None,
            ),
        ],
    )
    .await?;

    let two_doc_receivers = wait_for_peers_synced(
        vec![(
            peermerge_creator_2.clone(),
            peermerge_joiner_2.peer_id(),
            creator_2_state_event_receiver,
            document_2_id,
            ("main".to_string(), "value_2".to_string(), None),
            Some(shared_doc_id_a),
        )],
        vec![(
            peermerge_joiner_2.clone(),
            peermerge_creator_2.peer_id(),
            joiner_2_state_event_receiver,
            document_2_id,
            ("main".to_string(), "value_2".to_string(), None),
            Some(shared_doc_id_a),
        )],
    )
    .await?;

    // Shared document B: between first and third peer

    let shared_doc_b_initial_change_id: Vec<u8> = vec![2];
    let (shared_doc_info_b, _) = peermerge_creator_3
        .create_new_document_memory(
            CreateNewDocumentMemoryOptionsBuilder::default()
                .document_type("scale_shared".to_string())
                .document_header(NameDescription::new("b"))
                .parent_id(document_3_id)
                .build()?,
            |tx| tx.put(ROOT, "shared_counter", ScalarValue::Counter(0.into())),
            Some(shared_doc_b_initial_change_id.clone()),
        )
        .await?;
    let shared_doc_id_b = shared_doc_info_b.id();
    assert!(shared_doc_info_b.static_info.child);
    let document_secret_b = peermerge_creator_3
        .document_secret(&shared_doc_id_b)
        .await?
        .unwrap();
    let sharing_info_b = peermerge_creator_3.sharing_info(&shared_doc_id_b).await?;
    let shared_doc_info_attach_b = peermerge_creator_1
        .attach_document_memory(
            AttachDocumentMemoryOptionsBuilder::default()
                .document_url(sharing_info_b.read_write_doc_url)
                .document_secret(document_secret_b)
                .parent_id(document_1_id)
                .parent_header(NameDescription::new("document_1_override"))
                .build()?,
        )
        .await?;
    assert!(shared_doc_info_attach_b.static_info.child);

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

    // Only one of the peers per document is connected, the mutual sync between the peers
    // from before will sync to the other peer not connected to the proxy.
    connect_peers_memory(&mut peermerge_proxy, &mut peermerge_creator_1).await?;
    connect_peers_disk_to_memory(&mut peermerge_proxy, &mut peermerge_creator_2).await?;
    connect_peers_memory(&mut peermerge_proxy, &mut peermerge_joiner_3).await?;

    wait_for_proxy_mediated_increments(
        proxy_state_event_receiver,
        shared_doc_id_b,
        shared_doc_b_initial_change_id,
        two_doc_receivers,
        shared_doc_id_a,
        three_doc_receivers,
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
    document_main_value: &str,
    peermerge_creator: &mut Peermerge<RandomAccessMemory, FeedMemoryPersistence>,
    peermerge_joiner: &mut Peermerge<RandomAccessMemory, FeedMemoryPersistence>,
) -> anyhow::Result<(DocumentId, String)> {
    let (creator_doc_info, _) = peermerge_creator
        .create_new_document_memory(
            CreateNewDocumentMemoryOptionsBuilder::default()
                .document_type("scale".to_string())
                .document_header(NameDescription::new(document_name))
                .build()?,
            |tx| tx.put(ROOT, "main", document_main_value),
            None,
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
                .document_url(sharing_info.read_write_doc_url)
                .document_secret(document_secret)
                .build()?,
        )
        .await?;
    Ok((document_id, sharing_info.proxy_doc_url))
}

async fn create_main_document_disk(
    document_name: &str,
    document_main_value: &str,
    peermerge_creator: &mut Peermerge<RandomAccessDisk, FeedDiskPersistence>,
    peermerge_joiner: &mut Peermerge<RandomAccessDisk, FeedDiskPersistence>,
) -> anyhow::Result<(DocumentId, String)> {
    let (creator_doc_info, _) = peermerge_creator
        .create_new_document_disk(
            CreateNewDocumentDiskOptionsBuilder::default()
                .document_type("scale".to_string())
                .document_header(NameDescription::new(document_name))
                .build()?,
            |tx| {
                tx.put(ROOT, "counter", ScalarValue::Counter(0.into()))?;
                tx.put(ROOT, "main", document_main_value)
            },
            None,
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
                .document_url(sharing_info.read_write_doc_url)
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

async fn wait_for_peers_synced<T, U>(
    creators: Vec<(
        Peermerge<T, U>,
        PeerId,
        UnboundedReceiver<StateEvent>,
        DocumentId,
        (String, String, Option<Vec<u8>>),
        Option<DocumentId>,
    )>,
    joiners: Vec<(
        Peermerge<T, U>,
        PeerId,
        UnboundedReceiver<StateEvent>,
        DocumentId,
        (String, String, Option<Vec<u8>>),
        Option<DocumentId>,
    )>,
) -> anyhow::Result<Vec<(Peermerge<T, U>, DocumentId, UnboundedReceiver<StateEvent>)>>
where
    T: RandomAccess + Debug + Send + 'static,
    U: FeedPersistence,
{
    let mut processes = vec![];
    for (
        creator,
        joiner_peer_id,
        creator_state_event_receiver,
        doc_id,
        (expected_key, expected_value, expected_change_id),
        child_doc_id,
    ) in creators
    {
        processes.push(task::spawn(async move {
            process_until_synced(
                creator,
                creator_state_event_receiver,
                joiner_peer_id,
                doc_id,
                expected_key,
                expected_value,
                expected_change_id,
                child_doc_id,
            )
            .await
        }));
    }

    for (
        joiner,
        creator_peer_id,
        joiner_state_event_receiver,
        doc_id,
        (expected_key, expected_value, expected_change_id),
        child_doc_id,
    ) in joiners
    {
        processes.push(task::spawn(async move {
            process_until_synced(
                joiner,
                joiner_state_event_receiver,
                creator_peer_id,
                doc_id,
                expected_key,
                expected_value,
                expected_change_id,
                child_doc_id,
            )
            .await
        }));
    }

    let receiver_results = join_all(processes).await;
    let receivers = receiver_results
        .into_iter()
        .map(|result| result.unwrap())
        .collect();
    Ok(receivers)
}

async fn process_until_synced<T, U>(
    mut peermerge: Peermerge<T, U>,
    mut state_event_receiver: UnboundedReceiver<StateEvent>,
    remote_peer_id: PeerId,
    document_id: DocumentId,
    expected_key: String,
    expected_value: String,
    expected_change_id: Option<Vec<u8>>,
    child_document_id: Option<DocumentId>,
) -> (Peermerge<T, U>, DocumentId, UnboundedReceiver<StateEvent>)
where
    T: RandomAccess + Debug + Send + 'static,
    U: FeedPersistence,
{
    let mut peer_synced: bool = false;
    let mut remote_peer_synced: bool = false;
    let mut documents_synced: usize = 0;
    let mut expected_value_match: bool = false;
    let mut expected_change_found: bool =
        expected_change_id.as_ref().map(|_| false).unwrap_or(true);
    let local_peer_id = peermerge.peer_id();
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
                if (child
                    && child_document_id == Some(event.document_id)
                    && parent_document_ids == vec![document_id])
                    || (!child
                        && document_id == event.document_id
                        && parent_document_ids.is_empty())
                {
                    documents_synced += 1;
                } else {
                    panic!("Unexpected DocumentInitialized");
                }

                if event.document_id == document_id {
                    let value = peermerge
                        .transact(&document_id, |doc| {
                            if let Some((value, _)) = get(doc, ROOT, &expected_key)? {
                                Ok(value.into_scalar().unwrap().to_str().unwrap().to_string())
                            } else {
                                Ok("".to_string())
                            }
                        })
                        .await
                        .unwrap();
                    if value == expected_value {
                        expected_value_match = true;
                    }
                }
            }
            DocumentChanged { change_id, .. } => {
                if change_id.is_some() && change_id == expected_change_id {
                    expected_change_found = true;
                }
                if event.document_id == document_id {
                    let value = peermerge
                        .transact(&document_id, |doc| {
                            let (value, _) = get(doc, ROOT, &expected_key)?.unwrap();
                            Ok(value.into_scalar().unwrap().to_str().unwrap().to_string())
                        })
                        .await
                        .unwrap();
                    if value == expected_value {
                        expected_value_match = true;
                    }
                }
            }
            _ => {}
        }
        if peer_synced
            && remote_peer_synced
            && documents_synced == (if child_document_id.is_some() { 2 } else { 1 })
            && expected_value_match
            && expected_change_found
        {
            let state_event_receiver = create_state_events_channel(&mut peermerge).await.unwrap();
            return (peermerge, document_id, state_event_receiver);
        }
    }
    unreachable!()
}

async fn wait_for_proxy_mediated_increments(
    proxy_state_event_receiver: UnboundedReceiver<StateEvent>,
    two_shared_doc_id: DocumentId,
    two_shared_doc_initial_change_id: Vec<u8>,
    two_document_state_event_receivers: Vec<(
        Peermerge<RandomAccessDisk, FeedDiskPersistence>,
        DocumentId,
        UnboundedReceiver<StateEvent>,
    )>,
    three_shared_doc_id: DocumentId,
    three_document_state_event_receivers: Vec<(
        Peermerge<RandomAccessMemory, FeedMemoryPersistence>,
        DocumentId,
        UnboundedReceiver<StateEvent>,
    )>,
) -> anyhow::Result<()> {
    // TODO: The proxy now never exists
    let _proxy_process = task::spawn(async move {
        process_proxy_state_events(proxy_state_event_receiver)
            .await
            .unwrap();
    });
    let mut processes = vec![];
    for (peermerge, parent_document_id, state_event_receiver) in two_document_state_event_receivers
    {
        processes.push(task::spawn(async move {
            process_two_documents_state_events(
                peermerge,
                state_event_receiver,
                three_shared_doc_id,
                parent_document_id,
            )
            .await
            .unwrap();
        }));
    }
    for (peermerge, parent_document_id, state_event_receiver) in
        three_document_state_event_receivers.into_iter()
    {
        let two_shared_doc_initial_change_id_for_task = two_shared_doc_initial_change_id.clone();
        processes.push(task::spawn(async move {
            process_three_documents_state_events(
                peermerge,
                state_event_receiver,
                two_shared_doc_id,
                two_shared_doc_initial_change_id_for_task,
                three_shared_doc_id,
                parent_document_id,
            )
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
            // TODO: Break when there is the right number of these,
            // or some other thing happens
        }
    }
    Ok(())
}

async fn process_two_documents_state_events(
    mut peermerge: Peermerge<RandomAccessDisk, FeedDiskPersistence>,
    mut state_event_receiver: UnboundedReceiver<StateEvent>,
    three_shared_doc_id: DocumentId,
    parent_document_id: DocumentId,
) -> anyhow::Result<()> {
    let mut first_change = true;
    let mut all_peers_synced: bool = false;
    let mut peer_change_count: usize = 0;
    let mut increments_received: bool = false;
    while let Some(event) = state_event_receiver.next().await {
        match event.content {
            DocumentInitialized { .. } => {
                panic!("Unexpected DocumentInitialized for two docs")
            }
            DocumentChanged { .. } => {
                if event.document_id == three_shared_doc_id {
                    if first_change {
                        // Increment the doc shared with all three
                        peermerge
                            .transact_mut(
                                &three_shared_doc_id,
                                |doc| doc.increment(ROOT, "shared_counter", 1),
                                None,
                            )
                            .await?;
                        first_change = false;
                    } else {
                        let value = peermerge
                            .transact(&three_shared_doc_id, |doc| {
                                let (value, _) = get(doc, ROOT, "shared_counter")?.unwrap();
                                Ok(value.into_scalar().unwrap().to_u64().unwrap())
                            })
                            .await?;
                        if value == 6 && !all_peers_synced {
                            all_peers_synced = true;
                            // For phase two, start incrementing the parent document
                            // to cause one write feed replace.
                            for _ in 0..DISK_MAX_WRITE_FEED_LENGTH {
                                peermerge
                                    .transact_mut(
                                        &parent_document_id,
                                        |doc| doc.increment(ROOT, "counter", 1),
                                        None,
                                    )
                                    .await?;
                            }
                        }
                    }
                } else if event.document_id == parent_document_id && !increments_received {
                    let value = peermerge
                        .transact(&parent_document_id, |doc| {
                            let (value, _) = get(doc, ROOT, "counter")?.unwrap();
                            Ok(value.into_scalar().unwrap().to_u64().unwrap())
                        })
                        .await?;
                    // Two memory peers, all increment once over the line
                    if value == DISK_MAX_WRITE_FEED_LENGTH * 2 {
                        // Ready with increments
                        increments_received = true;
                    }
                } else {
                    panic!("Wrong document changed {}", event.document_id[0]);
                }
            }
            PeerChanged {
                replaced_discovery_key,
                ..
            } => {
                if event.document_id == parent_document_id {
                    if all_peers_synced {
                        assert!(replaced_discovery_key.is_some());
                    } else {
                        assert!(replaced_discovery_key.is_none());
                    }
                    peer_change_count += 1;
                }
            }
            _ => {}
        }
        if increments_received && peer_change_count == 2 {
            // READY
            break;
        }
    }
    Ok(())
}

async fn process_three_documents_state_events(
    mut peermerge: Peermerge<RandomAccessMemory, FeedMemoryPersistence>,
    mut state_event_receiver: UnboundedReceiver<StateEvent>,
    two_shared_doc_id: DocumentId,
    two_shared_doc_initial_change_id: Vec<u8>,
    three_shared_doc_id: DocumentId,
    parent_document_id: DocumentId,
) -> anyhow::Result<()> {
    let mut two_shared_initialized: bool = false;
    let mut three_shared_initialized: bool = false;
    let mut all_peers_synced: bool = false;
    let mut peer_change_count: usize = 0;
    let mut increments_received: bool = false;
    let peer_id = peermerge.peer_id();
    while let Some(event) = state_event_receiver.next().await {
        match event.content {
            DocumentInitialized {
                child,
                parent_document_ids,
                ..
            } => {
                if child
                    && (!two_shared_initialized
                        && event.document_id == two_shared_doc_id
                        && parent_document_ids == vec![parent_document_id])
                {
                    two_shared_initialized = true;
                } else if child
                    && (!three_shared_initialized
                        && event.document_id == three_shared_doc_id
                        && parent_document_ids == vec![parent_document_id])
                {
                    three_shared_initialized = true;
                } else {
                    panic!("Unexpected DocumentInitialized");
                }
                if two_shared_initialized && three_shared_initialized {
                    // Increment the doc shared with all three
                    peermerge
                        .transact_mut(
                            &three_shared_doc_id,
                            |doc| doc.increment(ROOT, "shared_counter", 1),
                            None,
                        )
                        .await?;
                }
            }
            DocumentChanged { change_id, .. } => {
                if event.document_id == three_shared_doc_id {
                    let value = peermerge
                        .transact(&three_shared_doc_id, |doc| {
                            let (value, _) = get(doc, ROOT, "shared_counter")?.unwrap();
                            Ok(value.into_scalar().unwrap().to_u64().unwrap())
                        })
                        .await?;
                    if value == 6 && !all_peers_synced {
                        all_peers_synced = true;
                        // For phase two, start incrementing the two shared doc enough to cause
                        // two write feed replacements.
                        for _ in 0..MEMORY_MAX_WRITE_FEED_LENGTH * 2 {
                            peermerge
                                .transact_mut(
                                    &two_shared_doc_id,
                                    |doc| doc.increment(ROOT, "shared_counter", 1),
                                    None,
                                )
                                .await?;
                        }
                    }
                } else if event.document_id == two_shared_doc_id && !increments_received {
                    if change_id == Some(two_shared_doc_initial_change_id.clone()) {
                        assert!(!all_peers_synced);
                    } else if all_peers_synced && !increments_received {
                        let value = peermerge
                            .transact(&two_shared_doc_id, |doc| {
                                let (value, _) = get(doc, ROOT, "shared_counter")?.unwrap();
                                Ok(value.into_scalar().unwrap().to_u64().unwrap())
                            })
                            .await?;
                        // Four memory peers, all increment twice over the line
                        if value == MEMORY_MAX_WRITE_FEED_LENGTH * 2 * 4 {
                            // Ready with increments
                            increments_received = true;
                        }
                    } else {
                        panic!(
                            "{} Wrong shared document changed {}",
                            peer_id[0], event.document_id[0]
                        );
                    }
                } else {
                    panic!("Wrong document changed {}", event.document_id[0]);
                }
            }
            PeerChanged {
                replaced_discovery_key,
                ..
            } => {
                if event.document_id == two_shared_doc_id {
                    if all_peers_synced {
                        assert!(replaced_discovery_key.is_some());
                    }
                    peer_change_count += 1;
                }
            }
            _ => {}
        }

        if increments_received && peer_change_count == 4 * 2 {
            // READY
            break;
        }
    }

    Ok(())
}
