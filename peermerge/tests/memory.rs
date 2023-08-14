use automerge::transaction::Transactable;
use automerge::{ObjId, ROOT};
use futures::channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use futures::stream::StreamExt;
use peermerge::{
    DocumentId, FeedMemoryPersistence, NameDescription, Patch, PeerId, Peermerge, StateEvent,
    StateEventContent::*,
};
use random_access_memory::RandomAccessMemory;
use std::collections::HashMap;
use std::sync::Arc;
use test_log::test;
use tracing::{info, instrument};

#[cfg(feature = "async-std")]
use async_std::{sync::Mutex, task, test as async_test};
#[cfg(feature = "tokio")]
use tokio::{sync::Mutex, task, test as async_test};

pub mod common;
use common::*;

#[derive(Clone, Debug, Default)]
struct MemoryThreeWritersResult {
    joiner_merge: Option<String>,
    creator_merge: Option<String>,
}
impl MemoryThreeWritersResult {
    pub fn merge_equals(&self) -> bool {
        self.joiner_merge.as_ref().unwrap() == self.creator_merge.as_ref().unwrap()
    }
}

#[test(async_test)]
async fn memory_three_writers() -> anyhow::Result<()> {
    let (proto_responder, proto_initiator) = create_pair_memory().await;

    let (creator_state_event_sender, creator_state_event_receiver): (
        UnboundedSender<StateEvent>,
        UnboundedReceiver<StateEvent>,
    ) = unbounded();
    let (joiner_state_event_sender, joiner_state_event_receiver): (
        UnboundedSender<StateEvent>,
        UnboundedReceiver<StateEvent>,
    ) = unbounded();
    let mut peermerge_creator = Peermerge::new_memory(
        NameDescription::new("creator"),
        Some(creator_state_event_sender),
    )
    .await;
    let (creator_doc_info, _) = peermerge_creator
        .create_new_document_memory(
            "test",
            Some(NameDescription::new("memory_test")),
            false,
            |tx| tx.put(ROOT, "version", 1),
        )
        .await?;
    assert_eq!(
        peermerge_creator
            .peer_ids(&creator_doc_info.id())
            .await
            .len(),
        1
    );

    // Insert a map with a text field to the document
    let texts_id = peermerge_creator
        .transact_mut(
            &creator_doc_info.id(),
            |doc| doc.put_object(ROOT, "texts", automerge::ObjType::Map),
            None,
        )
        .await
        .unwrap();
    let text_id = peermerge_creator
        .transact_mut(
            &creator_doc_info.id(),
            |doc| doc.put_object(&texts_id, "text", automerge::ObjType::Text),
            None,
        )
        .await
        .unwrap();

    // Set watching for the prop
    peermerge_creator
        .watch(
            &creator_doc_info.id(),
            Some(vec![texts_id.clone(), text_id.clone()]),
        )
        .await;

    let peermerge_creator_for_task = peermerge_creator.clone();
    task::spawn(async move {
        connect(peermerge_creator_for_task, proto_responder)
            .await
            .unwrap();
    });

    let mut peermerge_joiner = Peermerge::new_memory(
        NameDescription::new("joiner"),
        Some(joiner_state_event_sender),
    )
    .await;
    let joiner_doc_info = peermerge_joiner
        .attach_writer_document_memory(
            &peermerge_creator
                .sharing_info(&creator_doc_info.id())
                .await?
                .doc_url
                .unwrap(),
            &None,
        )
        .await?;
    peermerge_joiner
        .watch(&creator_doc_info.id(), Some(vec![]))
        .await;
    let peermerge_joiner_for_task = peermerge_joiner.clone();
    task::spawn(async move {
        connect(peermerge_joiner_for_task, proto_initiator)
            .await
            .unwrap();
    });

    let edit_sync_creator = init_condvar();
    let edit_sync_joiner = Arc::clone(&edit_sync_creator);
    let merge_result_for_creator = Arc::new(Mutex::new(MemoryThreeWritersResult::default()));
    let merge_result_for_joiner = merge_result_for_creator.clone();
    let append_sync_creator = init_condvar();
    let append_sync_joiner = Arc::clone(&append_sync_creator);
    task::spawn(async move {
        process_joiner_state_event(
            peermerge_joiner,
            joiner_doc_info.id(),
            joiner_state_event_receiver,
            edit_sync_joiner,
            merge_result_for_joiner,
            append_sync_joiner,
        )
        .await
        .unwrap();
    });

    process_creator_state_events(
        peermerge_creator,
        creator_doc_info.id(),
        creator_state_event_receiver,
        text_id,
        edit_sync_creator,
        merge_result_for_creator,
        append_sync_creator,
    )
    .await?;

    Ok(())
}

#[instrument(skip_all)]
async fn process_joiner_state_event(
    mut peermerge: Peermerge<RandomAccessMemory, FeedMemoryPersistence>,
    doc_id: DocumentId,
    mut joiner_state_event_receiver: UnboundedReceiver<StateEvent>,
    edit_sync: BoolCondvar,
    merge_result: Arc<Mutex<MemoryThreeWritersResult>>,
    append_sync: BoolCondvar,
) -> anyhow::Result<()> {
    let mut text_id: Option<ObjId> = None;
    let mut document_changes: Vec<Vec<Patch>> = vec![];
    let mut peer_synced: HashMap<String, u64> = HashMap::new();
    let mut remote_peer_synced: HashMap<PeerId, u64> = HashMap::new();
    while let Some(event) = joiner_state_event_receiver.next().await {
        info!(
            "Received event {:?}, document_changes={}",
            event,
            document_changes.len()
        );

        match event.content {
            PeerSynced {
                peer_id,
                contiguous_length,
                ..
            } => {
                let name = peermerge
                    .peer_header(&event.document_id, &peer_id)
                    .await
                    .unwrap()
                    .name;
                if !peer_synced.contains_key("creator") {
                    assert_eq!(name, "creator");
                    let local_texts_id = peermerge
                        .transact(&doc_id, |doc| get_id(doc, ROOT, "texts"))
                        .await?
                        .unwrap();
                    let local_text_id = peermerge
                        .transact(&doc_id, |doc| get_id(doc, &local_texts_id, "text"))
                        .await?
                        .unwrap();
                    text_id = Some(local_text_id.clone());
                    peermerge
                        .watch(
                            &doc_id,
                            Some(vec![local_texts_id, text_id.clone().unwrap()]),
                        )
                        .await;

                    // It's possible that this already contains "Hello" and the first DocumentChanged
                    // event will never come.
                    if assert_text_equals_either(&peermerge, &doc_id, &local_text_id, "", "Hello")
                        .await
                        == "Hello"
                    {
                        document_changes.push(vec![]);
                        peermerge
                            .transact_mut(
                                &doc_id,
                                |doc| doc.splice_text(&text_id.clone().unwrap(), 5, 0, ", world!"),
                                None,
                            )
                            .await?;
                    }
                } else {
                    assert!(name == "creator" || name == "latecomer");
                }
                peer_synced.insert(name.clone(), contiguous_length);
            }
            RemotePeerSynced {
                peer_id,
                contiguous_length,
                ..
            } => {
                remote_peer_synced.insert(peer_id, contiguous_length);
            }
            DocumentInitialized { .. } => {
                // Skip
            }
            DocumentChanged { patches, .. } => {
                if document_changes.is_empty() {
                    assert_eq!(patches.len(), 1); // "Hello" in one TextValue
                    document_changes.push(patches);
                    let text_id = text_id.clone().unwrap();
                    assert_text_equals(&peermerge, &doc_id, &text_id, "Hello").await;
                    peermerge
                        .transact_mut(
                            &doc_id,
                            |doc| doc.splice_text(&text_id, 5, 0, ", world!"),
                            None,
                        )
                        .await?;
                } else if document_changes.len() == 1 {
                    assert_eq!(patches.len(), 1); // ", world!" in one Splice patch
                    document_changes.push(patches);
                    let text_id = text_id.clone().unwrap();
                    assert_text_equals(&peermerge, &doc_id, &text_id, "Hello, world!").await;

                    // Let's make sure via variable that the other end is also ready to edit
                    wait_for_condvar(edit_sync.clone()).await;

                    // Ok, ready to edit in unison, reserve field
                    peermerge.reserve_object(&doc_id, &text_id).await?;
                    peermerge
                        .transact_mut(&doc_id, |doc| doc.splice_text(&text_id, 5, 2, ""), None)
                        .await?;
                    peermerge
                        .transact_mut(&doc_id, |doc| doc.splice_text(&text_id, 4, 0, "XX"), None)
                        .await?;
                    assert_text_equals(&peermerge, &doc_id, &text_id, "HellXXoworld!").await;
                } else if document_changes.len() == 2 {
                    // This is the two local deletions as one Splice message
                    assert_eq!(patches.len(), 1);
                    document_changes.push(patches);
                } else if document_changes.len() == 3 {
                    // This is the two local additions as one Splice patch
                    assert_eq!(patches.len(), 1);
                    document_changes.push(patches);
                    // Unreserve to get the peers' changes processed
                    let text_id = text_id.clone().unwrap();
                    peermerge.unreserve_object(&doc_id, &text_id).await?;
                } else if document_changes.len() == 4 {
                    assert_eq!(patches.len(), 2); // One overlapping delete, and YY as TextValue
                    document_changes.push(patches);
                    let text_id = text_id.clone().unwrap();
                    {
                        let mut merge_result = merge_result.lock().await;
                        merge_result.joiner_merge = Some(
                            assert_text_equals_either(
                                &peermerge,
                                &doc_id,
                                &text_id,
                                "HellXXYYworld!",
                                "HellYYXXworld!",
                            )
                            .await,
                        );
                        if merge_result.creator_merge.is_some() {
                            assert!(merge_result.merge_equals());
                        }
                    }
                } else if document_changes.len() == 5 {
                    assert_eq!(patches.len(), 1); // ZZ latecomer additions as one TextValue
                    document_changes.push(patches);
                    let text_id = text_id.clone().unwrap();
                    assert_text_equals_either(
                        &peermerge,
                        &doc_id,
                        &text_id,
                        "HellXXYYworldZZ!",
                        "HellYYXXworldZZ!",
                    )
                    .await;

                    // Notify about append to both
                    notify_all_condvar(append_sync).await;
                    break;
                } else {
                    panic!("Did not expect more joiner document changes");
                }
            }
            _ => {
                panic!("Unkown event {event:?}");
            }
        }
    }
    Ok(())
}

#[instrument(skip_all)]
async fn process_creator_state_events(
    mut peermerge: Peermerge<RandomAccessMemory, FeedMemoryPersistence>,
    doc_id: DocumentId,
    mut creator_state_event_receiver: UnboundedReceiver<StateEvent>,
    text_id: ObjId,
    edit_sync: BoolCondvar,
    merge_result: Arc<Mutex<MemoryThreeWritersResult>>,
    append_sync: BoolCondvar,
) -> anyhow::Result<()> {
    let mut document_changes: Vec<Vec<Patch>> = vec![];
    let mut latecomer_attached = false;
    let mut peer_synced: HashMap<String, u64> = HashMap::new();
    let mut remote_peer_synced: HashMap<PeerId, u64> = HashMap::new();

    while let Some(event) = creator_state_event_receiver.next().await {
        info!(
            "Received event {:?}, document_changes={}",
            event,
            document_changes.len()
        );

        let text_id = text_id.clone();
        match event.content {
            PeerSynced {
                peer_id,
                contiguous_length,
                ..
            } => {
                let name = peermerge
                    .peer_header(&event.document_id, &peer_id)
                    .await
                    .unwrap()
                    .name;
                peer_synced.insert(name.clone(), contiguous_length);
                if latecomer_attached {
                    assert!(name == "joiner" || name == "latecomer");
                } else {
                    assert_eq!(name, "joiner");
                }
            }
            RemotePeerSynced {
                peer_id,
                contiguous_length,
                ..
            } => {
                if remote_peer_synced.is_empty() {
                    peermerge
                        .transact_mut(
                            &doc_id,
                            |doc| doc.splice_text(&text_id, 0, 0, "Hello"),
                            None,
                        )
                        .await
                        .unwrap();
                    assert_text_equals(&peermerge, &doc_id, &text_id, "Hello").await;
                }
                remote_peer_synced.insert(peer_id, contiguous_length);
            }
            DocumentChanged { patches, .. } => {
                let document_changes_len = document_changes.len();
                match document_changes_len {
                    0 => {
                        assert_eq!(patches.len(), 2); // Original creation of "version" and "texts";
                        document_changes.push(patches);
                    }
                    1 => {
                        assert_eq!(patches.len(), 1); // Original creation of "text";
                        document_changes.push(patches);
                    }
                    2 => {
                        assert_eq!(patches.len(), 1); // "Hello" in one Splice patch
                        document_changes.push(patches);
                    }
                    3 => {
                        assert_eq!(patches.len(), 1); // ", world!" in one TextValue
                        document_changes.push(patches);
                        assert_text_equals(&peermerge, &doc_id, &text_id, "Hello, world!").await;

                        // Ready to notify about edit
                        notify_one_condvar(edit_sync.clone()).await;

                        // Ok, ready to edit, reserve the text_id to prevent the other peer from
                        // affecting our document too soon
                        peermerge.reserve_object(&doc_id, &text_id).await?;

                        // Let's create a conflict here, reserve prevent getting changes from the
                        // other peer before unreserve
                        peermerge
                            .transact_mut(&doc_id, |doc| doc.splice_text(&text_id, 4, 2, ""), None)
                            .await
                            .unwrap();
                        peermerge
                            .transact_mut(
                                &doc_id,
                                |doc| doc.splice_text(&text_id, 4, 0, "YY"),
                                None,
                            )
                            .await
                            .unwrap();
                        assert_text_equals(&peermerge, &doc_id, &text_id, "HellYY world!").await;
                    }
                    4 => {
                        // This is the local deletions, one Delete patch with num 2
                        assert_eq!(patches.len(), 1);
                        document_changes.push(patches);
                    }
                    5 => {
                        // This is the two local additions as one Splice message
                        assert_eq!(patches.len(), 1);
                        document_changes.push(patches);
                        // Unreserve to process the changes from the peer now
                        peermerge.unreserve_object(&doc_id, &text_id).await?;
                    }
                    6 => {
                        assert_eq!(patches.len(), 2); // One deletion that wasn't joined and two X chars as one TextValue
                        document_changes.push(patches);
                        {
                            let mut merge_result = merge_result.lock().await;
                            merge_result.creator_merge = Some(
                                assert_text_equals_either(
                                    &peermerge,
                                    &doc_id,
                                    &text_id,
                                    "HellXXYYworld!",
                                    "HellYYXXworld!",
                                )
                                .await,
                            );
                            if merge_result.joiner_merge.is_some() {
                                assert!(merge_result.merge_equals());
                            }
                        }
                        // Now let's join in a latecomer peer to the creator peer
                        latecomer_attached = true;
                        let (proto_responder, proto_initiator) = create_pair_memory().await;
                        let (latecomer_state_event_sender, latecomer_state_event_receiver): (
                            UnboundedSender<StateEvent>,
                            UnboundedReceiver<StateEvent>,
                        ) = unbounded();
                        let mut peermerge_latecomer = Peermerge::new_memory(
                            NameDescription::new("latecomer"),
                            Some(latecomer_state_event_sender),
                        )
                        .await;
                        let latecomer_doc_info = peermerge_latecomer
                            .attach_writer_document_memory(
                                &peermerge.sharing_info(&doc_id).await?.doc_url.unwrap(),
                                &None,
                            )
                            .await?;
                        peermerge_latecomer.watch(&doc_id, Some(vec![])).await;
                        let peermerge_latecomer_for_task = peermerge_latecomer.clone();
                        let peermerge_creator_for_task = peermerge.clone();
                        task::spawn(async move {
                            connect(peermerge_creator_for_task, proto_responder)
                                .await
                                .unwrap();
                        });
                        task::spawn(async move {
                            connect(peermerge_latecomer_for_task, proto_initiator)
                                .await
                                .unwrap();
                        });
                        let append_sync_latecomer = Arc::clone(&append_sync);
                        task::spawn(async move {
                            process_latecomer_state_event(
                                peermerge_latecomer,
                                latecomer_doc_info.id(),
                                latecomer_state_event_receiver,
                                append_sync_latecomer,
                            )
                            .await
                            .unwrap();
                        });
                    }
                    7 => {
                        assert_eq!(patches.len(), 1); // ZZ latecomer addition as one TextValue
                        assert_text_equals_either(
                            &peermerge,
                            &doc_id,
                            &text_id,
                            "HellXXYYworldZZ!",
                            "HellYYXXworldZZ!",
                        )
                        .await;

                        // Let's wait for the sync to end up to the joiner
                        wait_for_condvar(append_sync).await;
                        break;
                    }
                    _ => {
                        panic!("Did not expect more creator document changes");
                    }
                };
            }
            DocumentInitialized { .. } => {
                // Skip
            }
            _ => {
                panic!("Unkown event {event:?}");
            }
        }
    }
    Ok(())
}

#[instrument(skip_all)]
async fn process_latecomer_state_event(
    mut peermerge: Peermerge<RandomAccessMemory, FeedMemoryPersistence>,
    doc_id: DocumentId,
    mut latecomer_state_event_receiver: UnboundedReceiver<StateEvent>,
    append_sync: BoolCondvar,
) -> anyhow::Result<()> {
    let mut text_id: Option<ObjId> = None;
    let mut document_changes: Vec<Vec<Patch>> = vec![];
    let mut peer_synced: HashMap<String, u64> = HashMap::new();
    let mut remote_peer_synced: HashMap<PeerId, u64> = HashMap::new();
    while let Some(event) = latecomer_state_event_receiver.next().await {
        info!(
            "Received event {:?}, document_changes={}",
            event,
            document_changes.len()
        );
        match event.content {
            PeerSynced {
                peer_id,
                contiguous_length,
                ..
            } => {
                let name = peermerge
                    .peer_header(&event.document_id, &peer_id)
                    .await
                    .unwrap()
                    .name;
                assert!(name == "creator" || name == "joiner");
                peer_synced.insert(name.clone(), contiguous_length);
                if peer_synced.contains_key("creator")
                    && peer_synced.contains_key("joiner")
                    && text_id.is_none()
                {
                    let local_texts_id = peermerge
                        .transact(&doc_id, |doc| get_id(doc, ROOT, "texts"))
                        .await?
                        .unwrap();
                    let local_text_id = peermerge
                        .transact(&doc_id, |doc| get_id(doc, &local_texts_id, "text"))
                        .await?
                        .unwrap();
                    assert_text_equals_either(
                        &peermerge,
                        &doc_id,
                        &local_text_id,
                        "HellXXYYworld!",
                        "HellYYXXworld!",
                    )
                    .await;
                    text_id = Some(local_text_id.clone());
                    peermerge
                        .watch(
                            &doc_id,
                            Some(vec![local_texts_id, text_id.clone().unwrap()]),
                        )
                        .await;
                    // Make one final change and see that it propagates through to the creator
                    peermerge
                        .transact_mut(
                            &doc_id,
                            |doc| doc.splice_text(&local_text_id, 13, 0, "ZZ"),
                            None,
                        )
                        .await?;
                }
            }
            RemotePeerSynced {
                peer_id,
                contiguous_length,
                ..
            } => {
                remote_peer_synced.insert(peer_id, contiguous_length);
            }
            DocumentInitialized { .. } => {
                // Ignore, this happens with the root hypercore
            }
            DocumentChanged { patches, .. } => {
                if document_changes.is_empty() {
                    assert_eq!(patches.len(), 1); // Two local additions as one Splice
                    assert_text_equals_either(
                        &peermerge,
                        &doc_id,
                        &text_id.clone().unwrap(),
                        "HellXXYYworldZZ!",
                        "HellYYXXworldZZ!",
                    )
                    .await;
                    document_changes.push(patches);
                }

                // Let's wait for this to end up, via the creator, to the joiner
                wait_for_condvar(append_sync).await;
                break;
            }
            _ => {
                panic!("Unkown event {event:?}");
            }
        }
    }

    Ok(())
}

async fn connect(
    mut peermerge: Peermerge<RandomAccessMemory, FeedMemoryPersistence>,
    mut protocol: MemoryProtocol,
) -> anyhow::Result<()> {
    peermerge.connect_protocol_memory(&mut protocol).await?;
    Ok(())
}

async fn assert_text_equals(
    peermerge: &Peermerge<RandomAccessMemory, FeedMemoryPersistence>,
    doc_id: &DocumentId,
    obj: &ObjId,
    expected: &str,
) {
    let result = peermerge
        .transact(doc_id, |doc| realize_text(doc, obj))
        .await;
    assert_eq!(result.unwrap().unwrap(), expected);
}

async fn assert_text_equals_either(
    peermerge: &Peermerge<RandomAccessMemory, FeedMemoryPersistence>,
    doc_id: &DocumentId,
    obj: &ObjId,
    expected_1: &str,
    expected_2: &str,
) -> String {
    let result = peermerge
        .transact(doc_id, |doc| realize_text(doc, obj))
        .await
        .unwrap()
        .unwrap();
    return if result == expected_1 {
        expected_1.to_string()
    } else if result == expected_2 {
        expected_2.to_string()
    } else {
        panic!("Text {result} did not match either {expected_1} or {expected_2}",);
    };
}
