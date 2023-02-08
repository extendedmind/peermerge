use automerge::{ObjId, ROOT};
use futures::channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use futures::stream::StreamExt;
use peermerge::{
    DocumentId, FeedMemoryPersistence, Patch, Peermerge, StateEvent, StateEventContent::*,
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

mod common;
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
    let mut peermerge_creator = Peermerge::new_memory("creator").await;
    let creator_doc_id = peermerge_creator
        .create_new_document_memory(vec![("version", 1)], false)
        .await;

    // Insert a map with a text field to the document
    let texts_id = peermerge_creator
        .put_object(&creator_doc_id, ROOT, "texts", automerge::ObjType::Map)
        .await
        .unwrap();
    let text_id = peermerge_creator
        .put_object(&creator_doc_id, &texts_id, "text", automerge::ObjType::Text)
        .await
        .unwrap();

    // Set watching for the prop
    peermerge_creator
        .watch(&creator_doc_id, vec![texts_id.clone(), text_id.clone()])
        .await;

    let peermerge_creator_for_task = peermerge_creator.clone();
    let creator_state_event_sender_for_task = creator_state_event_sender.clone();
    task::spawn(async move {
        connect(
            peermerge_creator_for_task,
            proto_responder,
            creator_state_event_sender_for_task,
        )
        .await
        .unwrap();
    });

    let mut peermerge_joiner = Peermerge::new_memory("joiner").await;
    let joiner_doc_id = peermerge_joiner
        .attach_writer_document_memory(&peermerge_creator.doc_url(&creator_doc_id), &None)
        .await;
    let peermerge_joiner_for_task = peermerge_joiner.clone();
    task::spawn(async move {
        connect(
            peermerge_joiner_for_task,
            proto_initiator,
            joiner_state_event_sender,
        )
        .await
        .unwrap();
    });

    let cork_sync_creator = init_condvar();
    let cork_sync_joiner = Arc::clone(&cork_sync_creator);
    let merge_result_for_creator = Arc::new(Mutex::new(MemoryThreeWritersResult::default()));
    let merge_result_for_joiner = merge_result_for_creator.clone();
    let append_sync_creator = init_condvar();
    let append_sync_joiner = Arc::clone(&append_sync_creator);
    task::spawn(async move {
        process_joiner_state_event(
            peermerge_joiner,
            joiner_doc_id,
            joiner_state_event_receiver,
            cork_sync_joiner,
            merge_result_for_joiner,
            append_sync_joiner,
        )
        .await
        .unwrap();
    });

    process_creator_state_events(
        peermerge_creator,
        creator_doc_id,
        creator_state_event_sender,
        creator_state_event_receiver,
        text_id,
        cork_sync_creator,
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
    cork_sync: BoolCondvar,
    merge_result: Arc<Mutex<MemoryThreeWritersResult>>,
    append_sync: BoolCondvar,
) -> anyhow::Result<()> {
    let mut text_id: Option<ObjId> = None;
    let mut document_changes: Vec<Vec<Patch>> = vec![];
    let mut peer_synced: HashMap<String, u64> = HashMap::new();
    let mut remote_peer_synced: HashMap<[u8; 32], u64> = HashMap::new();
    while let Some(event) = joiner_state_event_receiver.next().await {
        info!(
            "Received event {:?}, document_changes={}",
            event,
            document_changes.len()
        );
        match event.content {
            PeerSynced((Some(name), _, len)) => {
                if !peer_synced.contains_key("creator") {
                    assert_eq!(name, "creator");
                    let local_texts_id = peermerge.get_id(&doc_id, ROOT, "texts").await?.unwrap();
                    let local_text_id = peermerge
                        .get_id(&doc_id, &local_texts_id, "text")
                        .await?
                        .unwrap();
                    assert_text_equals(&peermerge, &doc_id, &local_text_id, "").await;
                    text_id = Some(local_text_id.clone());
                    peermerge
                        .watch(&doc_id, vec![local_texts_id, text_id.clone().unwrap()])
                        .await;
                } else {
                    assert!(name == "creator" || name == "latecomer");
                }
                peer_synced.insert(name.clone(), len);
            }
            RemotePeerSynced((discovery_key, len)) => {
                remote_peer_synced.insert(discovery_key, len);
            }
            DocumentChanged(patches) => {
                if document_changes.len() == 0 {
                    assert_eq!(patches.len(), 5); // "Hello" has 5 chars
                    document_changes.push(patches);
                    let text_id = text_id.clone().unwrap();
                    assert_text_equals(&peermerge, &doc_id, &text_id, "Hello").await;
                    peermerge
                        .splice_text(&doc_id, &text_id, 5, 0, ", world!")
                        .await?;
                } else if document_changes.len() == 1 {
                    assert_eq!(patches.len(), 1); // ", world!" in one Splice patch
                    document_changes.push(patches);
                    let text_id = text_id.clone().unwrap();
                    assert_text_equals(&peermerge, &doc_id, &text_id, "Hello, world!").await;

                    // Let's make sure via variable that the other end is also ready to cork
                    wait_for_condvar(cork_sync.clone()).await;

                    // Ok, ready to cork in unison
                    peermerge.cork(&doc_id).await;
                    peermerge.splice_text(&doc_id, &text_id, 5, 2, "").await?;
                    peermerge.splice_text(&doc_id, &text_id, 4, 0, "XX").await?;
                    assert_text_equals(&peermerge, &doc_id, &text_id, "HellXXoworld!").await;
                } else if document_changes.len() == 2 {
                    // This is the two local deletions as one Splice message
                    assert_eq!(patches.len(), 1);
                    document_changes.push(patches);
                } else if document_changes.len() == 3 {
                    // This is the two local additions as one Splice patch
                    assert_eq!(patches.len(), 1);
                    document_changes.push(patches);
                } else if document_changes.len() == 4 {
                    assert_eq!(patches.len(), 3); // One overlapping delete, and two Y
                    document_changes.push(patches);
                    let text_id = text_id.clone().unwrap();
                    merge_result.lock().await.joiner_merge = Some(
                        assert_text_equals_either(
                            &peermerge,
                            &doc_id,
                            &text_id,
                            "HellXXYYworld!",
                            "HellYYXXworld!",
                        )
                        .await,
                    );
                    // These are the changes sent by the peer, uncork to send the changes to the peer now
                    peermerge.uncork(&doc_id).await.unwrap();
                } else if document_changes.len() == 5 {
                    assert_eq!(patches.len(), 2); // Two latecomer additions
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
                panic!("Unkown event {:?}", event);
            }
        }
    }
    Ok(())
}

#[instrument(skip_all)]
async fn process_creator_state_events(
    mut peermerge: Peermerge<RandomAccessMemory, FeedMemoryPersistence>,
    doc_id: DocumentId,
    creator_state_event_sender: UnboundedSender<StateEvent>,
    mut creator_state_event_receiver: UnboundedReceiver<StateEvent>,
    text_id: ObjId,
    cork_sync: BoolCondvar,
    merge_result: Arc<Mutex<MemoryThreeWritersResult>>,
    append_sync: BoolCondvar,
) -> anyhow::Result<()> {
    let mut document_changes: Vec<Vec<Patch>> = vec![];
    let mut latecomer_attached = false;
    let mut peer_synced: HashMap<String, u64> = HashMap::new();
    let mut remote_peer_synced: HashMap<[u8; 32], u64> = HashMap::new();

    while let Some(event) = creator_state_event_receiver.next().await {
        info!(
            "Received event {:?}, document_changes={}",
            event,
            document_changes.len()
        );
        let text_id = text_id.clone();
        match event.content {
            PeerSynced((Some(name), _, len)) => {
                peer_synced.insert(name.clone(), len);
                if latecomer_attached {
                    assert!(name == "joiner" || name == "latecomer");
                } else {
                    assert_eq!(name, "joiner");
                }
            }
            RemotePeerSynced((discovery_key, len)) => {
                if remote_peer_synced.is_empty() {
                    peermerge
                        .splice_text(&doc_id, &text_id, 0, 0, "Hello")
                        .await
                        .unwrap();
                    assert_text_equals(&peermerge, &doc_id, &text_id, "Hello").await;
                }
                remote_peer_synced.insert(discovery_key, len);
            }
            DocumentChanged(patches) => {
                if document_changes.len() == 0 {
                    assert_eq!(patches.len(), 2); // Original creation of "texts" and "text";
                    document_changes.push(patches);
                } else if document_changes.len() == 1 {
                    assert_eq!(patches.len(), 1); // "Hello" in one Splice patch
                    document_changes.push(patches);
                } else if document_changes.len() == 2 {
                    assert_eq!(patches.len(), 8); // ", world!" has 8 chars
                    document_changes.push(patches);
                    assert_text_equals(&peermerge, &doc_id, &text_id, "Hello, world!").await;

                    // Ready to notify about cork
                    notify_one_condvar(cork_sync.clone()).await;

                    // Ok, ready to cork
                    peermerge.cork(&doc_id).await;

                    // Let's create a conflict here, cork to prevent sending these changes to the
                    // peer
                    peermerge
                        .splice_text(&doc_id, &text_id, 4, 2, "")
                        .await
                        .unwrap();
                    peermerge
                        .splice_text(&doc_id, &text_id, 4, 0, "YY")
                        .await
                        .unwrap();
                    assert_text_equals(&peermerge, &doc_id, &text_id, "HellYY world!").await;
                } else if document_changes.len() == 3 {
                    // This is the local deletions, one Delete patch with num 2
                    assert_eq!(patches.len(), 1);
                    document_changes.push(patches);
                } else if document_changes.len() == 4 {
                    // This is the two local additions as one Splice message
                    assert_eq!(patches.len(), 1);
                    document_changes.push(patches);
                    // Uncork to send the changes to the peer now
                    peermerge.uncork(&doc_id).await?;
                } else if document_changes.len() == 5 {
                    assert_eq!(patches.len(), 3); // One deletion that wasn't joined and two X chars
                    document_changes.push(patches);
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
                    assert!(merge_result.merge_equals());

                    // Now let's join in a latecomer peer to the creator peer
                    latecomer_attached = true;
                    let (proto_responder, proto_initiator) = create_pair_memory().await;
                    let (latecomer_state_event_sender, latecomer_state_event_receiver): (
                        UnboundedSender<StateEvent>,
                        UnboundedReceiver<StateEvent>,
                    ) = unbounded();
                    let mut peermerge_latecomer = Peermerge::new_memory("latecomer").await;
                    let latecomer_doc_id = peermerge_latecomer
                        .attach_writer_document_memory(&peermerge.doc_url(&doc_id), &None)
                        .await;
                    let peermerge_latecomer_for_task = peermerge_latecomer.clone();
                    let peermerge_creator_for_task = peermerge.clone();
                    let creator_state_event_sender_for_task = creator_state_event_sender.clone();
                    task::spawn(async move {
                        connect(
                            peermerge_creator_for_task,
                            proto_responder,
                            creator_state_event_sender_for_task,
                        )
                        .await
                        .unwrap();
                    });
                    task::spawn(async move {
                        connect(
                            peermerge_latecomer_for_task,
                            proto_initiator,
                            latecomer_state_event_sender,
                        )
                        .await
                        .unwrap();
                    });
                    let append_sync_latecomer = Arc::clone(&append_sync);
                    task::spawn(async move {
                        process_latecomer_state_event(
                            peermerge_latecomer,
                            latecomer_doc_id,
                            latecomer_state_event_receiver,
                            append_sync_latecomer,
                        )
                        .await
                        .unwrap();
                    });
                } else if document_changes.len() == 6 {
                    assert_eq!(patches.len(), 2); // Two latecomer additions
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
                } else {
                    panic!("Did not expect more creator document changes");
                }
            }
            _ => {
                panic!("Unkown event {:?}", event);
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
    let mut remote_peer_synced: HashMap<[u8; 32], u64> = HashMap::new();
    while let Some(event) = latecomer_state_event_receiver.next().await {
        info!(
            "Received event {:?}, document_changes={}",
            event,
            document_changes.len()
        );
        match event.content {
            PeerSynced((Some(name), _, len)) => {
                assert!(name == "creator" || name == "joiner");
                peer_synced.insert(name.clone(), len);
                if peer_synced.contains_key("creator")
                    && peer_synced.contains_key("joiner")
                    && text_id.is_none()
                {
                    let local_texts_id = peermerge.get_id(&doc_id, ROOT, "texts").await?.unwrap();
                    let local_text_id = peermerge
                        .get_id(&doc_id, &local_texts_id, "text")
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
                        .watch(&doc_id, vec![local_texts_id, text_id.clone().unwrap()])
                        .await;
                    // Make one final change and see that it propagates through to the creator
                    peermerge
                        .splice_text(&doc_id, &local_text_id, 13, 0, "ZZ")
                        .await?;
                }
            }
            RemotePeerSynced((discovery_key, len)) => {
                remote_peer_synced.insert(discovery_key, len);
            }
            DocumentChanged(patches) => {
                if document_changes.len() == 0 {
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

                    // Let's wait for this to end up, via the creator, to the joiner
                    wait_for_condvar(append_sync).await;
                    break;
                }
            }
            _ => {
                panic!("Unkown event {:?}", event);
            }
        }
    }
    Ok(())
}

async fn connect(
    mut peermerge: Peermerge<RandomAccessMemory, FeedMemoryPersistence>,
    mut protocol: MemoryProtocol,
    mut state_event_sender: UnboundedSender<StateEvent>,
) -> anyhow::Result<()> {
    peermerge
        .connect_protocol_memory(&mut protocol, &mut state_event_sender)
        .await?;
    Ok(())
}

async fn assert_text_equals(
    peermerge: &Peermerge<RandomAccessMemory, FeedMemoryPersistence>,
    doc_id: &DocumentId,
    obj: &ObjId,
    expected: &str,
) {
    let result = peermerge.realize_text(doc_id, obj).await;
    assert_eq!(result.unwrap().unwrap(), expected);
}

async fn assert_text_equals_either(
    peermerge: &Peermerge<RandomAccessMemory, FeedMemoryPersistence>,
    doc_id: &DocumentId,
    obj: &ObjId,
    expected_1: &str,
    expected_2: &str,
) -> String {
    let result = peermerge.realize_text(doc_id, obj).await.unwrap().unwrap();
    if result == expected_1 {
        return expected_1.to_string();
    } else if result == expected_2 {
        return expected_1.to_string();
    } else {
        panic!(
            "Text {} did not match either {} or {}",
            result, expected_1, expected_2
        );
    }
}
