#![allow(dead_code, unused_imports)]

use async_std::net::TcpStream;
use async_std::prelude::*;
use async_std::sync::{Arc, Condvar, Mutex};
use async_std::task;
use automerge::ObjId;
use automerge::ROOT;
use futures::channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use futures::io::{AsyncRead, AsyncWrite};
use futures::stream::StreamExt;
use hypercore_protocol::{discovery_key, Channel, Event, Message, Protocol, ProtocolBuilder};
use hypercore_protocol::{schema::*, DiscoveryKey};
use hypermerge::Hypermerge;
use hypermerge::Patch;
use hypermerge::StateEvent;
use hypermerge::Value;
use random_access_memory::RandomAccessMemory;
use std::collections::HashMap;
use std::io;
use std::time::Duration;
use test_log::test;
use tracing::{info, instrument};

mod common;
use common::*;

#[derive(Clone, Debug, Default)]
struct ProtocolThreeWritersResult {
    joiner_merge: Option<String>,
    creator_merge: Option<String>,
}
impl ProtocolThreeWritersResult {
    pub fn merge_equals(&self) -> bool {
        self.joiner_merge.as_ref().unwrap() == self.creator_merge.as_ref().unwrap()
    }
}

#[test(async_std::test)]
async fn protocol_three_writers() -> anyhow::Result<()> {
    let (proto_responder, proto_initiator) = create_pair_memory().await;

    let (creator_state_event_sender, creator_state_event_receiver): (
        UnboundedSender<StateEvent>,
        UnboundedReceiver<StateEvent>,
    ) = unbounded();
    let (joiner_state_event_sender, joiner_state_event_receiver): (
        UnboundedSender<StateEvent>,
        UnboundedReceiver<StateEvent>,
    ) = unbounded();
    let mut hypermerge_creator =
        Hypermerge::create_doc_memory("creator", vec![("version", 1)]).await;

    // Insert a map with a text field to the document
    let texts_id = hypermerge_creator
        .put_object(ROOT, "texts", automerge::ObjType::Map)
        .await
        .unwrap();
    let text_id = hypermerge_creator
        .put_object(&texts_id, "text", automerge::ObjType::Text)
        .await
        .unwrap();

    // Set watching for the prop
    hypermerge_creator
        .watch(vec![texts_id.clone(), text_id.clone()])
        .await;

    let hypermerge_creator_for_task = hypermerge_creator.clone();
    let creator_state_event_sender_for_task = creator_state_event_sender.clone();
    task::spawn(async move {
        connect(
            hypermerge_creator_for_task,
            proto_responder,
            creator_state_event_sender_for_task,
        )
        .await
        .unwrap();
    });

    let hypermerge_joiner =
        Hypermerge::register_doc_memory("joiner", &hypermerge_creator.doc_url()).await;
    let hypermerge_joiner_for_task = hypermerge_joiner.clone();
    task::spawn(async move {
        connect(
            hypermerge_joiner_for_task,
            proto_initiator,
            joiner_state_event_sender,
        )
        .await
        .unwrap();
    });

    let cork_sync_creator = Arc::new((Mutex::new(false), Condvar::new()));
    let cork_sync_joiner = Arc::clone(&cork_sync_creator);
    let merge_result_for_creator = Arc::new(Mutex::new(ProtocolThreeWritersResult::default()));
    let merge_result_for_joiner = merge_result_for_creator.clone();
    let append_sync_creator = Arc::new((Mutex::new(false), Condvar::new()));
    let append_sync_joiner = Arc::clone(&append_sync_creator);

    task::spawn(async move {
        process_joiner_state_event(
            hypermerge_joiner,
            joiner_state_event_receiver,
            cork_sync_joiner,
            merge_result_for_joiner,
            append_sync_joiner,
        )
        .await
        .unwrap();
    });

    process_creator_state_events(
        hypermerge_creator,
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
    mut hypermerge: Hypermerge<RandomAccessMemory>,
    mut joiner_state_event_receiver: UnboundedReceiver<StateEvent>,
    cork_sync: Arc<(Mutex<bool>, Condvar)>,
    merge_result: Arc<Mutex<ProtocolThreeWritersResult>>,
    append_sync: Arc<(Mutex<bool>, Condvar)>,
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
        match event {
            StateEvent::PeerSynced((name, _, len)) => {
                if !peer_synced.contains_key("creator") {
                    assert_eq!(name, "creator");
                    let (_value, local_texts_id) = hypermerge.get(ROOT, "texts").await?.unwrap();
                    let (_value, local_text_id) =
                        hypermerge.get(&local_texts_id, "text").await?.unwrap();
                    assert_text_equals(&hypermerge, &local_text_id, "").await;
                    text_id = Some(local_text_id.clone());
                    hypermerge
                        .watch(vec![local_texts_id, text_id.clone().unwrap()])
                        .await;
                } else {
                    assert!(name == "creator" || name == "latecomer");
                }
                peer_synced.insert(name.clone(), len);
            }
            StateEvent::RemotePeerSynced((discovery_key, len)) => {
                remote_peer_synced.insert(discovery_key, len);
            }
            StateEvent::DocumentChanged(patches) => {
                if document_changes.len() == 0 {
                    assert_eq!(patches.len(), 5); // "Hello" has 5 chars
                    document_changes.push(patches);
                    let text_id = text_id.clone().unwrap();
                    assert_text_equals(&hypermerge, &text_id, "Hello").await;
                    hypermerge.splice_text(&text_id, 5, 0, ", world!").await?;
                } else if document_changes.len() == 1 {
                    assert_eq!(patches.len(), 1); // ", world!" in one Splice patch
                    document_changes.push(patches);
                    let text_id = text_id.clone().unwrap();
                    assert_text_equals(&hypermerge, &text_id, "Hello, world!").await;

                    // Let's make sure via variable that the other end is also ready to cork
                    let (lock, cvar) = &*cork_sync;
                    let mut started = lock.lock().await;
                    while !*started {
                        started = cvar.wait(started).await;
                    }

                    // Ok, ready to cork in unison
                    hypermerge.cork().await;
                    hypermerge.splice_text(&text_id, 5, 2, "").await?;
                    hypermerge.splice_text(&text_id, 4, 0, "XX").await?;
                    assert_text_equals(&hypermerge, &text_id, "HellXXoworld!").await;
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
                            &hypermerge,
                            &text_id,
                            "HellXXYYworld!",
                            "HellYYXXworld!",
                        )
                        .await,
                    );
                    // These are the changes sent by the peer, uncork to send the changes to the peer now
                    hypermerge.uncork().await.unwrap();
                } else if document_changes.len() == 5 {
                    assert_eq!(patches.len(), 2); // Two latecomer additions
                    document_changes.push(patches);
                    let text_id = text_id.clone().unwrap();
                    assert_text_equals_either(
                        &hypermerge,
                        &text_id,
                        "HellXXYYworldZZ!",
                        "HellYYXXworldZZ!",
                    )
                    .await;

                    // Notify about append to both
                    let (lock, cvar) = &*append_sync;
                    let mut appended = lock.lock().await;
                    *appended = true;
                    cvar.notify_all();
                    break;
                } else {
                    panic!("Did not expect more joiner document changes");
                }
            }
        }
    }
    Ok(())
}

#[instrument(skip_all)]
async fn process_creator_state_events(
    mut hypermerge: Hypermerge<RandomAccessMemory>,
    creator_state_event_sender: UnboundedSender<StateEvent>,
    mut creator_state_event_receiver: UnboundedReceiver<StateEvent>,
    text_id: ObjId,
    cork_sync: Arc<(Mutex<bool>, Condvar)>,
    merge_result: Arc<Mutex<ProtocolThreeWritersResult>>,
    append_sync: Arc<(Mutex<bool>, Condvar)>,
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
        match event {
            StateEvent::PeerSynced((name, _, len)) => {
                peer_synced.insert(name.clone(), len);
                if latecomer_attached {
                    assert!(name == "joiner" || name == "latecomer");
                } else {
                    assert_eq!(name, "joiner");
                }
            }
            StateEvent::RemotePeerSynced((discovery_key, len)) => {
                if remote_peer_synced.is_empty() {
                    hypermerge
                        .splice_text(&text_id, 0, 0, "Hello")
                        .await
                        .unwrap();
                    assert_text_equals(&hypermerge, &text_id, "Hello").await;
                }
                remote_peer_synced.insert(discovery_key, len);
            }
            StateEvent::DocumentChanged(patches) => {
                if document_changes.len() == 0 {
                    assert_eq!(patches.len(), 2); // Original creation of "texts" and "text";
                    document_changes.push(patches);
                } else if document_changes.len() == 1 {
                    assert_eq!(patches.len(), 1); // "Hello" in one Splice patch
                    document_changes.push(patches);
                } else if document_changes.len() == 2 {
                    assert_eq!(patches.len(), 8); // ", world!" has 8 chars
                    document_changes.push(patches);
                    assert_text_equals(&hypermerge, &text_id, "Hello, world!").await;

                    // Ready to notify about cork
                    let (lock, cvar) = &*cork_sync;
                    let mut started = lock.lock().await;
                    *started = true;
                    cvar.notify_one();

                    // Ok, ready to cork
                    hypermerge.cork().await;

                    // Let's create a conflict here, cork to prevent sending these changes to the
                    // peer
                    hypermerge.splice_text(&text_id, 4, 2, "").await.unwrap();
                    hypermerge.splice_text(&text_id, 4, 0, "YY").await.unwrap();
                    assert_text_equals(&hypermerge, &text_id, "HellYY world!").await;
                } else if document_changes.len() == 3 {
                    // This is the local deletions, one Delete patch with num 2
                    assert_eq!(patches.len(), 1);
                    document_changes.push(patches);
                } else if document_changes.len() == 4 {
                    // This is the two local additions as one Splice message
                    assert_eq!(patches.len(), 1);
                    document_changes.push(patches);
                    // Uncork to send the changes to the peer now
                    hypermerge.uncork().await?;
                } else if document_changes.len() == 5 {
                    assert_eq!(patches.len(), 3); // One deletion that wasn't joined and two X chars
                    document_changes.push(patches);
                    let mut merge_result = merge_result.lock().await;
                    merge_result.creator_merge = Some(
                        assert_text_equals_either(
                            &hypermerge,
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
                    let hypermerge_latecomer =
                        Hypermerge::register_doc_memory("latecomer", &hypermerge.doc_url()).await;
                    let hypermerge_latecomer_for_task = hypermerge_latecomer.clone();
                    let hypermerge_creator_for_task = hypermerge.clone();
                    let creator_state_event_sender_for_task = creator_state_event_sender.clone();
                    task::spawn(async move {
                        connect(
                            hypermerge_creator_for_task,
                            proto_responder,
                            creator_state_event_sender_for_task,
                        )
                        .await
                        .unwrap();
                    });
                    task::spawn(async move {
                        connect(
                            hypermerge_latecomer_for_task,
                            proto_initiator,
                            latecomer_state_event_sender,
                        )
                        .await
                        .unwrap();
                    });
                    let append_sync_latecomer = Arc::clone(&append_sync);
                    task::spawn(async move {
                        process_latecomer_state_event(
                            hypermerge_latecomer,
                            latecomer_state_event_receiver,
                            append_sync_latecomer,
                        )
                        .await
                        .unwrap();
                    });
                } else if document_changes.len() == 6 {
                    assert_eq!(patches.len(), 2); // Two latecomer additions
                    assert_text_equals_either(
                        &hypermerge,
                        &text_id,
                        "HellXXYYworldZZ!",
                        "HellYYXXworldZZ!",
                    )
                    .await;

                    // Let's wait for the sync to end up to the joiner
                    let (lock, cvar) = &*append_sync;
                    let mut appended = lock.lock().await;
                    while !*appended {
                        appended = cvar.wait(appended).await;
                    }
                    break;
                } else {
                    panic!("Did not expect more creator document changes");
                }
            }
        }
    }
    Ok(())
}

#[instrument(skip_all)]
async fn process_latecomer_state_event(
    mut hypermerge: Hypermerge<RandomAccessMemory>,
    mut latecomer_state_event_receiver: UnboundedReceiver<StateEvent>,
    append_sync: Arc<(Mutex<bool>, Condvar)>,
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
        match event {
            StateEvent::PeerSynced((name, _, len)) => {
                assert!(name == "creator" || name == "joiner");
                peer_synced.insert(name.clone(), len);
                if peer_synced.contains_key("creator")
                    && peer_synced.contains_key("joiner")
                    && text_id.is_none()
                {
                    let (_value, local_texts_id) = hypermerge.get(ROOT, "texts").await?.unwrap();
                    let (_value, local_text_id) =
                        hypermerge.get(&local_texts_id, "text").await?.unwrap();
                    assert_text_equals_either(
                        &hypermerge,
                        &local_text_id,
                        "HellXXYYworld!",
                        "HellYYXXworld!",
                    )
                    .await;
                    text_id = Some(local_text_id.clone());
                    hypermerge
                        .watch(vec![local_texts_id, text_id.clone().unwrap()])
                        .await;
                    // Make one final change and see that it propagates through to the creator
                    hypermerge.splice_text(&local_text_id, 13, 0, "ZZ").await?;
                }
            }
            StateEvent::RemotePeerSynced((discovery_key, len)) => {
                remote_peer_synced.insert(discovery_key, len);
            }
            StateEvent::DocumentChanged(patches) => {
                if document_changes.len() == 0 {
                    assert_eq!(patches.len(), 1); // Two local additions as one Splice
                    assert_text_equals_either(
                        &hypermerge,
                        &text_id.clone().unwrap(),
                        "HellXXYYworldZZ!",
                        "HellYYXXworldZZ!",
                    )
                    .await;
                    document_changes.push(patches);

                    // Let's wait for this to end up, via the creator, to the joiner
                    let (lock, cvar) = &*append_sync;
                    let mut appended = lock.lock().await;
                    while !*appended {
                        appended = cvar.wait(appended).await;
                    }
                    break;
                }
            }
        }
    }
    Ok(())
}

async fn connect(
    mut hypermerge: Hypermerge<RandomAccessMemory>,
    mut protocol: MemoryProtocol,
    mut state_event_sender: UnboundedSender<StateEvent>,
) -> anyhow::Result<()> {
    hypermerge
        .connect_protocol_memory(&mut protocol, &mut state_event_sender)
        .await?;
    Ok(())
}

async fn assert_text_equals(
    hypermerge: &Hypermerge<RandomAccessMemory>,
    obj: &ObjId,
    expected: &str,
) {
    let result = hypermerge.realize_text(obj).await;
    assert_eq!(result.unwrap().unwrap(), expected);
}

async fn assert_text_equals_either(
    hypermerge: &Hypermerge<RandomAccessMemory>,
    obj: &ObjId,
    expected_1: &str,
    expected_2: &str,
) -> String {
    let result = hypermerge.realize_text(obj).await.unwrap().unwrap();
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
