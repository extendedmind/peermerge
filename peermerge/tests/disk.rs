use automerge::ROOT;
use futures::channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use futures::stream::StreamExt;
use peermerge::doc_url_encrypted;
use peermerge::Patch;
use peermerge::Peermerge;
use peermerge::StateEvent;
use random_access_disk::RandomAccessDisk;
use std::sync::Arc;
use tempfile::Builder;
use test_log::test;
use tracing::{info, instrument};

#[cfg(feature = "async-std")]
use async_std::{task, test as async_test};
#[cfg(feature = "tokio")]
use tokio::{task, test as async_test};

mod common;
use common::*;

#[test(async_test)]
async fn disk_two_peers_plain() -> anyhow::Result<()> {
    disk_two_peers(false).await
}

#[test(async_test)]
async fn disk_two_peers_encrypted() -> anyhow::Result<()> {
    disk_two_peers(true).await
}

async fn disk_two_peers(encrypted: bool) -> anyhow::Result<()> {
    let creator_dir = Builder::new()
        .prefix(&format!(
            "disk_two_peers_creator_{}",
            if encrypted { "encrypted" } else { "plain" }
        ))
        .tempdir()
        .unwrap()
        .into_path();
    let peermerge_creator =
        Peermerge::create_new_disk("creator", vec![("version", 1)], encrypted, &creator_dir).await;
    let doc_url = peermerge_creator.doc_url();
    let encryption_key = peermerge_creator.encryption_key();
    assert_eq!(doc_url_encrypted(&doc_url), encrypted);
    assert_eq!(encryption_key.is_some(), encrypted);

    let joiner_dir = Builder::new()
        .prefix(&format!(
            "disk_two_peers_joiner_{}",
            if encrypted { "encrypted" } else { "plain" }
        ))
        .tempdir()
        .unwrap()
        .into_path();
    let peermerge_joiner =
        Peermerge::attach_writer_disk("joiner", &doc_url, &encryption_key, &joiner_dir).await;

    run_disk_two_peers(
        peermerge_creator,
        peermerge_joiner,
        vec![("version".to_string(), 1)],
    )
    .await?;

    // Reopen the disk peermerges from disk, assert that opening works with new scalar

    let mut peermerge_creator = Peermerge::open_disk(&encryption_key, &creator_dir).await;
    peermerge_creator.put_scalar(ROOT, "open", 2).await?;

    let peermerge_joiner = Peermerge::open_disk(&encryption_key, &joiner_dir).await;

    run_disk_two_peers(
        peermerge_creator,
        peermerge_joiner,
        vec![("version".to_string(), 1), ("open".to_string(), 2)],
    )
    .await?;

    Ok(())
}

async fn run_disk_two_peers(
    peermerge_creator: Peermerge<RandomAccessDisk>,
    peermerge_joiner: Peermerge<RandomAccessDisk>,
    expected_scalars: Vec<(String, u64)>,
) -> anyhow::Result<()> {
    let (mut proto_responder, mut proto_initiator) = create_pair_memory().await;
    let (mut creator_state_event_sender, creator_state_event_receiver): (
        UnboundedSender<StateEvent>,
        UnboundedReceiver<StateEvent>,
    ) = unbounded();
    let (mut joiner_state_event_sender, joiner_state_event_receiver): (
        UnboundedSender<StateEvent>,
        UnboundedReceiver<StateEvent>,
    ) = unbounded();

    let assert_sync_creator = init_condvar();
    let assert_sync_joiner = Arc::clone(&assert_sync_creator);

    let mut peermerge_creator_for_task = peermerge_creator.clone();
    task::spawn(async move {
        peermerge_creator_for_task
            .connect_protocol_disk(&mut proto_responder, &mut creator_state_event_sender)
            .await
            .unwrap();
    });

    let mut peermerge_joiner_for_task = peermerge_joiner.clone();
    task::spawn(async move {
        peermerge_joiner_for_task
            .connect_protocol_disk(&mut proto_initiator, &mut joiner_state_event_sender)
            .await
            .unwrap();
    });
    let expected_scalars_for_task = expected_scalars.clone();
    task::spawn(async move {
        process_joiner_state_event(
            peermerge_joiner,
            joiner_state_event_receiver,
            assert_sync_joiner,
            expected_scalars_for_task,
        )
        .await
        .unwrap();
    });

    process_creator_state_events(
        peermerge_creator,
        creator_state_event_receiver,
        assert_sync_creator,
        expected_scalars,
    )
    .await?;
    Ok(())
}

#[instrument(skip_all)]
async fn process_joiner_state_event(
    peermerge: Peermerge<RandomAccessDisk>,
    mut joiner_state_event_receiver: UnboundedReceiver<StateEvent>,
    assert_sync: BoolCondvar,
    expected_scalars: Vec<(String, u64)>,
) -> anyhow::Result<()> {
    let mut document_changes: Vec<Vec<Patch>> = vec![];
    while let Some(event) = joiner_state_event_receiver.next().await {
        info!(
            "Received event {:?}, document_changes {:?}",
            event, document_changes
        );
        match event {
            StateEvent::PeerSynced((_, Some(name), _, len)) => {
                assert_eq!(name, "creator");
                assert_eq!(len, expected_scalars.len() as u64);
                for (field, expected) in &expected_scalars {
                    let (value, _) = peermerge.get(ROOT, field).await?.unwrap();
                    assert_eq!(value.to_u64().unwrap(), *expected);
                }
                notify_one_condvar(assert_sync.clone()).await;
                break;
            }
            StateEvent::RemotePeerSynced(_) => {}
            StateEvent::DocumentChanged((_, patches)) => {
                document_changes.push(patches);
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
    peermerge: Peermerge<RandomAccessDisk>,
    mut creator_state_event_receiver: UnboundedReceiver<StateEvent>,
    assert_sync: BoolCondvar,
    expected_scalars: Vec<(String, u64)>,
) -> anyhow::Result<()> {
    while let Some(event) = creator_state_event_receiver.next().await {
        info!("Received event {:?}", event);
        match event {
            StateEvent::PeerSynced((_, Some(name), _, len)) => {
                if expected_scalars.len() == 2 {
                    panic!("Invalid creator peer sync {:?}", name);
                }
                assert_eq!(name, "joiner");
                assert_eq!(len, expected_scalars.len() as u64);
                for (field, expected) in &expected_scalars {
                    let (value, _) = peermerge.get(ROOT, field).await?.unwrap();
                    assert_eq!(value.to_u64().unwrap(), *expected);
                }
                wait_for_condvar(assert_sync).await;
                break;
            }
            StateEvent::RemotePeerSynced((_, _, len)) => {
                if expected_scalars.len() > 1 {
                    assert_eq!(len, expected_scalars.len() as u64);
                    for (field, expected) in &expected_scalars {
                        let (value, _) = peermerge.get(ROOT, field).await?.unwrap();
                        assert_eq!(value.to_u64().unwrap(), *expected);
                    }
                    wait_for_condvar(assert_sync).await;
                    break;
                }
            }
            StateEvent::DocumentChanged((_, patches)) => {
                if expected_scalars.len() == 1 {
                    panic!("Invalid creator document changes {:?}", patches);
                }
                assert_eq!(patches.len(), 1);
            }
            _ => {
                panic!("Unkown event {:?}", event);
            }
        }
    }
    Ok(())
}
