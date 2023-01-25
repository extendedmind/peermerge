use automerge::ROOT;
use futures::channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use futures::stream::StreamExt;
use peermerge::doc_url_encrypted;
use peermerge::Patch;
use peermerge::Peermerge;
use peermerge::StateEvent;
use random_access_disk::RandomAccessDisk;
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
    let (mut proto_responder, mut proto_initiator) = create_pair_memory().await;
    let creator_dir = Builder::new()
        .prefix(&format!(
            "disk_two_peers_creator_{}",
            if encrypted { "encrypted" } else { "plain" }
        ))
        .tempdir()
        .unwrap()
        .into_path();

    let (mut creator_state_event_sender, creator_state_event_receiver): (
        UnboundedSender<StateEvent>,
        UnboundedReceiver<StateEvent>,
    ) = unbounded();
    let (mut joiner_state_event_sender, joiner_state_event_receiver): (
        UnboundedSender<StateEvent>,
        UnboundedReceiver<StateEvent>,
    ) = unbounded();
    let peermerge_creator =
        Peermerge::create_new_disk("creator", vec![("version", 1)], encrypted, creator_dir).await;
    let doc_url = peermerge_creator.doc_url();
    let encryption_key = peermerge_creator.encryption_key();
    assert_eq!(doc_url_encrypted(&doc_url), encrypted);
    assert_eq!(encryption_key.is_some(), encrypted);

    let mut peermerge_creator_for_task = peermerge_creator.clone();
    task::spawn(async move {
        peermerge_creator_for_task
            .connect_protocol_disk(&mut proto_responder, &mut creator_state_event_sender)
            .await
            .unwrap();
    });

    let joiner_dir = Builder::new()
        .prefix(&format!(
            "disk_two_peers_joiner_{}",
            if encrypted { "encrypted" } else { "plain" }
        ))
        .tempdir()
        .unwrap()
        .into_path();
    let peermerge_joiner =
        Peermerge::attach_write_peer_disk("joiner", &doc_url, &encryption_key, joiner_dir).await;
    let mut peermerge_joiner_for_task = peermerge_joiner.clone();
    task::spawn(async move {
        peermerge_joiner_for_task
            .connect_protocol_disk(&mut proto_initiator, &mut joiner_state_event_sender)
            .await
            .unwrap();
    });

    task::spawn(async move {
        process_joiner_state_event(peermerge_joiner, joiner_state_event_receiver)
            .await
            .unwrap();
    });

    process_creator_state_events(peermerge_creator, creator_state_event_receiver).await?;

    Ok(())
}

#[instrument(skip_all)]
async fn process_joiner_state_event(
    peermerge: Peermerge<RandomAccessDisk>,
    mut joiner_state_event_receiver: UnboundedReceiver<StateEvent>,
) -> anyhow::Result<()> {
    let mut document_changes: Vec<Vec<Patch>> = vec![];
    while let Some(event) = joiner_state_event_receiver.next().await {
        info!(
            "Received event {:?}, document_changes {:?}",
            event, document_changes
        );
        match event {
            StateEvent::PeerSynced((Some(name), _, len)) => {
                assert_eq!(name, "creator");
                assert_eq!(len, 1);
                let (value, _) = peermerge.get(ROOT, "version").await?.unwrap();
                assert_eq!(value.to_u64().unwrap(), 1);
            }
            StateEvent::RemotePeerSynced(_) => {}
            StateEvent::DocumentChanged(patches) => {
                document_changes.push(patches);
                break;
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
) -> anyhow::Result<()> {
    let mut document_changes: Vec<Vec<Patch>> = vec![];
    while let Some(event) = creator_state_event_receiver.next().await {
        info!(
            "Received event {:?}, document_changes {:?}",
            event, document_changes
        );
        match event {
            StateEvent::PeerSynced((Some(name), _, len)) => {
                assert_eq!(name, "joiner");
                assert_eq!(len, 1);
                let (value, _) = peermerge.get(ROOT, "version").await?.unwrap();
                assert_eq!(value.to_u64().unwrap(), 1);
                break;
            }
            StateEvent::RemotePeerSynced(_) => {}
            StateEvent::DocumentChanged(patches) => {
                document_changes.push(patches);
            }
            _ => {
                panic!("Unkown event {:?}", event);
            }
        }
    }
    Ok(())
}
