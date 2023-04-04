use automerge::ROOT;
use futures::{
    channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender},
    stream::StreamExt,
};
use peermerge::{
    get_doc_url_info, DocumentId, FeedDiskPersistence, NameDescription, Patch, Peermerge,
    StateEvent, StateEventContent::*,
};
use random_access_disk::RandomAccessDisk;
use std::{collections::HashMap, sync::Arc};
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

    let creator_document_infos = Peermerge::document_infos_disk(&creator_dir).await?;
    assert!(creator_document_infos.is_none());

    // let debug = format!("target/creator_{}", encrypted);
    // std::fs::create_dir_all(&debug).unwrap();
    // let creator_dir = std::path::Path::new(&debug).to_path_buf();

    let mut peermerge_creator =
        Peermerge::create_new_disk(NameDescription::new("creator"), &creator_dir).await;
    let document_name = "disk_test";
    let creator_doc_id = peermerge_creator
        .create_new_document_disk(
            NameDescription::new(document_name),
            vec![("version", 1)],
            encrypted,
        )
        .await?;
    let doc_url = peermerge_creator.doc_url(&creator_doc_id).await;
    let encryption_key = peermerge_creator.encryption_key(&creator_doc_id).await;
    assert_eq!(get_doc_url_info(&doc_url).encrypted, Some(encrypted));
    assert_eq!(encryption_key.is_some(), encrypted);

    let joiner_dir = Builder::new()
        .prefix(&format!(
            "disk_two_peers_joiner_{}",
            if encrypted { "encrypted" } else { "plain" }
        ))
        .tempdir()
        .unwrap()
        .into_path();

    // let debug = format!("target/joiner_{}", encrypted);
    // std::fs::create_dir_all(&debug).unwrap();
    // let joiner_dir = std::path::Path::new(&debug).to_path_buf();

    let mut peermerge_joiner =
        Peermerge::create_new_disk(NameDescription::new("joiner"), &joiner_dir).await;
    let joiner_doc_id = peermerge_joiner
        .attach_writer_document_disk(&doc_url, &encryption_key)
        .await?;

    run_disk_two_peers(
        peermerge_creator,
        creator_doc_id.clone(),
        peermerge_joiner,
        joiner_doc_id.clone(),
        vec![("version".to_string(), 1)],
    )
    .await?;

    // Reopen the disk peermerges from disk, assert that opening works with new scalar
    let creator_document_infos = Peermerge::document_infos_disk(&creator_dir).await?.unwrap();
    assert_eq!(creator_document_infos.len(), 1);
    assert_eq!(
        creator_document_infos[0].doc_url_info.encrypted,
        Some(encrypted)
    );
    assert_eq!(
        creator_document_infos[0]
            .document_header
            .clone()
            .unwrap()
            .name,
        document_name
    );
    assert_eq!(creator_document_infos[0].document_id, creator_doc_id);

    let mut creator_encryption_keys = HashMap::new();
    if let Some(encryption_key) = encryption_key.as_ref() {
        creator_encryption_keys.insert(creator_doc_id.clone(), encryption_key.clone());
    }
    let mut peermerge_creator = Peermerge::open_disk(creator_encryption_keys, &creator_dir).await?;
    peermerge_creator
        .put_scalar(&creator_doc_id, ROOT, "open", 2)
        .await?;

    let joiner_document_infos = Peermerge::document_infos_disk(&creator_dir).await?.unwrap();
    assert_eq!(joiner_document_infos.len(), 1);
    assert_eq!(
        joiner_document_infos[0].doc_url_info.encrypted,
        Some(encrypted)
    );
    assert_eq!(
        joiner_document_infos[0]
            .document_header
            .clone()
            .unwrap()
            .name,
        document_name
    );
    assert_eq!(joiner_document_infos[0].document_id, joiner_doc_id);

    let mut joiner_encryption_keys = HashMap::new();
    if let Some(encryption_key) = encryption_key.as_ref() {
        joiner_encryption_keys.insert(joiner_doc_id.clone(), encryption_key.clone());
    }
    let peermerge_joiner = Peermerge::open_disk(joiner_encryption_keys, &joiner_dir).await?;

    run_disk_two_peers(
        peermerge_creator,
        creator_doc_id,
        peermerge_joiner,
        joiner_doc_id,
        vec![("version".to_string(), 1), ("open".to_string(), 2)],
    )
    .await?;

    Ok(())
}

async fn run_disk_two_peers(
    peermerge_creator: Peermerge<RandomAccessDisk, FeedDiskPersistence>,
    creator_doc_id: DocumentId,
    peermerge_joiner: Peermerge<RandomAccessDisk, FeedDiskPersistence>,
    joiner_doc_id: DocumentId,
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
            joiner_doc_id,
            joiner_state_event_receiver,
            assert_sync_joiner,
            expected_scalars_for_task,
        )
        .await
        .unwrap();
    });

    process_creator_state_events(
        peermerge_creator,
        creator_doc_id,
        creator_state_event_receiver,
        assert_sync_creator,
        expected_scalars,
    )
    .await?;
    Ok(())
}

#[instrument(skip_all)]
async fn process_joiner_state_event(
    peermerge: Peermerge<RandomAccessDisk, FeedDiskPersistence>,
    doc_id: DocumentId,
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
        match event.content {
            PeerSynced((Some(name), _, len)) => {
                assert_eq!(name, "creator");
                assert_eq!(len, expected_scalars.len() as u64);
                for (field, expected) in &expected_scalars {
                    let value = peermerge.get_scalar(&doc_id, ROOT, field).await?.unwrap();
                    assert_eq!(value.to_u64().unwrap(), *expected);
                }
                notify_one_condvar(assert_sync.clone()).await;
                break;
            }
            RemotePeerSynced(_) => {}
            DocumentInitialized() => {
                // Just ignore for now
            }
            DocumentChanged(patches) => {
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
    peermerge: Peermerge<RandomAccessDisk, FeedDiskPersistence>,
    doc_id: DocumentId,
    mut creator_state_event_receiver: UnboundedReceiver<StateEvent>,
    assert_sync: BoolCondvar,
    expected_scalars: Vec<(String, u64)>,
) -> anyhow::Result<()> {
    while let Some(event) = creator_state_event_receiver.next().await {
        info!("Received event {:?}", event);
        match event.content {
            PeerSynced((Some(name), _, len)) => {
                if expected_scalars.len() == 2 {
                    panic!("Invalid creator peer sync {:?}", name);
                }
                assert_eq!(name, "joiner");
                assert_eq!(len, expected_scalars.len() as u64);
                for (field, expected) in &expected_scalars {
                    let value = peermerge.get_scalar(&doc_id, ROOT, field).await?.unwrap();
                    assert_eq!(value.to_u64().unwrap(), *expected);
                }
                wait_for_condvar(assert_sync).await;
                break;
            }
            RemotePeerSynced((discovery_key, len)) => {
                if expected_scalars.len() > 1 && discovery_key != doc_id {
                    assert_eq!(len, expected_scalars.len() as u64);
                    for (field, expected) in &expected_scalars {
                        let value = peermerge.get_scalar(&doc_id, ROOT, field).await?.unwrap();
                        assert_eq!(value.to_u64().unwrap(), *expected);
                    }
                    wait_for_condvar(assert_sync).await;
                    break;
                }
            }
            DocumentChanged(patches) => {
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
