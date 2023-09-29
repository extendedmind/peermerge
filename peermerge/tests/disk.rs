use futures::{
    channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender},
    future::join_all,
    stream::StreamExt,
};
use peermerge::{
    automerge::{transaction::Transactable, Patch, ROOT},
    get_document_info, AttachDocumentDiskOptionsBuilder, CreateNewDocumentDiskOptionsBuilder,
    DocumentId, FeedDiskPersistence, NameDescription, Peermerge, PeermergeDiskOptionsBuilder,
    StateEvent,
    StateEventContent::*,
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

pub mod common;
use common::*;

#[test(async_test)]
async fn disk_two_peers_plain() -> anyhow::Result<()> {
    disk_two_peers(false, 10).await
}

#[test(async_test)]
async fn disk_two_peers_encrypted() -> anyhow::Result<()> {
    disk_two_peers(true, 5).await
}

async fn disk_two_peers(encrypted: bool, max_write_feed_length: u64) -> anyhow::Result<()> {
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

    let mut peermerge_creator = Peermerge::new_disk(
        PeermergeDiskOptionsBuilder::default()
            .default_peer_header(NameDescription::new("creator"))
            .data_root_dir(creator_dir.clone())
            .max_write_feed_length(max_write_feed_length)
            .build()?,
    )
    .await?;
    let document_name = "disk_test";
    let (creator_doc_info, _) = peermerge_creator
        .create_new_document_disk(
            CreateNewDocumentDiskOptionsBuilder::default()
                .document_type("test".to_string())
                .document_header(NameDescription::new(document_name))
                .encrypted(encrypted)
                .build()?,
            |tx| tx.put(ROOT, "version", 1),
            None,
        )
        .await?;
    let doc_url = peermerge_creator
        .sharing_info(&creator_doc_info.id())
        .await?
        .read_write_document_url;
    let document_secret = peermerge_creator
        .document_secret(&creator_doc_info.id())
        .await?
        .unwrap();
    assert_eq!(
        get_document_info(&doc_url, Some(document_secret.clone()))
            .unwrap()
            .encrypted,
        Some(encrypted)
    );
    peermerge_creator.close().await?;

    // Reopen immediately after create

    let creator_document_infos = Peermerge::document_infos_disk(&creator_dir).await?.unwrap();
    assert_eq!(creator_document_infos.len(), 1);
    assert_eq!(creator_document_infos[0].encrypted, Some(encrypted));
    assert_eq!(
        creator_document_infos[0]
            .dynamic_info
            .clone()
            .unwrap()
            .document_header
            .unwrap()
            .name,
        document_name
    );
    assert_eq!(creator_document_infos[0].id(), creator_doc_info.id());
    let mut creator_document_secrets = HashMap::new();
    creator_document_secrets.insert(creator_doc_info.id(), document_secret.clone());
    let peermerge_creator =
        Peermerge::open_disk(creator_document_secrets, &creator_dir, None).await?;

    // Create joinder

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

    let mut peermerge_joiner = Peermerge::new_disk(
        PeermergeDiskOptionsBuilder::default()
            .default_peer_header(NameDescription::new("joiner"))
            .data_root_dir(joiner_dir.clone())
            .build()?,
    )
    .await?;
    let joiner_doc_info = peermerge_joiner
        .attach_document_disk(
            AttachDocumentDiskOptionsBuilder::default()
                .document_url(doc_url)
                .document_secret(document_secret.clone())
                .build()?,
        )
        .await?;
    assert_eq!(
        joiner_doc_info.dynamic_info.as_ref().unwrap().document_type,
        "test".to_string()
    );
    assert_eq!(
        joiner_doc_info
            .dynamic_info
            .as_ref()
            .unwrap()
            .document_header
            .as_ref()
            .unwrap()
            .name,
        document_name
    );

    // Run synchronize and creation of value

    run_disk_two_peers(
        peermerge_creator,
        creator_doc_info.id(),
        peermerge_joiner,
        joiner_doc_info.id(),
        vec![("version".to_string(), 1)],
        1,
    )
    .await?;

    // Verify that document infos can be fetched

    let creator_document_infos = Peermerge::document_infos_disk(&creator_dir).await?.unwrap();
    assert_eq!(creator_document_infos.len(), 1);
    assert_eq!(creator_document_infos[0].encrypted, Some(encrypted));
    assert_eq!(
        creator_document_infos[0]
            .dynamic_info
            .clone()
            .unwrap()
            .document_header
            .unwrap()
            .name,
        document_name
    );
    assert_eq!(creator_document_infos[0].id(), creator_doc_info.id());

    let mut creator_document_secrets = HashMap::new();
    creator_document_secrets.insert(creator_doc_info.id(), document_secret.clone());

    // Reopen and append values standalone creator that cause the max feed length to be exceeded
    {
        let mut peermerge_creator =
            Peermerge::open_disk(creator_document_secrets.clone(), &creator_dir, None).await?;
        for i in 0..max_write_feed_length {
            peermerge_creator
                .transact_mut(
                    &creator_doc_info.id(),
                    |doc| doc.put(ROOT, format!("s{i}"), i),
                    None,
                )
                .await?;
        }
        let (creator_state_event_sender, mut creator_state_event_receiver): (
            UnboundedSender<StateEvent>,
            UnboundedReceiver<StateEvent>,
        ) = unbounded();
        Peermerge::open_disk(
            creator_document_secrets.clone(),
            &creator_dir,
            Some(creator_state_event_sender),
        )
        .await?;
        if let Some(event) = creator_state_event_receiver.next().await {
            assert!(matches!(event.content, PeerChanged { .. }));
        }
        if let Some(event) = creator_state_event_receiver.next().await {
            assert!(matches!(event.content, DocumentInitialized { .. }));
        }
    }

    // Reopen both the disk peermerges, assert that opening works with new scalar

    let mut peermerge_creator =
        Peermerge::open_disk(creator_document_secrets, &creator_dir, None).await?;
    let values = peermerge_creator
        .transact_mut(
            &creator_doc_info.id(),
            |doc| {
                let open_value = 2;
                let reopen_value = 3;
                doc.put(ROOT, "open", open_value)?;
                doc.put(ROOT, "reopen", reopen_value)?;
                Ok(vec![open_value, reopen_value])
            },
            None,
        )
        .await?;
    assert_eq!(values, vec![2, 3]);

    let joiner_document_infos = Peermerge::document_infos_disk(&creator_dir).await?.unwrap();
    assert_eq!(joiner_document_infos.len(), 1);
    assert_eq!(joiner_document_infos[0].encrypted, Some(encrypted));
    assert_eq!(
        joiner_document_infos[0]
            .dynamic_info
            .clone()
            .unwrap()
            .document_header
            .unwrap()
            .name,
        document_name
    );
    assert_eq!(joiner_document_infos[0].id(), joiner_doc_info.id());

    let mut joiner_document_secrets = HashMap::new();
    joiner_document_secrets.insert(joiner_doc_info.id(), document_secret);
    let peermerge_joiner = Peermerge::open_disk(joiner_document_secrets, &joiner_dir, None).await?;

    run_disk_two_peers(
        peermerge_creator,
        creator_doc_info.id(),
        peermerge_joiner,
        joiner_doc_info.id(),
        vec![
            ("version".to_string(), 1),
            ("open".to_string(), 2),
            ("reopen".to_string(), 3),
        ],
        2,
    )
    .await?;

    Ok(())
}

async fn run_disk_two_peers(
    mut peermerge_creator: Peermerge<RandomAccessDisk, FeedDiskPersistence>,
    creator_doc_id: DocumentId,
    mut peermerge_joiner: Peermerge<RandomAccessDisk, FeedDiskPersistence>,
    joiner_doc_id: DocumentId,
    expected_scalars: Vec<(String, u64)>,
    expected_changes: usize,
) -> anyhow::Result<()> {
    let (mut proto_responder, mut proto_initiator) = create_pair_memory().await;
    let (creator_state_event_sender, creator_state_event_receiver): (
        UnboundedSender<StateEvent>,
        UnboundedReceiver<StateEvent>,
    ) = unbounded();
    peermerge_creator
        .set_state_event_sender(Some(creator_state_event_sender))
        .await?;
    let (joiner_state_event_sender, joiner_state_event_receiver): (
        UnboundedSender<StateEvent>,
        UnboundedReceiver<StateEvent>,
    ) = unbounded();
    peermerge_joiner
        .set_state_event_sender(Some(joiner_state_event_sender))
        .await?;

    let assert_sync_creator = init_condvar();
    let assert_sync_joiner = Arc::clone(&assert_sync_creator);

    let mut peermerge_creator_for_task = peermerge_creator.clone();
    let creator_connect = task::spawn(async move {
        peermerge_creator_for_task
            .connect_protocol_disk(&mut proto_responder)
            .await
            .unwrap();
    });

    let mut peermerge_joiner_for_task = peermerge_joiner.clone();
    let joiner_connect = task::spawn(async move {
        peermerge_joiner_for_task
            .connect_protocol_disk(&mut proto_initiator)
            .await
            .unwrap();
    });
    let expected_scalars_for_task = expected_scalars.clone();
    let joiner_process = task::spawn(async move {
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
        &mut peermerge_creator,
        creator_doc_id,
        creator_state_event_receiver,
        assert_sync_creator,
        expected_scalars,
        expected_changes,
    )
    .await?;

    peermerge_creator.close().await.unwrap();
    join_all(vec![creator_connect, joiner_connect, joiner_process]).await;
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
        assert_eq!(
            event.document_id, doc_id,
            "Event {event:?} has the wrong document id"
        );
        match event.content {
            PeerSynced {
                peer_id,
                contiguous_length,
                ..
            } => {
                let name = peermerge
                    .peer_header(&event.document_id, &peer_id)
                    .await?
                    .unwrap()
                    .name;
                assert_eq!(name, "creator");
                let expected_len = if expected_scalars.len() > 1 { 2 } else { 1 };
                assert_eq!(contiguous_length, expected_len);
                for (field, expected) in &expected_scalars {
                    let value = peermerge
                        .transact(&doc_id, |doc| get_scalar(doc, ROOT, field))
                        .await?
                        .unwrap();
                    assert_eq!(value.to_u64().unwrap(), *expected);
                }
                notify_one_condvar(assert_sync.clone()).await;
                break;
            }
            RemotePeerSynced { .. } => {}
            DocumentInitialized { .. } => {
                // Just ignore for now
            }
            DocumentChanged { change_id, patches } => {
                assert!(change_id.is_none());
                document_changes.push(patches);
            }
            PeerChanged { .. } => {}
        }
    }
    Ok(())
}

#[instrument(skip_all)]
async fn process_creator_state_events(
    peermerge: &mut Peermerge<RandomAccessDisk, FeedDiskPersistence>,
    doc_id: DocumentId,
    mut creator_state_event_receiver: UnboundedReceiver<StateEvent>,
    assert_sync: BoolCondvar,
    expected_scalars: Vec<(String, u64)>,
    expected_changes: usize,
) -> anyhow::Result<()> {
    let mut document_changes: Vec<Vec<Patch>> = vec![];
    while let Some(event) = creator_state_event_receiver.next().await {
        info!("Received event {:?}", event);
        assert_eq!(
            event.document_id, doc_id,
            "Event {event:?} has the wrong document id"
        );
        match event.content {
            PeerSynced {
                peer_id,
                contiguous_length,
                ..
            } => {
                let name = peermerge
                    .peer_header(&event.document_id, &peer_id)
                    .await?
                    .unwrap()
                    .name;

                if expected_scalars.len() == 2 {
                    panic!("Invalid creator peer sync {name:?}");
                }
                assert_eq!(name, "joiner");
                assert_eq!(contiguous_length, expected_scalars.len() as u64);
                for (field, expected) in &expected_scalars {
                    let value = peermerge
                        .transact(&doc_id, |doc| get_scalar(doc, ROOT, field))
                        .await?
                        .unwrap();
                    assert_eq!(value.to_u64().unwrap(), *expected);
                }
                wait_for_condvar(assert_sync).await;
                break;
            }
            RemotePeerSynced {
                contiguous_length, ..
            } => {
                if expected_scalars.len() > 1 {
                    assert_eq!(contiguous_length, 2);
                    for (field, expected) in &expected_scalars {
                        let value = peermerge
                            .transact(&doc_id, |doc| get_scalar(doc, ROOT, field))
                            .await?
                            .unwrap();

                        assert_eq!(value.to_u64().unwrap(), *expected);
                    }
                    wait_for_condvar(assert_sync).await;
                    break;
                }
            }
            DocumentChanged { patches, .. } => {
                assert_eq!(patches.len(), expected_changes);
                document_changes.push(patches);
            }
            PeerChanged {
                replaced_discovery_key,
                ..
            } => {
                assert!(replaced_discovery_key.is_none());
            }
            _ => {
                panic!("Unkown event {event:?}");
            }
        }
    }
    Ok(())
}
