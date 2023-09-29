use std::collections::HashMap;
use std::fmt::Debug;

use automerge::ScalarValue;
use futures::channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use futures::future::join_all;
use futures::stream::StreamExt;
use peermerge::{
    automerge::{transaction::Transactable, Patch, ROOT},
    get_document_info, AttachDocumentDiskOptionsBuilder, AttachDocumentMemoryOptionsBuilder,
    CreateNewDocumentMemoryOptionsBuilder, DocumentId, FeedMemoryPersistence, NameDescription,
    PeerId, Peermerge, PeermergeDiskOptionsBuilder, PeermergeMemoryOptionsBuilder, StateEvent,
    StateEventContent::*,
};
use peermerge::{CreateNewDocumentDiskOptionsBuilder, FeedPersistence};
use random_access_memory::RandomAccessMemory;
use random_access_storage::RandomAccess;
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
async fn proxy_disk_encrypted() -> anyhow::Result<()> {
    let (mut proto_responder, mut proto_initiator) = create_pair_memory().await;
    let (creator_state_event_sender, creator_state_event_receiver): (
        UnboundedSender<StateEvent>,
        UnboundedReceiver<StateEvent>,
    ) = unbounded();
    let (proxy_state_event_sender, proxy_state_event_receiver): (
        UnboundedSender<StateEvent>,
        UnboundedReceiver<StateEvent>,
    ) = unbounded();
    let mut peermerge_creator = Peermerge::new_memory(
        PeermergeMemoryOptionsBuilder::default()
            .default_peer_header(NameDescription::new("creator"))
            .state_event_sender(creator_state_event_sender)
            .build()?,
    )
    .await?;
    let creator_peer_id = peermerge_creator.peer_id();
    let (creator_doc_info, _) = peermerge_creator
        .create_new_document_memory(
            CreateNewDocumentMemoryOptionsBuilder::default()
                .document_type("test".to_string())
                .document_header(NameDescription::new("proxy_test"))
                .encrypted(true)
                .build()?,
            |tx| tx.put(ROOT, "version", 1),
            None,
        )
        .await?;
    peermerge_creator
        .watch(&creator_doc_info.id(), Some(vec![ROOT]))
        .await?;
    let sharing_info = peermerge_creator
        .sharing_info(&creator_doc_info.id())
        .await?;
    let doc_url = sharing_info.read_write_doc_url;
    let proxy_doc_url = sharing_info.proxy_doc_url;
    let document_secret = peermerge_creator
        .document_secret(&creator_doc_info.id())
        .await?
        .unwrap();
    let document_info = get_document_info(&doc_url, Some(document_secret.clone()))?;
    assert_eq!(document_info.encrypted, Some(true));
    assert!(!sharing_info.proxy);

    let mut peermerge_creator_for_task = peermerge_creator.clone();
    let creator_connect = task::spawn(async move {
        peermerge_creator_for_task
            .connect_protocol_memory(&mut proto_responder)
            .await
            .unwrap();
    });

    let proxy_dir = Builder::new()
        .prefix("proxy_disk_encrypted")
        .tempdir()
        .unwrap()
        .into_path();

    let mut peermerge_proxy = Peermerge::new_disk(
        PeermergeDiskOptionsBuilder::default()
            .default_peer_header(NameDescription::new("proxy"))
            .state_event_sender(proxy_state_event_sender)
            .data_root_dir(proxy_dir.clone())
            .build()?,
    )
    .await?;
    let proxy_peer_id = peermerge_proxy.peer_id();
    peermerge_proxy
        .attach_document_disk(
            AttachDocumentDiskOptionsBuilder::default()
                .document_url(proxy_doc_url)
                .build()?,
        )
        .await?;

    let mut peermerge_proxy_for_task = peermerge_proxy.clone();
    let proxy_connect = task::spawn(async move {
        peermerge_proxy_for_task
            .connect_protocol_disk(&mut proto_initiator)
            .await
            .unwrap();
    });

    let proxy_process = task::spawn(async move {
        process_proxy_state_event_with_creator(proxy_state_event_receiver)
            .await
            .unwrap();
    });

    process_creator_state_events(
        peermerge_creator.clone(),
        creator_doc_info.id(),
        creator_state_event_receiver,
    )
    .await?;

    // Close the other side and wait for protocol handles to exit
    peermerge_creator.close().await.unwrap();
    join_all(vec![creator_connect, proxy_connect, proxy_process]).await;

    // Create an in-memory write peer, storing the write keypair in between
    let (mut proto_responder, mut proto_initiator) = create_pair_memory().await;
    let (joiner_state_event_sender, joiner_state_event_receiver): (
        UnboundedSender<StateEvent>,
        UnboundedReceiver<StateEvent>,
    ) = unbounded();
    let (proxy_state_event_sender, proxy_state_event_receiver): (
        UnboundedSender<StateEvent>,
        UnboundedReceiver<StateEvent>,
    ) = unbounded();
    let mut peermerge_joiner = Peermerge::new_memory(
        PeermergeMemoryOptionsBuilder::default()
            .default_peer_header(NameDescription::new("joiner"))
            .state_event_sender(joiner_state_event_sender)
            .build()?,
    )
    .await?;
    let joiner_peer_id = peermerge_joiner.peer_id();
    peermerge_proxy
        .set_state_event_sender(Some(proxy_state_event_sender))
        .await?;
    let joiner_doc_info = peermerge_joiner
        .attach_document_memory(
            AttachDocumentMemoryOptionsBuilder::default()
                .document_url(doc_url.clone())
                .document_secret(document_secret.clone())
                .build()?,
        )
        .await?;
    let document_id = joiner_doc_info.id();
    let reattach_secrets: HashMap<DocumentId, String> = HashMap::from([(
        document_id,
        peermerge_joiner.reattach_secret(&document_id).await?,
    )]);
    let mut peermerge_joiner_for_task = peermerge_joiner.clone();
    let joiner_connect = task::spawn(async move {
        peermerge_joiner_for_task
            .connect_protocol_memory(&mut proto_responder)
            .await
            .unwrap();
    });

    let mut peermerge_proxy_for_task = peermerge_proxy.clone();
    let proxy_connect = task::spawn(async move {
        peermerge_proxy_for_task
            .connect_protocol_disk(&mut proto_initiator)
            .await
            .unwrap();
    });

    let proxy_process = task::spawn(async move {
        process_proxy_state_event_with_joiner_initial(
            proxy_state_event_receiver,
            creator_peer_id,
            joiner_peer_id,
        )
        .await
        .unwrap();
    });

    let peermerge_joiner_for_task = peermerge_joiner.clone();
    process_joiner_state_events_initial(
        peermerge_joiner_for_task,
        joiner_doc_info.id(),
        joiner_state_event_receiver,
    )
    .await?;
    // Close the other side and wait for protocol handles to exit
    peermerge_joiner.close().await.unwrap();
    join_all(vec![joiner_connect, proxy_connect, proxy_process]).await;

    // Open again using stored keypair
    let (mut proto_responder, mut proto_initiator) = create_pair_memory().await;
    let (joiner_state_event_sender, joiner_state_event_receiver): (
        UnboundedSender<StateEvent>,
        UnboundedReceiver<StateEvent>,
    ) = unbounded();
    let (proxy_state_event_sender, proxy_state_event_receiver): (
        UnboundedSender<StateEvent>,
        UnboundedReceiver<StateEvent>,
    ) = unbounded();
    let mut peermerge_joiner = Peermerge::new_memory(
        PeermergeMemoryOptionsBuilder::default()
            .default_peer_header(NameDescription::new("joiner"))
            .state_event_sender(joiner_state_event_sender)
            .reattach_secrets(reattach_secrets)
            .build()?,
    )
    .await?;
    peermerge_proxy
        .set_state_event_sender(Some(proxy_state_event_sender))
        .await?;
    let _joiner_doc_info = peermerge_joiner
        .attach_document_memory(
            AttachDocumentMemoryOptionsBuilder::default()
                .document_url(doc_url.clone())
                .document_secret(document_secret)
                .build()?,
        )
        .await?;
    assert_eq!(peermerge_joiner.peer_id(), joiner_peer_id);
    let mut peermerge_joiner_for_task = peermerge_joiner.clone();
    let joiner_connect = task::spawn(async move {
        peermerge_joiner_for_task
            .connect_protocol_memory(&mut proto_responder)
            .await
            .unwrap();
    });

    let mut peermerge_proxy_for_task = peermerge_proxy.clone();
    let proxy_connect = task::spawn(async move {
        peermerge_proxy_for_task
            .connect_protocol_disk(&mut proto_initiator)
            .await
            .unwrap();
    });

    let proxy_process = task::spawn(async move {
        process_proxy_state_event_with_joiner_reopen(
            proxy_state_event_receiver,
            creator_peer_id,
            joiner_peer_id,
        )
        .await
        .unwrap();
    });

    let peermerge_joiner_for_task = peermerge_joiner.clone();
    process_joiner_state_events_reopen(
        peermerge_joiner_for_task,
        joiner_doc_info.id(),
        joiner_state_event_receiver,
        creator_peer_id,
        proxy_peer_id,
    )
    .await?;

    // Close the other side and wait for protocol handles to exit
    peermerge_joiner.close().await.unwrap();
    join_all(vec![joiner_connect, proxy_connect, proxy_process]).await;

    Ok(())
}

#[test(async_test)]
async fn proxy_memory_replace_feeds() -> anyhow::Result<()> {
    let (mut proto_responder, mut proto_initiator) = create_pair_memory().await;
    let (creator_state_event_sender, creator_state_event_receiver): (
        UnboundedSender<StateEvent>,
        UnboundedReceiver<StateEvent>,
    ) = unbounded();
    let (proxy_state_event_sender, proxy_state_event_receiver): (
        UnboundedSender<StateEvent>,
        UnboundedReceiver<StateEvent>,
    ) = unbounded();
    let mut peermerge_creator = Peermerge::new_memory(
        PeermergeMemoryOptionsBuilder::default()
            .default_peer_header(NameDescription::new("creator"))
            .state_event_sender(creator_state_event_sender)
            .max_write_feed_length(4)
            .build()?,
    )
    .await?;
    let (creator_doc_info, _) = peermerge_creator
        .create_new_document_memory(
            CreateNewDocumentMemoryOptionsBuilder::default()
                .document_type("test".to_string())
                .document_header(NameDescription::new("proxy_replace_test"))
                .encrypted(true)
                .build()?,
            |tx| tx.put(ROOT, "counter", ScalarValue::Counter(0.into())),
            None,
        )
        .await?;
    let document_id = creator_doc_info.id();
    let document_secret = peermerge_creator
        .document_secret(&document_id)
        .await?
        .unwrap();
    let sharing_info = peermerge_creator.sharing_info(&document_id).await?;
    let mut peermerge_creator_for_task = peermerge_creator.clone();
    task::spawn(async move {
        peermerge_creator_for_task
            .connect_protocol_memory(&mut proto_responder)
            .await
            .unwrap();
    });

    let mut peermerge_proxy = Peermerge::new_memory(
        PeermergeMemoryOptionsBuilder::default()
            .default_peer_header(NameDescription::new("proxy"))
            .state_event_sender(proxy_state_event_sender)
            .build()?,
    )
    .await?;
    peermerge_proxy
        .attach_document_memory(
            AttachDocumentMemoryOptionsBuilder::default()
                .document_url(sharing_info.proxy_doc_url)
                .build()?,
        )
        .await?;

    let mut peermerge_proxy_for_task = peermerge_proxy.clone();
    task::spawn(async move {
        peermerge_proxy_for_task
            .connect_protocol_memory(&mut proto_initiator)
            .await
            .unwrap();
    });

    task::spawn(async move {
        process_proxy_state_events_indefinitely(proxy_state_event_receiver)
            .await
            .unwrap();
    });

    let peermerge_creator_for_task = peermerge_creator.clone();
    let expected_count: u64 = 512;
    let creator_process = task::spawn(async move {
        process_state_events_feed_replace(
            peermerge_creator_for_task,
            &document_id,
            creator_state_event_receiver,
            expected_count,
        )
        .await
        .unwrap();
    });

    for _ in 0..expected_count {
        peermerge_creator
            .transact_mut(&document_id, |doc| doc.increment(ROOT, "counter", 1), None)
            .await?;
    }
    join_all(vec![creator_process]).await;

    // Test that a latecomer gets the same value out of the proxy
    // that has many replaced feeds.

    let (mut proto_responder, mut proto_initiator) = create_pair_memory().await;
    let (latecomer_state_event_sender, latecomer_state_event_receiver): (
        UnboundedSender<StateEvent>,
        UnboundedReceiver<StateEvent>,
    ) = unbounded();

    let mut peermerge_latecomer = Peermerge::new_memory(
        PeermergeMemoryOptionsBuilder::default()
            .default_peer_header(NameDescription::new("latecomer"))
            .state_event_sender(latecomer_state_event_sender)
            .build()?,
    )
    .await?;
    peermerge_latecomer
        .attach_document_memory(
            AttachDocumentMemoryOptionsBuilder::default()
                .document_url(sharing_info.read_write_doc_url)
                .document_secret(document_secret)
                .build()?,
        )
        .await?;

    task::spawn(async move {
        peermerge_proxy
            .connect_protocol_memory(&mut proto_responder)
            .await
            .unwrap();
    });

    let mut peermerge_latecomer_for_task = peermerge_latecomer.clone();
    task::spawn(async move {
        peermerge_latecomer_for_task
            .connect_protocol_memory(&mut proto_initiator)
            .await
            .unwrap();
    });

    process_state_events_feed_replace(
        peermerge_latecomer,
        &document_id,
        latecomer_state_event_receiver,
        expected_count,
    )
    .await
    .unwrap();

    Ok(())
}

#[test(async_test)]
async fn proxy_disk_replace_feeds() -> anyhow::Result<()> {
    let (mut proto_responder, mut proto_initiator) = create_pair_memory().await;
    let (creator_state_event_sender, creator_state_event_receiver): (
        UnboundedSender<StateEvent>,
        UnboundedReceiver<StateEvent>,
    ) = unbounded();
    let (proxy_state_event_sender, proxy_state_event_receiver): (
        UnboundedSender<StateEvent>,
        UnboundedReceiver<StateEvent>,
    ) = unbounded();

    let creator_dir = Builder::new()
        .prefix("creator_dir_replace")
        .tempdir()
        .unwrap()
        .into_path();

    // let debug = "target/creator_dir_replace".to_string();
    // std::fs::create_dir_all(&debug).unwrap();
    // let creator_dir = std::path::Path::new(&debug).to_path_buf();

    let mut peermerge_creator = Peermerge::new_disk(
        PeermergeDiskOptionsBuilder::default()
            .default_peer_header(NameDescription::new("creator"))
            .state_event_sender(creator_state_event_sender)
            .max_write_feed_length(4)
            .data_root_dir(creator_dir)
            .build()?,
    )
    .await?;
    let (creator_doc_info, _) = peermerge_creator
        .create_new_document_disk(
            CreateNewDocumentDiskOptionsBuilder::default()
                .document_type("test".to_string())
                .document_header(NameDescription::new("proxy_test"))
                .encrypted(true)
                .build()?,
            |tx| tx.put(ROOT, "counter", ScalarValue::Counter(0.into())),
            None,
        )
        .await?;
    let document_id = creator_doc_info.id();
    let document_secret = peermerge_creator
        .document_secret(&document_id)
        .await?
        .unwrap();
    let sharing_info = peermerge_creator.sharing_info(&document_id).await?;
    let mut peermerge_creator_for_task = peermerge_creator.clone();
    task::spawn(async move {
        peermerge_creator_for_task
            .connect_protocol_disk(&mut proto_responder)
            .await
            .unwrap();
    });

    let proxy_dir = Builder::new()
        .prefix("proxy_dir_replace")
        .tempdir()
        .unwrap()
        .into_path();

    // let debug = "target/proxy_dir_replace".to_string();
    // std::fs::create_dir_all(&debug).unwrap();
    // let proxy_dir = std::path::Path::new(&debug).to_path_buf();

    let mut peermerge_proxy = Peermerge::new_disk(
        PeermergeDiskOptionsBuilder::default()
            .default_peer_header(NameDescription::new("proxy"))
            .state_event_sender(proxy_state_event_sender)
            .data_root_dir(proxy_dir)
            .build()?,
    )
    .await?;
    peermerge_proxy
        .attach_document_disk(
            AttachDocumentDiskOptionsBuilder::default()
                .document_url(sharing_info.proxy_doc_url)
                .build()?,
        )
        .await?;

    let mut peermerge_proxy_for_task = peermerge_proxy.clone();
    task::spawn(async move {
        peermerge_proxy_for_task
            .connect_protocol_disk(&mut proto_initiator)
            .await
            .unwrap();
    });

    task::spawn(async move {
        process_proxy_state_events_indefinitely(proxy_state_event_receiver)
            .await
            .unwrap();
    });

    let peermerge_creator_for_task = peermerge_creator.clone();
    let expected_count: u64 = 32;
    let creator_process = task::spawn(async move {
        process_state_events_feed_replace(
            peermerge_creator_for_task,
            &document_id,
            creator_state_event_receiver,
            expected_count,
        )
        .await
        .unwrap();
    });

    for _ in 0..expected_count {
        peermerge_creator
            .transact_mut(&document_id, |doc| doc.increment(ROOT, "counter", 1), None)
            .await?;
    }
    join_all(vec![creator_process]).await;

    // Test that a latecomer gets the same value out of the proxy
    // that has many replaced feeds.

    let (mut proto_responder, mut proto_initiator) = create_pair_memory().await;
    let (latecomer_state_event_sender, latecomer_state_event_receiver): (
        UnboundedSender<StateEvent>,
        UnboundedReceiver<StateEvent>,
    ) = unbounded();

    let latecomer_dir = Builder::new()
        .prefix("latecomer_dir_replace")
        .tempdir()
        .unwrap()
        .into_path();

    let mut peermerge_latecomer = Peermerge::new_disk(
        PeermergeDiskOptionsBuilder::default()
            .default_peer_header(NameDescription::new("latecomer"))
            .state_event_sender(latecomer_state_event_sender)
            .data_root_dir(latecomer_dir)
            .build()?,
    )
    .await?;
    peermerge_latecomer
        .attach_document_disk(
            AttachDocumentDiskOptionsBuilder::default()
                .document_url(sharing_info.read_write_doc_url)
                .document_secret(document_secret)
                .build()?,
        )
        .await?;

    task::spawn(async move {
        peermerge_proxy
            .connect_protocol_disk(&mut proto_responder)
            .await
            .unwrap();
    });

    let mut peermerge_latecomer_for_task = peermerge_latecomer.clone();
    task::spawn(async move {
        peermerge_latecomer_for_task
            .connect_protocol_disk(&mut proto_initiator)
            .await
            .unwrap();
    });

    process_state_events_feed_replace(
        peermerge_latecomer,
        &document_id,
        latecomer_state_event_receiver,
        expected_count,
    )
    .await
    .unwrap();

    Ok(())
}

#[instrument(skip_all)]
async fn process_proxy_state_event_with_creator(
    mut proxy_state_event_receiver: UnboundedReceiver<StateEvent>,
) -> anyhow::Result<()> {
    let mut peer_syncs = 0;
    while let Some(event) = proxy_state_event_receiver.next().await {
        info!("Received event {:?}", event);
        match event.content {
            PeerSynced {
                contiguous_length, ..
            } => {
                peer_syncs += 1;
                if peer_syncs == 1 {
                    assert_eq!(contiguous_length, 1);
                } else if peer_syncs == 2 {
                    assert_eq!(contiguous_length, 2);
                    break;
                } else {
                    panic!("Too many peer syncs");
                }
            }
            _ => {
                panic!("Unexpected event {event:?}");
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
) -> anyhow::Result<()> {
    let mut document_changes: Vec<Vec<Patch>> = vec![];
    let mut remote_peer_syncs = 0;
    while let Some(event) = creator_state_event_receiver.next().await {
        info!(
            "Received event {:?}, document_changes {:?}",
            event, document_changes
        );
        match event.content {
            PeerSynced { .. } => {
                panic!("Creator should not get peer synced events {event:?}");
            }
            RemotePeerSynced {
                contiguous_length, ..
            } => {
                remote_peer_syncs += 1;
                if remote_peer_syncs == 1 {
                    assert_eq!(contiguous_length, 1);
                    peermerge
                        .transact_mut(&doc_id, |doc| doc.put(ROOT, "creator", "testing"), None)
                        .await?;
                } else if remote_peer_syncs == 2 {
                    assert_eq!(contiguous_length, 2);
                    assert_eq!(document_changes.len(), 2);
                    break;
                }
            }
            DocumentChanged { patches, .. } => {
                document_changes.push(patches);
            }
            DocumentInitialized { .. } => {
                // Skip
            }
            PeerChanged { .. } => {
                // Skip
            }
        }
    }
    Ok(())
}

#[instrument(skip_all)]
async fn process_proxy_state_event_with_joiner_initial(
    mut proxy_state_event_receiver: UnboundedReceiver<StateEvent>,
    creator_peer_id: PeerId,
    joiner_peer_id: PeerId,
) -> anyhow::Result<()> {
    let mut remote_peer_syncs = 0;
    while let Some(event) = proxy_state_event_receiver.next().await {
        info!("Received event {:?}", event);
        match event.content {
            PeerSynced {
                peer_id,
                contiguous_length,
                ..
            } => {
                if peer_id == creator_peer_id {
                    info!("Creator feed synced to {contiguous_length}");
                } else if peer_id == joiner_peer_id {
                    info!("Joiner feed synced to {contiguous_length}");
                }
                if contiguous_length == 2 {
                    // Joiner's own write feed init and one change.
                    break;
                } else if contiguous_length > 2 {
                    panic!("Too large continuous length");
                }
            }
            RemotePeerSynced {
                contiguous_length, ..
            } => {
                remote_peer_syncs += 1;
                if remote_peer_syncs == 1 {
                    assert_eq!(contiguous_length, 2);
                } else {
                    panic!("Too many remote peer syncs");
                }
            }
            PeerChanged { .. } => {
                // Skip
            }
            _ => {
                panic!("Unexpected event {event:?}");
            }
        }
    }
    Ok(())
}

#[instrument(skip_all)]
async fn process_joiner_state_events_initial(
    mut peermerge: Peermerge<RandomAccessMemory, FeedMemoryPersistence>,
    doc_id: DocumentId,
    mut joiner_state_event_receiver: UnboundedReceiver<StateEvent>,
) -> anyhow::Result<()> {
    let mut document_changes: Vec<Vec<Patch>> = vec![];
    let mut peer_syncs = 0;
    let mut creator_synced = false;
    let mut document_initialized = false;
    while let Some(event) = joiner_state_event_receiver.next().await {
        info!(
            "Received event {:?}, document_changes {:?}",
            event, document_changes
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
                peer_syncs += 1;
                // The first one is the creators write feed
                assert_eq!(name, "creator");
                if peer_syncs == 1 {
                    assert_eq!(contiguous_length, 2);
                    creator_synced = true;
                    if document_initialized {
                        peermerge
                            .transact_mut(&doc_id, |doc| doc.put(ROOT, "joiner", "testing"), None)
                            .await?;
                    }
                } else {
                    panic!("Too many peer syncs");
                }
            }
            RemotePeerSynced {
                contiguous_length, ..
            } => {
                // Only remote peer it can sync is its own writer
                if contiguous_length == 2 {
                    break;
                }
            }
            DocumentChanged { patches, .. } => {
                document_changes.push(patches);
            }
            DocumentInitialized { .. } => {
                document_initialized = true;
                if creator_synced {
                    peermerge
                        .transact_mut(&doc_id, |doc| doc.put(ROOT, "joiner", "testing"), None)
                        .await?;
                }
            }
            PeerChanged { .. } => {
                // Skip
            }
        }
    }
    Ok(())
}

#[instrument(skip_all)]
async fn process_proxy_state_event_with_joiner_reopen(
    mut proxy_state_event_receiver: UnboundedReceiver<StateEvent>,
    creator_peer_id: PeerId,
    joiner_peer_id: PeerId,
) -> anyhow::Result<()> {
    let mut remote_peer_syncs = 0;
    while let Some(event) = proxy_state_event_receiver.next().await {
        info!("Received event {:?}", event);
        match event.content {
            PeerSynced {
                peer_id,
                contiguous_length,
                ..
            } => {
                if peer_id == creator_peer_id {
                    info!("Creator feed synced to {contiguous_length}");
                } else if peer_id == joiner_peer_id {
                    info!("Joiner feed synced to {contiguous_length}");
                }
                if contiguous_length == 3 {
                    // Joiners own write feed with init, original change and reopen change
                    break;
                } else if contiguous_length > 3 {
                    panic!("Too large contiguous length");
                }
            }
            RemotePeerSynced {
                contiguous_length, ..
            } => {
                remote_peer_syncs += 1;
                if remote_peer_syncs == 1 {
                    assert_eq!(contiguous_length, 2);
                } else if remote_peer_syncs == 2 {
                    assert_eq!(contiguous_length, 2);
                } else {
                    panic!("Too many remote peer syncs");
                }
            }
            _ => {
                panic!("Unexpected event {event:?}");
            }
        }
    }
    Ok(())
}

#[instrument(skip_all)]
async fn process_joiner_state_events_reopen(
    mut peermerge: Peermerge<RandomAccessMemory, FeedMemoryPersistence>,
    doc_id: DocumentId,
    mut joiner_state_event_receiver: UnboundedReceiver<StateEvent>,
    creator_peer_id: PeerId,
    proxy_peer_id: PeerId,
) -> anyhow::Result<()> {
    let mut document_changes: Vec<Vec<Patch>> = vec![];
    let mut full_peer_syncs = 0;
    let mut creator_and_joiner_synced = false;
    let mut document_initialized = false;
    while let Some(event) = joiner_state_event_receiver.next().await {
        info!("Received event {event:?}, document_changes {document_changes:?}");
        match event.content {
            PeerSynced {
                peer_id,
                contiguous_length,
                ..
            } => {
                if peer_id == creator_peer_id {
                    info!("Creator feed synced to {contiguous_length}");
                } else if peer_id == proxy_peer_id {
                    info!("Proxy feed synced to {contiguous_length}");
                }
                // There are two, creator and joiner itself, both have init and one change
                if contiguous_length == 2 {
                    full_peer_syncs += 1;
                }
                if full_peer_syncs == 2 {
                    creator_and_joiner_synced = true;
                    if document_initialized {
                        let value = peermerge
                            .transact(&doc_id, |doc| get_scalar(doc, ROOT, "creator"))
                            .await?
                            .unwrap();
                        assert_eq!(value.to_str().unwrap(), "testing");
                        let value = peermerge
                            .transact(&doc_id, |doc| get_scalar(doc, ROOT, "joiner"))
                            .await?
                            .unwrap();
                        assert_eq!(value.to_str().unwrap(), "testing");
                        peermerge
                            .transact_mut(
                                &doc_id,
                                |doc| doc.put(ROOT, "joiner_reopen", "testing"),
                                None,
                            )
                            .await?;
                    }
                }
            }
            DocumentChanged { patches, .. } => {
                document_changes.push(patches);
            }
            DocumentInitialized { .. } => {
                document_initialized = true;
                if creator_and_joiner_synced {
                    let value = peermerge
                        .transact(&doc_id, |doc| get_scalar(doc, ROOT, "creator"))
                        .await?
                        .unwrap();
                    assert_eq!(value.to_str().unwrap(), "testing");
                    let value = peermerge
                        .transact(&doc_id, |doc| get_scalar(doc, ROOT, "joiner"))
                        .await?
                        .unwrap();
                    assert_eq!(value.to_str().unwrap(), "testing");
                    peermerge
                        .transact_mut(
                            &doc_id,
                            |doc| doc.put(ROOT, "joiner_reopen", "testing"),
                            None,
                        )
                        .await?;
                }
            }
            PeerChanged { .. } => {
                // Ignore
            }
            RemotePeerSynced {
                contiguous_length, ..
            } => {
                if contiguous_length == 3 {
                    break;
                } else if contiguous_length > 3 {
                    panic!("Too large contiguous length")
                }
            }
        }
    }
    Ok(())
}

#[instrument(skip_all)]
async fn process_proxy_state_events_indefinitely(
    mut proxy_state_event_receiver: UnboundedReceiver<StateEvent>,
) -> anyhow::Result<()> {
    while let Some(event) = proxy_state_event_receiver.next().await {
        info!("Received proxy event {:?}", event);
    }
    Ok(())
}

#[instrument(skip_all)]
async fn process_state_events_feed_replace<T, U>(
    peermerge: Peermerge<T, U>,
    document_id: &DocumentId,
    mut state_event_receiver: UnboundedReceiver<StateEvent>,
    expected_count: u64,
) -> anyhow::Result<()>
where
    T: RandomAccess + Debug + Send + 'static,
    U: FeedPersistence,
{
    while let Some(event) = state_event_receiver.next().await {
        info!("Received event {event:?}");

        if matches!(
            event.content,
            DocumentInitialized { .. } | DocumentChanged { .. }
        ) {
            let value = peermerge
                .transact(document_id, |doc| {
                    let (value, _) = get(doc, ROOT, "counter")?.unwrap();
                    Ok(value.into_scalar().unwrap().to_u64().unwrap())
                })
                .await?;
            if value == expected_count {
                break;
            }
        }
    }
    Ok(())
}
