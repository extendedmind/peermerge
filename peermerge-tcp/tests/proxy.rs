use std::collections::HashMap;

use futures::channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use futures::stream::StreamExt;
use peermerge::transaction::Transactable;
use peermerge::{get_doc_url_info, DocumentId};
use peermerge::{DiskPeermergeOptionsBuilder, ROOT};
use peermerge::{
    FeedDiskPersistence, Peermerge, RandomAccessDisk, StateEvent, StateEventContent::*,
};
use peermerge::{NameDescription, Patch};
use peermerge_tcp::{connect_tcp_client_disk, connect_tcp_server_disk};
use tempfile::Builder;
use test_log::test;
use tracing::{info, instrument};

#[cfg(feature = "async-std")]
use async_std::{task, test as async_test};
#[cfg(feature = "tokio")]
use tokio::{task, test as async_test};

#[test(async_test)]
async fn tcp_proxy_disk_encrypted() -> anyhow::Result<()> {
    let host = "localhost";
    let port: u16 = 8101;
    let (creator_state_event_sender, creator_state_event_receiver): (
        UnboundedSender<StateEvent>,
        UnboundedReceiver<StateEvent>,
    ) = unbounded();
    let (proxy_state_event_sender, proxy_state_event_receiver): (
        UnboundedSender<StateEvent>,
        UnboundedReceiver<StateEvent>,
    ) = unbounded();
    let creator_dir = Builder::new()
        .prefix("creator_disk_encrypted")
        .tempdir()
        .unwrap()
        .into_path();
    let mut peermerge_creator = Peermerge::create_new_disk(
        DiskPeermergeOptionsBuilder::default()
            .default_peer_header(NameDescription::new("creator"))
            .state_event_sender(creator_state_event_sender)
            .data_root_dir(creator_dir.clone())
            .build()?,
    )
    .await;
    let (creator_doc_info, _) = peermerge_creator
        .create_new_document_disk(
            "test",
            Some(NameDescription::new("tcp_proxy_test")),
            true,
            |tx| tx.put(ROOT, "version", 1),
        )
        .await?;

    peermerge_creator
        .watch(&creator_doc_info.id(), Some(vec![ROOT]))
        .await;
    let doc_url = peermerge_creator
        .sharing_info(&creator_doc_info.id())
        .await?
        .doc_url
        .unwrap();
    let encryption_key = peermerge_creator
        .encryption_key(&creator_doc_info.id())
        .await;
    assert_eq!(get_doc_url_info(&doc_url)?.encrypted, Some(true));
    assert!(encryption_key.is_some());

    let proxy_dir = Builder::new()
        .prefix("proxy_disk_encrypted")
        .tempdir()
        .unwrap()
        .into_path();

    let mut peermerge_proxy = Peermerge::create_new_disk(
        DiskPeermergeOptionsBuilder::default()
            .default_peer_header(NameDescription::new("proxy"))
            .state_event_sender(proxy_state_event_sender)
            .data_root_dir(proxy_dir.clone())
            .build()?,
    )
    .await;
    let peermerge_proxy_for_task = peermerge_proxy.clone();
    task::spawn(async move {
        connect_tcp_server_disk(peermerge_proxy_for_task, host, port)
            .await
            .unwrap();
    });

    // Reopen peermerge_creator
    drop(peermerge_creator);
    let mut creator_encryption_keys = HashMap::new();
    if let Some(encryption_key) = encryption_key.as_ref() {
        creator_encryption_keys.insert(creator_doc_info.id(), encryption_key.clone());
    }
    let peermerge_creator =
        Peermerge::open_disk(creator_encryption_keys, &creator_dir, None).await?;

    // Delay attaching proxy document until after server above has been started.
    let _proxy_doc_id = peermerge_proxy.attach_proxy_document_disk(&doc_url).await?;

    // Now ready to start client
    let peermerge_creator_for_task = peermerge_creator.clone();
    task::spawn(async move {
        connect_tcp_client_disk(peermerge_creator_for_task, host, port)
            .await
            .unwrap();
    });

    task::spawn(async move {
        process_proxy_state_event(proxy_state_event_receiver)
            .await
            .unwrap();
    });

    process_creator_state_events(
        peermerge_creator,
        creator_doc_info.id(),
        creator_state_event_receiver,
    )
    .await?;

    Ok(())
}

#[instrument(skip_all)]
async fn process_proxy_state_event(
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
                } else {
                    panic!("Too many peer syncs");
                }
            }
            RemotePeerSynced { .. } => {
                panic!("Should not get remote peer synced events {event:?}");
            }
            DocumentChanged { .. } => {
                panic!("Should not get document changed event {event:?}");
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
    mut peermerge: Peermerge<RandomAccessDisk, FeedDiskPersistence>,
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
                panic!("Should not get remote peer synced events {event:?}");
            }
            DocumentInitialized { .. } => {
                // Skip
            }
            RemotePeerSynced {
                contiguous_length, ..
            } => {
                remote_peer_syncs += 1;
                if remote_peer_syncs == 1 {
                    assert_eq!(contiguous_length, 1);
                    peermerge
                        .transact_mut(&doc_id, |doc| doc.put(ROOT, "test", "value"), None)
                        .await?;
                } else if remote_peer_syncs == 2 {
                    assert_eq!(contiguous_length, 2);
                    assert_eq!(document_changes.len(), 1);
                    break;
                }
            }
            DocumentChanged { patches, .. } => {
                document_changes.push(patches);
            }
            Reattached { .. } => {
                panic!("Should not get reattached");
            }
        }
    }
    Ok(())
}
