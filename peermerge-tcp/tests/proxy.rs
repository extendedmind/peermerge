use std::collections::HashMap;

use futures::channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use futures::stream::StreamExt;
use peermerge::transaction::Transactable;
use peermerge::{get_document_info, DiskCreateNewDocumentOptionsBuilder, DocumentId};
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
            DiskCreateNewDocumentOptionsBuilder::default()
                .document_type("test".to_string())
                .document_header(NameDescription::new("tcp_proxy_test"))
                .encrypted(true)
                .build()?,
            |tx| tx.put(ROOT, "version", 1),
        )
        .await?;

    peermerge_creator
        .watch(&creator_doc_info.id(), Some(vec![ROOT]))
        .await;
    let doc_url = peermerge_creator
        .sharing_info(&creator_doc_info.id())
        .await?
        .doc_url;
    let document_secret = peermerge_creator
        .document_secret(&creator_doc_info.id())
        .await
        .unwrap();
    assert_eq!(
        get_document_info(&doc_url, Some(document_secret.clone()))?.encrypted,
        Some(true)
    );
    let proxy_dir = Builder::new()
        .prefix("tcp_proxy_disk_encrypted")
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
    let mut creator_document_secrets = HashMap::new();
    creator_document_secrets.insert(creator_doc_info.id(), document_secret.clone());
    let peermerge_creator =
        Peermerge::open_disk(creator_document_secrets, &creator_dir, None).await?;

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
            PeerChanged { .. } => {
                // Ignore
            }
        }
    }
    Ok(())
}
