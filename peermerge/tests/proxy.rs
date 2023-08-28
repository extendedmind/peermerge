use automerge::transaction::Transactable;
use automerge::ROOT;
use futures::channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use futures::future::join_all;
use futures::stream::StreamExt;
use peermerge::{
    get_doc_url_info, CreateNewDocumentOptionsBuilder, DiskPeermergeOptionsBuilder, DocumentId,
    FeedMemoryPersistence, MemoryPeermergeOptionsBuilder, NameDescription, Patch, Peermerge,
    StateEvent, StateEventContent::*,
};
use random_access_memory::RandomAccessMemory;
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
        MemoryPeermergeOptionsBuilder::default()
            .default_peer_header(NameDescription::new("creator"))
            .state_event_sender(creator_state_event_sender)
            .build()?,
    )
    .await;
    let (creator_doc_info, _) = peermerge_creator
        .create_new_document_memory(
            CreateNewDocumentOptionsBuilder::default()
                .document_type("test".to_string())
                .document_header(NameDescription::new("proxy_test"))
                .encrypted(true)
                .build()?,
            |tx| tx.put(ROOT, "version", 1),
        )
        .await?;
    peermerge_creator
        .watch(&creator_doc_info.id(), Some(vec![ROOT]))
        .await;
    let sharing_info = peermerge_creator
        .sharing_info(&creator_doc_info.id())
        .await?;
    let doc_url = sharing_info.doc_url;
    let proxy_doc_url = sharing_info.proxy_doc_url;
    let document_secret = peermerge_creator
        .document_secret(&creator_doc_info.id())
        .await
        .unwrap();
    let doc_url_info = get_doc_url_info(&doc_url)?;
    assert_eq!(doc_url_info.encrypted, Some(true));
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

    let mut peermerge_proxy = Peermerge::create_new_disk(
        DiskPeermergeOptionsBuilder::default()
            .default_peer_header(NameDescription::new("proxy"))
            .state_event_sender(proxy_state_event_sender)
            .data_root_dir(proxy_dir.clone())
            .build()?,
    )
    .await;
    peermerge_proxy
        .attach_proxy_document_disk(&proxy_doc_url)
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
        MemoryPeermergeOptionsBuilder::default()
            .default_peer_header(NameDescription::new("joiner"))
            .state_event_sender(joiner_state_event_sender)
            .build()?,
    )
    .await;
    peermerge_proxy
        .set_state_event_sender(Some(proxy_state_event_sender))
        .await;
    let joiner_doc_info = peermerge_joiner
        .attach_writer_document_memory(&doc_url, &document_secret)
        .await?;
    let reattach_secret: String = peermerge_joiner
        .reattach_secret(&joiner_doc_info.id())
        .await;
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
        process_proxy_state_event_with_joiner_initial(proxy_state_event_receiver)
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
        MemoryPeermergeOptionsBuilder::default()
            .default_peer_header(NameDescription::new("joiner"))
            .state_event_sender(joiner_state_event_sender)
            .build()?,
    )
    .await;
    peermerge_proxy
        .set_state_event_sender(Some(proxy_state_event_sender))
        .await;
    let _joiner_doc_info = peermerge_joiner
        .reattach_writer_document_memory(&doc_url, &document_secret, &reattach_secret)
        .await?;
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
        process_proxy_state_event_with_joiner_reopen(proxy_state_event_receiver)
            .await
            .unwrap();
    });

    let peermerge_joiner_for_task = peermerge_joiner.clone();
    process_joiner_state_events_reopen(
        peermerge_joiner_for_task,
        joiner_doc_info.id(),
        joiner_state_event_receiver,
    )
    .await?;

    // Close the other side and wait for protocol handles to exit
    peermerge_joiner.close().await.unwrap();
    join_all(vec![joiner_connect, proxy_connect, proxy_process]).await;

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
                    assert_eq!(document_changes.len(), 1);
                    break;
                }
            }
            DocumentChanged { patches, .. } => {
                document_changes.push(patches);
            }
            DocumentInitialized { .. } => {
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
async fn process_proxy_state_event_with_joiner_initial(
    mut proxy_state_event_receiver: UnboundedReceiver<StateEvent>,
) -> anyhow::Result<()> {
    let mut peer_syncs = 0;
    let mut remote_peer_syncs = 0;
    while let Some(event) = proxy_state_event_receiver.next().await {
        info!("Received event {:?}", event);
        match event.content {
            PeerSynced {
                contiguous_length, ..
            } => {
                peer_syncs += 1;
                if peer_syncs == 1 {
                    // The first one that will come is joiners own write feed with init
                    assert_eq!(contiguous_length, 1);
                } else if peer_syncs == 2 {
                    // ..after that the same but now with a change.
                    assert_eq!(contiguous_length, 2);
                    break;
                } else {
                    panic!("Too many peer syncs");
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
                    .await
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
            _ => {
                panic!("Unexpected event {event:?}");
            }
        }
    }
    Ok(())
}

#[instrument(skip_all)]
async fn process_proxy_state_event_with_joiner_reopen(
    mut proxy_state_event_receiver: UnboundedReceiver<StateEvent>,
) -> anyhow::Result<()> {
    let mut peer_syncs = 0;
    let mut remote_peer_syncs = 0;
    while let Some(event) = proxy_state_event_receiver.next().await {
        info!("Received event {:?}", event);
        match event.content {
            PeerSynced {
                contiguous_length, ..
            } => {
                peer_syncs += 1;
                if peer_syncs == 1 {
                    // The first one that will come is joiners own write feed with init, original
                    // change and reopen change
                    assert_eq!(contiguous_length, 3);
                    break;
                } else {
                    panic!("Too many peer syncs");
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
                panic!("Unexpected event {:?}", event);
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
) -> anyhow::Result<()> {
    let mut document_changes: Vec<Vec<Patch>> = vec![];
    let mut full_peer_syncs = 0;
    let mut creator_and_joiner_synced = false;
    let mut document_initialized = false;
    while let Some(event) = joiner_state_event_receiver.next().await {
        info!(
            "Received event {:?}, document_changes {:?}",
            event, document_changes
        );
        match event.content {
            PeerSynced {
                contiguous_length, ..
            } => {
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
                assert_eq!(contiguous_length, 3);
                break;
            }
        }
    }
    Ok(())
}
