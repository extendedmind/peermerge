use automerge::transaction::Transactable;
use automerge::ROOT;
use futures::channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use futures::future::join_all;
use futures::stream::StreamExt;
use peermerge::{
    get_doc_url_info, DocumentId, FeedMemoryPersistence, NameDescription, Patch, Peermerge,
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
        NameDescription::new("creator"),
        Some(creator_state_event_sender),
    )
    .await;
    let (creator_doc_info, _) = peermerge_creator
        .create_new_document_memory(
            "test",
            Some(NameDescription::new("proxy_test")),
            true,
            |tx| tx.put(ROOT, "version", 1),
        )
        .await?;
    peermerge_creator
        .watch(&creator_doc_info.id(), Some(vec![ROOT]))
        .await;
    let sharing_info = peermerge_creator
        .sharing_info(&creator_doc_info.id())
        .await?;
    let doc_url = sharing_info.doc_url.unwrap();
    let proxy_doc_url = sharing_info.proxy_doc_url;
    let encryption_key = peermerge_creator
        .encryption_key(&creator_doc_info.id())
        .await;
    let doc_url_info = get_doc_url_info(&doc_url);
    assert_eq!(doc_url_info.encrypted, Some(true));
    assert!(encryption_key.is_some());
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
        NameDescription::new("proxy"),
        Some(proxy_state_event_sender),
        &proxy_dir,
    )
    .await;
    let proxy_doc_info = peermerge_proxy
        .attach_proxy_document_disk(&proxy_doc_url)
        .await;
    let sharing_info = peermerge_proxy.sharing_info(&proxy_doc_info.id()).await?;
    assert!(sharing_info.proxy);
    assert!(sharing_info.doc_url.is_none());

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
        NameDescription::new("joiner"),
        Some(joiner_state_event_sender),
    )
    .await;
    peermerge_proxy
        .set_state_event_sender(Some(proxy_state_event_sender))
        .await;
    let joiner_doc_info = peermerge_joiner
        .attach_writer_document_memory(&doc_url, &encryption_key)
        .await?;
    let write_key_pair: String = peermerge_joiner.write_key_pair(&joiner_doc_info.id()).await;
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
        NameDescription::new("joiner"),
        Some(joiner_state_event_sender),
    )
    .await;
    peermerge_proxy
        .set_state_event_sender(Some(proxy_state_event_sender))
        .await;
    let _joiner_doc_info = peermerge_joiner
        .reattach_writer_document_memory(&doc_url, &encryption_key, &write_key_pair)
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
            PeerSynced((_, _, len)) => {
                peer_syncs += 1;
                if peer_syncs == 1 {
                    assert_eq!(len, 1);
                } else if peer_syncs == 2 {
                    assert_eq!(len, 2);
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
            PeerSynced(_) => {
                panic!("Creator should not get peer synced events {event:?}");
            }
            RemotePeerSynced((_, _, len)) => {
                remote_peer_syncs += 1;
                if remote_peer_syncs == 1 {
                    assert_eq!(len, 1);
                    peermerge
                        .transact_mut(&doc_id, |doc| doc.put(ROOT, "creator", "testing"), None)
                        .await?;
                } else if remote_peer_syncs == 2 {
                    assert_eq!(len, 2);
                    assert_eq!(document_changes.len(), 1);
                    break;
                }
            }
            DocumentChanged((_, patches)) => {
                document_changes.push(patches);
            }
            DocumentInitialized(..) => {
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
            PeerSynced((_, _, len)) => {
                peer_syncs += 1;
                if peer_syncs == 1 {
                    // The first one that will come is joiners own write feed with init
                    assert_eq!(len, 1);
                } else if peer_syncs == 2 {
                    // ..after that the same but now with a change.
                    assert_eq!(len, 2);
                    break;
                } else {
                    panic!("Too many peer syncs");
                }
            }
            RemotePeerSynced((_, _, len)) => {
                remote_peer_syncs += 1;
                if remote_peer_syncs == 1 {
                    assert_eq!(len, 2);
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
            PeerSynced((peer_id, _, len)) => {
                let name = peermerge
                    .peer_header(&event.document_id, &peer_id)
                    .await
                    .unwrap()
                    .name;
                peer_syncs += 1;
                // The first one is the creators write feed
                assert_eq!(name, "creator");
                if peer_syncs == 1 {
                    assert_eq!(len, 2);
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
            RemotePeerSynced((_, _, len)) => {
                // Only remote peer it can sync is its own writer
                if len == 2 {
                    break;
                }
            }
            DocumentChanged((_, patches)) => {
                document_changes.push(patches);
            }
            DocumentInitialized(..) => {
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
            PeerSynced((_, _, len)) => {
                peer_syncs += 1;
                if peer_syncs == 1 {
                    // The first one that will come is joiners own write feed with init, original
                    // change and reopen change
                    assert_eq!(len, 3);
                    break;
                } else {
                    panic!("Too many peer syncs");
                }
            }
            RemotePeerSynced((_, _, len)) => {
                remote_peer_syncs += 1;
                if remote_peer_syncs == 1 {
                    assert_eq!(len, 2);
                } else if remote_peer_syncs == 2 {
                    assert_eq!(len, 2);
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
            PeerSynced((_, _, len)) => {
                // There are two, creator and joiner itself, both have init and one change
                if len == 2 {
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
            DocumentChanged((_, patches)) => {
                document_changes.push(patches);
            }
            DocumentInitialized(..) => {
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
            Reattached(peer_header) => {
                assert_eq!(peer_header, NameDescription::new("joiner"))
            }
            RemotePeerSynced((_, _, len)) => {
                assert_eq!(len, 3);
                break;
            }
        }
    }
    Ok(())
}
