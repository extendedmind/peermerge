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

mod common;
use common::*;

#[test(async_test)]
async fn proxy_disk_encrypted() -> anyhow::Result<()> {
    let (mut proto_responder, mut proto_initiator) = create_pair_memory().await;
    let (mut creator_state_event_sender, creator_state_event_receiver): (
        UnboundedSender<StateEvent>,
        UnboundedReceiver<StateEvent>,
    ) = unbounded();
    let (mut proxy_state_event_sender, proxy_state_event_receiver): (
        UnboundedSender<StateEvent>,
        UnboundedReceiver<StateEvent>,
    ) = unbounded();
    let mut peermerge_creator = Peermerge::new_memory(NameDescription::new("creator")).await;
    let creator_doc_id = peermerge_creator
        .create_new_document_memory(
            NameDescription::new("proxy_test"),
            vec![("version", 1)],
            true,
        )
        .await;
    peermerge_creator.watch(&creator_doc_id, vec![ROOT]).await;
    let doc_url = peermerge_creator.doc_url(&creator_doc_id).await;
    let proxy_doc_url = peermerge_creator.proxy_doc_url(&creator_doc_id).await;
    let encryption_key = peermerge_creator.encryption_key(&creator_doc_id).await;
    assert_eq!(get_doc_url_info(&doc_url).encrypted, Some(true));
    assert_eq!(encryption_key.is_some(), true);

    let mut peermerge_creator_for_task = peermerge_creator.clone();
    let creator_connect = task::spawn(async move {
        peermerge_creator_for_task
            .connect_protocol_memory(&mut proto_responder, &mut creator_state_event_sender)
            .await
            .unwrap();
    });

    let proxy_dir = Builder::new()
        .prefix("proxy_disk_encrypted")
        .tempdir()
        .unwrap()
        .into_path();

    let mut peermerge_proxy =
        Peermerge::create_new_disk(NameDescription::new("proxy"), &proxy_dir).await;
    let _proxy_doc_id = peermerge_proxy
        .attach_proxy_document_disk(&proxy_doc_url)
        .await;
    let mut peermerge_proxy_for_task = peermerge_proxy.clone();
    let proxy_connect = task::spawn(async move {
        peermerge_proxy_for_task
            .connect_protocol_disk(&mut proto_initiator, &mut proxy_state_event_sender)
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
        creator_doc_id,
        creator_state_event_receiver,
    )
    .await?;

    // Close the other side and wait for protocol handles to exit
    peermerge_creator.close().await.unwrap();
    join_all(vec![creator_connect, proxy_connect, proxy_process]).await;

    // Create an in-memory write peer, storing the write keypair in between
    let (mut proto_responder, mut proto_initiator) = create_pair_memory().await;
    let (mut joiner_state_event_sender, joiner_state_event_receiver): (
        UnboundedSender<StateEvent>,
        UnboundedReceiver<StateEvent>,
    ) = unbounded();
    let (mut proxy_state_event_sender, proxy_state_event_receiver): (
        UnboundedSender<StateEvent>,
        UnboundedReceiver<StateEvent>,
    ) = unbounded();
    let mut peermerge_joiner = Peermerge::new_memory(NameDescription::new("joiner")).await;
    let joiner_doc_id = peermerge_joiner
        .attach_writer_document_memory(&doc_url, &encryption_key)
        .await;
    let write_key_pair: String = peermerge_joiner.write_key_pair(&joiner_doc_id).await;
    let mut peermerge_joiner_for_task = peermerge_joiner.clone();
    let joiner_connect = task::spawn(async move {
        peermerge_joiner_for_task
            .connect_protocol_memory(&mut proto_responder, &mut joiner_state_event_sender)
            .await
            .unwrap();
    });

    let mut peermerge_proxy_for_task = peermerge_proxy.clone();
    let proxy_connect = task::spawn(async move {
        peermerge_proxy_for_task
            .connect_protocol_disk(&mut proto_initiator, &mut proxy_state_event_sender)
            .await
            .unwrap();
    });

    let joiner_doc_id_for_task = joiner_doc_id.clone();
    let proxy_process = task::spawn(async move {
        process_proxy_state_event_with_joiner_initial(
            joiner_doc_id_for_task,
            proxy_state_event_receiver,
        )
        .await
        .unwrap();
    });

    let peermerge_joiner_for_task = peermerge_joiner.clone();
    process_joiner_state_events_initial(
        peermerge_joiner_for_task,
        joiner_doc_id,
        joiner_state_event_receiver,
    )
    .await?;

    // Close the other side and wait for protocol handles to exit
    peermerge_joiner.close().await.unwrap();
    join_all(vec![joiner_connect, proxy_connect, proxy_process]).await;

    // Open again using stored keypair
    let (mut proto_responder, mut proto_initiator) = create_pair_memory().await;
    let (mut joiner_state_event_sender, joiner_state_event_receiver): (
        UnboundedSender<StateEvent>,
        UnboundedReceiver<StateEvent>,
    ) = unbounded();
    let (mut proxy_state_event_sender, proxy_state_event_receiver): (
        UnboundedSender<StateEvent>,
        UnboundedReceiver<StateEvent>,
    ) = unbounded();
    let mut peermerge_joiner = Peermerge::new_memory(NameDescription::new("joiner")).await;
    let joiner_doc_id = peermerge_joiner
        .reattach_writer_document_memory(&write_key_pair, "joiner", &doc_url, &encryption_key)
        .await;
    let mut peermerge_joiner_for_task = peermerge_joiner.clone();
    let joiner_connect = task::spawn(async move {
        peermerge_joiner_for_task
            .connect_protocol_memory(&mut proto_responder, &mut joiner_state_event_sender)
            .await
            .unwrap();
    });

    let mut peermerge_proxy_for_task = peermerge_proxy.clone();
    let proxy_connect = task::spawn(async move {
        peermerge_proxy_for_task
            .connect_protocol_disk(&mut proto_initiator, &mut proxy_state_event_sender)
            .await
            .unwrap();
    });

    let joiner_doc_id_for_task = joiner_doc_id.clone();
    let proxy_process = task::spawn(async move {
        process_proxy_state_event_with_joiner_reopen(
            joiner_doc_id_for_task,
            proxy_state_event_receiver,
        )
        .await
        .unwrap();
    });

    let peermerge_joiner_for_task = peermerge_joiner.clone();
    process_joiner_state_events_reopen(
        peermerge_joiner_for_task,
        joiner_doc_id,
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
            PeerSynced((None, _, len)) => {
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
                panic!("Unexpected event {:?}", event);
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
                panic!("Creator should not get peer synced events {:?}", event);
            }
            RemotePeerSynced((discovery_key, len)) => {
                if discovery_key != doc_id {
                    remote_peer_syncs += 1;
                    if remote_peer_syncs == 1 {
                        assert_eq!(len, 1);
                        peermerge
                            .put_scalar(&doc_id, ROOT, "creator", "testing")
                            .await?;
                    } else if remote_peer_syncs == 2 {
                        assert_eq!(len, 2);
                        assert_eq!(document_changes.len(), 1);
                        break;
                    }
                }
            }
            DocumentChanged(patches) => {
                document_changes.push(patches);
            }
            _ => {
                panic!("Unexpected event {:?}", event);
            }
        }
    }
    Ok(())
}

#[instrument(skip_all)]
async fn process_proxy_state_event_with_joiner_initial(
    doc_id: DocumentId,
    mut proxy_state_event_receiver: UnboundedReceiver<StateEvent>,
) -> anyhow::Result<()> {
    let mut peer_syncs = 0;
    let mut remote_peer_syncs = 0;
    while let Some(event) = proxy_state_event_receiver.next().await {
        info!("Received event {:?}", event);
        match event.content {
            PeerSynced((None, _, len)) => {
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
            RemotePeerSynced((discovery_key, len)) => {
                if discovery_key != doc_id {
                    remote_peer_syncs += 1;
                    if remote_peer_syncs == 1 {
                        assert_eq!(len, 2);
                    } else {
                        panic!("Too many remote peer syncs");
                    }
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
            PeerSynced((name, discovery_key, len)) => {
                if discovery_key != doc_id {
                    peer_syncs += 1;
                    // The first one is the creators write feed
                    assert_eq!(name, Some("creator".to_string()));
                    if peer_syncs == 1 {
                        assert_eq!(len, 2);
                        creator_synced = true;
                        if document_initialized {
                            peermerge
                                .put_scalar(&doc_id, ROOT, "joiner", "testing")
                                .await?;
                        }
                    } else {
                        panic!("Too many peer syncs");
                    }
                } else {
                    assert_eq!(len, 1);
                }
            }
            RemotePeerSynced((discovery_key, len)) => {
                // Only remote peer it can sync is its own writer
                if len == 2 {
                    break;
                }
            }
            DocumentChanged(patches) => {
                document_changes.push(patches);
            }
            DocumentInitialized() => {
                document_initialized = true;
                if creator_synced {
                    peermerge
                        .put_scalar(&doc_id, ROOT, "joiner", "testing")
                        .await?;
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
async fn process_proxy_state_event_with_joiner_reopen(
    doc_id: DocumentId,
    mut proxy_state_event_receiver: UnboundedReceiver<StateEvent>,
) -> anyhow::Result<()> {
    let mut peer_syncs = 0;
    let mut remote_peer_syncs = 0;
    while let Some(event) = proxy_state_event_receiver.next().await {
        info!("Received event {:?}", event);
        match event.content {
            PeerSynced((None, _, len)) => {
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
            RemotePeerSynced((discovery_key, len)) => {
                if discovery_key != doc_id {
                    remote_peer_syncs += 1;
                    if remote_peer_syncs == 1 {
                        assert_eq!(len, 2);
                    } else if remote_peer_syncs == 2 {
                        assert_eq!(len, 2);
                    } else {
                        panic!("Too many remote peer syncs");
                    }
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
            PeerSynced((_, discovery_key, len)) => {
                if discovery_key != doc_id {
                    // There are two, creator and joiner itself, both have init and one change
                    if len == 2 {
                        full_peer_syncs += 1;
                    }
                    if full_peer_syncs == 2 {
                        creator_and_joiner_synced = true;
                        if document_initialized {
                            let value = peermerge
                                .get_scalar(&doc_id, ROOT, "creator")
                                .await?
                                .unwrap();
                            assert_eq!(value.to_str().unwrap(), "testing");
                            let value = peermerge
                                .get_scalar(&doc_id, ROOT, "joiner")
                                .await?
                                .unwrap();
                            assert_eq!(value.to_str().unwrap(), "testing");
                            peermerge
                                .put_scalar(&doc_id, ROOT, "joiner_reopen", "testing")
                                .await?;
                        }
                    }
                } else {
                    assert_eq!(len, 1);
                }
            }
            DocumentChanged(patches) => {
                document_changes.push(patches);
            }
            DocumentInitialized() => {
                document_initialized = true;
                if creator_and_joiner_synced {
                    let value = peermerge
                        .get_scalar(&doc_id, ROOT, "creator")
                        .await?
                        .unwrap();
                    assert_eq!(value.to_str().unwrap(), "testing");
                    let value = peermerge
                        .get_scalar(&doc_id, ROOT, "joiner")
                        .await?
                        .unwrap();
                    assert_eq!(value.to_str().unwrap(), "testing");
                    peermerge
                        .put_scalar(&doc_id, ROOT, "joiner_reopen", "testing")
                        .await?;
                }
            }
            Reattached(peer_header) => {
                assert_eq!(peer_header, NameDescription::new("joiner"))
            }
            RemotePeerSynced((_, len)) => {
                assert_eq!(len, 3);
                break;
            }
        }
    }
    Ok(())
}
