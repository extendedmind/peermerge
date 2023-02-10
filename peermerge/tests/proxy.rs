use automerge::ROOT;
use futures::channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
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
    let encryption_key = peermerge_creator.encryption_key(&creator_doc_id).await;
    assert_eq!(get_doc_url_info(&doc_url).encrypted, Some(true));
    assert_eq!(encryption_key.is_some(), true);

    let mut peermerge_creator_for_task = peermerge_creator.clone();
    task::spawn(async move {
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
    let _proxy_doc_id = peermerge_proxy.attach_proxy_document_disk(&doc_url).await;
    let mut peermerge_proxy_for_task = peermerge_proxy.clone();
    task::spawn(async move {
        peermerge_proxy_for_task
            .connect_protocol_disk(&mut proto_initiator, &mut proxy_state_event_sender)
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
        creator_doc_id,
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
            PeerSynced((None, _, len)) => {
                peer_syncs += 1;
                if peer_syncs == 1 {
                    assert_eq!(len, 1);
                } else if peer_syncs == 2 {
                    assert_eq!(len, 2);
                } else {
                    panic!("Too many peer syncs");
                }
            }
            RemotePeerSynced(_) => {
                panic!("Should not get remote peer synced events {:?}", event);
            }
            DocumentChanged(_) => {
                panic!("Should not get document changed event {:?}", event);
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
                panic!("Should not get remote peer synced events {:?}", event);
            }
            RemotePeerSynced((discovery_key, len)) => {
                if discovery_key != doc_id {
                    remote_peer_syncs += 1;
                    if remote_peer_syncs == 1 {
                        assert_eq!(len, 1);
                        peermerge.put_scalar(&doc_id, ROOT, "test", "value").await?;
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
        }
    }
    Ok(())
}
