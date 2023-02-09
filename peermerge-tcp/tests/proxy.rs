use futures::channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use futures::stream::StreamExt;
use peermerge::Patch;
use peermerge::Peermerge;
use peermerge::ROOT;
use peermerge::{doc_url_encrypted, DocumentId};
use peermerge::{FeedMemoryPersistence, StateEvent, StateEventContent::*};
use peermerge_tcp::{connect_tcp_client_disk, connect_tcp_server_memory};
use random_access_memory::RandomAccessMemory;
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
    let port: u32 = 8101;
    let (mut creator_state_event_sender, creator_state_event_receiver): (
        UnboundedSender<StateEvent>,
        UnboundedReceiver<StateEvent>,
    ) = unbounded();
    let (mut proxy_state_event_sender, proxy_state_event_receiver): (
        UnboundedSender<StateEvent>,
        UnboundedReceiver<StateEvent>,
    ) = unbounded();
    let mut peermerge_creator = Peermerge::new_memory("creator").await;
    let creator_doc_id = peermerge_creator
        .create_new_document_memory("proxy_test", vec![("version", 1)], true)
        .await;

    peermerge_creator.watch(&creator_doc_id, vec![ROOT]).await;
    let doc_url = peermerge_creator.doc_url(&creator_doc_id);
    let encryption_key = peermerge_creator.encryption_key(&creator_doc_id);
    assert_eq!(doc_url_encrypted(&doc_url), true);
    assert_eq!(encryption_key.is_some(), true);

    let peermerge_creator_for_task = peermerge_creator.clone();
    task::spawn(async move {
        connect_tcp_server_memory(
            peermerge_creator_for_task,
            host,
            port,
            &mut creator_state_event_sender,
        )
        .await
        .unwrap();
    });

    let proxy_dir = Builder::new()
        .prefix("proxy_disk_encrypted")
        .tempdir()
        .unwrap()
        .into_path();

    let mut peermerge_proxy = Peermerge::create_new_disk("proxy", &proxy_dir).await;
    let _proxy_doc_id = peermerge_proxy.attach_proxy_document_disk(&doc_url).await;
    let peermerge_proxy_for_task = peermerge_proxy.clone();
    task::spawn(async move {
        connect_tcp_client_disk(
            peermerge_proxy_for_task,
            host,
            port,
            &mut proxy_state_event_sender,
        )
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
            RemotePeerSynced((_, len)) => {
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
            DocumentChanged(patches) => {
                document_changes.push(patches);
            }
        }
    }
    Ok(())
}
