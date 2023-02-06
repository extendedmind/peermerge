use automerge::ROOT;
use futures::channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use futures::stream::StreamExt;
use peermerge::doc_url_encrypted;
use peermerge::Patch;
use peermerge::Peermerge;
use peermerge::StateEvent;
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
    let mut peermerge_creator =
        Peermerge::create_new_memory("creator", vec![("version", 1)], true).await;
    peermerge_creator.watch(vec![ROOT]).await;
    let doc_url = peermerge_creator.doc_url();
    let encryption_key = peermerge_creator.encryption_key();
    assert_eq!(doc_url_encrypted(&doc_url), true);
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

    let peermerge_proxy = Peermerge::attach_proxy_disk("proxy", &doc_url, &proxy_dir).await;
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

    process_creator_state_events(peermerge_creator, creator_state_event_receiver).await?;

    Ok(())
}

#[instrument(skip_all)]
async fn process_proxy_state_event(
    mut proxy_state_event_receiver: UnboundedReceiver<StateEvent>,
) -> anyhow::Result<()> {
    let mut peer_syncs = 0;
    while let Some(event) = proxy_state_event_receiver.next().await {
        info!("Received event {:?}", event);
        match event {
            StateEvent::PeerSynced((None, _, len)) => {
                peer_syncs += 1;
                if peer_syncs == 1 {
                    assert_eq!(len, 1);
                } else if peer_syncs == 2 {
                    assert_eq!(len, 2);
                } else {
                    panic!("Too many peer syncs");
                }
            }
            StateEvent::RemotePeerSynced(_) => {
                panic!("Should not get remote peer synced events {:?}", event);
            }
            StateEvent::DocumentChanged(_) => {
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
    mut peermerge: Peermerge<RandomAccessMemory>,
    mut creator_state_event_receiver: UnboundedReceiver<StateEvent>,
) -> anyhow::Result<()> {
    let mut document_changes: Vec<Vec<Patch>> = vec![];
    let mut remote_peer_syncs = 0;
    while let Some(event) = creator_state_event_receiver.next().await {
        info!(
            "Received event {:?}, document_changes {:?}",
            event, document_changes
        );
        match event {
            StateEvent::PeerSynced(_) => {
                panic!("Should not get remote peer synced events {:?}", event);
            }
            StateEvent::RemotePeerSynced((_, len)) => {
                remote_peer_syncs += 1;
                if remote_peer_syncs == 1 {
                    assert_eq!(len, 1);
                    peermerge.put_scalar(ROOT, "test", "value").await?;
                } else if remote_peer_syncs == 2 {
                    assert_eq!(len, 2);
                    assert_eq!(document_changes.len(), 1);
                    break;
                }
            }
            StateEvent::DocumentChanged(patches) => {
                document_changes.push(patches);
            }
        }
    }
    Ok(())
}
