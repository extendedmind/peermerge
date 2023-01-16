#![allow(dead_code, unused_imports)]

use async_std::net::TcpStream;
use async_std::prelude::*;
use async_std::sync::{Arc, Condvar, Mutex};
use async_std::task;
use automerge::ObjId;
use automerge::ROOT;
use futures::channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use futures::io::{AsyncRead, AsyncWrite};
use futures::stream::StreamExt;
use hypercore_protocol::{discovery_key, Channel, Event, Message, Protocol, ProtocolBuilder};
use hypercore_protocol::{schema::*, DiscoveryKey};
use hypermerge::Hypermerge;
use hypermerge::Patch;
use hypermerge::StateEvent;
use hypermerge::Value;
use random_access_disk::RandomAccessDisk;
use random_access_memory::RandomAccessMemory;
use std::collections::HashMap;
use std::io;
use std::time::Duration;
use tempfile::Builder;
use test_log::test;
use tracing::{info, instrument};

mod common;
use common::*;

#[derive(Clone, Debug, Default)]
struct ProtocolThreeWritersResult {
    joiner_merge: Option<String>,
    creator_merge: Option<String>,
}
impl ProtocolThreeWritersResult {
    pub fn merge_equals(&self) -> bool {
        self.joiner_merge.as_ref().unwrap() == self.creator_merge.as_ref().unwrap()
    }
}

#[test(async_std::test)]
async fn disk_two_peers_plain() -> anyhow::Result<()> {
    disk_two_peers(false).await
}

async fn disk_two_peers(encrypted: bool) -> anyhow::Result<()> {
    let (mut proto_responder, mut proto_initiator) = create_pair_memory().await;
    let creator_dir = Builder::new()
        .prefix(&format!(
            "disk_two_peers_creator_{}",
            if encrypted { "encrypted" } else { "plain" }
        ))
        .tempdir()
        .unwrap()
        .into_path();

    let (mut creator_state_event_sender, creator_state_event_receiver): (
        UnboundedSender<StateEvent>,
        UnboundedReceiver<StateEvent>,
    ) = unbounded();
    let (mut joiner_state_event_sender, joiner_state_event_receiver): (
        UnboundedSender<StateEvent>,
        UnboundedReceiver<StateEvent>,
    ) = unbounded();
    let hypermerge_creator =
        Hypermerge::create_doc_disk("creator", vec![("version", 1)], creator_dir).await;

    let mut hypermerge_creator_for_task = hypermerge_creator.clone();
    task::spawn(async move {
        hypermerge_creator_for_task
            .connect_protocol_disk(&mut proto_responder, &mut creator_state_event_sender)
            .await
            .unwrap();
    });

    let joiner_dir = Builder::new()
        .prefix(&format!(
            "disk_two_peers_joiner_{}",
            if encrypted { "encrypted" } else { "plain" }
        ))
        .tempdir()
        .unwrap()
        .into_path();
    let hypermerge_joiner =
        Hypermerge::register_doc_disk("joiner", &hypermerge_creator.doc_url(), joiner_dir).await;
    let mut hypermerge_joiner_for_task = hypermerge_joiner.clone();
    task::spawn(async move {
        hypermerge_joiner_for_task
            .connect_protocol_disk(&mut proto_initiator, &mut joiner_state_event_sender)
            .await
            .unwrap();
    });

    task::spawn(async move {
        process_joiner_state_event(hypermerge_joiner, joiner_state_event_receiver)
            .await
            .unwrap();
    });

    process_creator_state_events(hypermerge_creator, creator_state_event_receiver).await?;

    Ok(())
}

#[instrument(skip_all)]
async fn process_joiner_state_event(
    hypermerge: Hypermerge<RandomAccessDisk>,
    mut joiner_state_event_receiver: UnboundedReceiver<StateEvent>,
) -> anyhow::Result<()> {
    let mut document_changes: Vec<Vec<Patch>> = vec![];
    while let Some(event) = joiner_state_event_receiver.next().await {
        info!(
            "Received event {:?}, document_changes {:?}",
            event, document_changes
        );
        match event {
            StateEvent::PeerSynced((name, _, len)) => {
                assert_eq!(name, "creator");
                assert_eq!(len, 1);
                let (value, _) = hypermerge.get(ROOT, "version").await?.unwrap();
                assert_eq!(value.to_u64().unwrap(), 1);
            }
            StateEvent::RemotePeerSynced(_) => {}
            StateEvent::DocumentChanged(patches) => {
                document_changes.push(patches);
                break;
            }
        }
    }
    Ok(())
}

#[instrument(skip_all)]
async fn process_creator_state_events(
    hypermerge: Hypermerge<RandomAccessDisk>,
    mut creator_state_event_receiver: UnboundedReceiver<StateEvent>,
) -> anyhow::Result<()> {
    let mut document_changes: Vec<Vec<Patch>> = vec![];
    while let Some(event) = creator_state_event_receiver.next().await {
        info!(
            "Received event {:?}, document_changes {:?}",
            event, document_changes
        );
        match event {
            StateEvent::PeerSynced((name, _, len)) => {
                assert_eq!(name, "joiner");
                assert_eq!(len, 1);
                let (value, _) = hypermerge.get(ROOT, "version").await?.unwrap();
                assert_eq!(value.to_u64().unwrap(), 1);
                break;
            }
            StateEvent::RemotePeerSynced(_) => {}
            StateEvent::DocumentChanged(patches) => {
                document_changes.push(patches);
            }
        }
    }
    Ok(())
}
