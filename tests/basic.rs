#![allow(dead_code, unused_imports)]

use async_channel::{unbounded, Receiver, Sender};
use async_std::net::TcpStream;
use async_std::prelude::*;
use async_std::task;
use automerge::ObjId;
use automerge::ROOT;
use futures_lite::io::{AsyncRead, AsyncWrite};
use futures_lite::stream::StreamExt;
use hypercore_protocol::{discovery_key, Channel, Event, Message, Protocol, ProtocolBuilder};
use hypercore_protocol::{schema::*, DiscoveryKey};
use hypermerge::Repo;
use hypermerge::StateEvent;
use hypermerge::SynchronizeEvent;
use random_access_memory::RandomAccessMemory;
use std::io;

mod common;
use common::*;

#[async_std::test]
async fn basic_two_writers() -> anyhow::Result<()> {
    let (proto_responder, proto_initiator) = create_pair_memory().await?;

    let mut repo_creator = Repo::new_memory().await;
    let (creator_state_event_sender, mut creator_state_event_receiver): (
        Sender<StateEvent>,
        Receiver<StateEvent>,
    ) = unbounded();
    let mut repo_joiner = Repo::new_memory().await;
    let (joiner_state_event_sender, mut joiner_state_event_receiver): (
        Sender<StateEvent>,
        Receiver<StateEvent>,
    ) = unbounded();
    let (discovery_key, doc_url) = repo_creator.create_doc_memory(vec![("version", 1)]).await;

    // Insert a map with a text field to the document
    let texts_id = repo_creator
        .put_object(&discovery_key, ROOT, "texts", automerge::ObjType::Map)
        .await
        .unwrap();
    repo_creator
        .put_object(&discovery_key, &texts_id, "text", automerge::ObjType::Text)
        .await
        .unwrap();

    // Set watching for the prop
    repo_creator.watch(&discovery_key, vec![&texts_id]).await;

    let repo_creator_for_task = repo_creator.clone();
    task::spawn(async move {
        connect_repo(
            repo_creator_for_task,
            proto_responder,
            &discovery_key,
            joiner_state_event_sender,
        )
        .await
        .unwrap();
    });

    let joiner_discovery_key = repo_joiner.register_doc_memory(&doc_url).await;
    let joiner_discovery_key_for_task = joiner_discovery_key.clone();
    let repo_joiner_for_task = repo_joiner.clone();
    task::spawn(async move {
        connect_repo(
            repo_joiner_for_task,
            proto_initiator,
            &joiner_discovery_key_for_task,
            creator_state_event_sender,
        )
        .await
        .unwrap();
    });

    // Simulate UI threads here
    task::spawn(async move {
        let mut peers_synced = false;
        let mut texts_id: Option<ObjId> = None;
        while let Some(event) = joiner_state_event_receiver.next().await {
            println!("TEST: JOINER got event {:?}", event);
            match event {
                StateEvent::PeersSynced(len) => {
                    assert!(!peers_synced);
                    assert_eq!(len, 1);
                    peers_synced = true;
                    println!("TEST: JOINER calling get()");
                    let (value, id) = repo_joiner
                        .get(&joiner_discovery_key, ROOT, "texts")
                        .await
                        .unwrap()
                        .unwrap();
                    println!("TEST: JOINER VALUE: {:?}", value);
                    texts_id = Some(id);
                }
                StateEvent::DocumentChanged(_) => {}
            }
        }
    });

    while let Some(event) = creator_state_event_receiver.next().await {
        println!("TEST: CREATOR got event {:?}", event);
        let mut peers_synced = false;
        match event {
            StateEvent::PeersSynced(len) => {
                assert!(!peers_synced);
                assert_eq!(len, 1);
                peers_synced = true;
            }
            StateEvent::DocumentChanged(_) => {}
        }
    }
    Ok(())
}

async fn connect_repo(
    mut repo: Repo<RandomAccessMemory>,
    mut protocol: MemoryProtocol,
    discovery_key: &[u8; 32],
    state_event_sender: Sender<StateEvent>,
) -> anyhow::Result<()> {
    let (mut sync_event_sender, mut sync_event_receiver): (
        Sender<SynchronizeEvent>,
        Receiver<SynchronizeEvent>,
    ) = unbounded();
    let discovery_key_for_task = discovery_key.clone();
    let mut repo_for_task = repo.clone();
    task::spawn(async move {
        repo_for_task
            .connect_document(
                discovery_key_for_task,
                state_event_sender,
                &mut sync_event_receiver,
            )
            .await
            .expect("Connect should not thorw error");
    });
    repo.connect_protocol(discovery_key, &mut protocol, &mut sync_event_sender)
        .await?;
    Ok(())
}
