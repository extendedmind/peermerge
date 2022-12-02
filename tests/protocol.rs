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
use hypermerge::Hypermerge;
use hypermerge::StateEvent;
use hypermerge::SynchronizeEvent;
use random_access_memory::RandomAccessMemory;
use std::io;

mod common;
use common::*;

#[async_std::test]
async fn protocol_two_writers() -> anyhow::Result<()> {
    let (proto_responder, proto_initiator) = create_pair_memory().await?;

    let (creator_state_event_sender, mut creator_state_event_receiver): (
        Sender<StateEvent>,
        Receiver<StateEvent>,
    ) = unbounded();
    let (joiner_state_event_sender, mut joiner_state_event_receiver): (
        Sender<StateEvent>,
        Receiver<StateEvent>,
    ) = unbounded();
    let mut hypermerge_creator = Hypermerge::create_doc_memory(vec![("version", 1)]).await;

    // Insert a map with a text field to the document
    let texts_id = hypermerge_creator
        .put_object(ROOT, "texts", automerge::ObjType::Map)
        .await
        .unwrap();
    hypermerge_creator
        .put_object(&texts_id, "text", automerge::ObjType::Text)
        .await
        .unwrap();

    // Set watching for the prop
    hypermerge_creator.watch(vec![texts_id]).await;

    let hypermerge_creator_for_task = hypermerge_creator.clone();
    task::spawn(async move {
        connect(
            hypermerge_creator_for_task,
            proto_responder,
            creator_state_event_sender,
        )
        .await
        .unwrap();
    });

    let hypermerge_joiner = Hypermerge::register_doc_memory(&hypermerge_creator.doc_url()).await;
    let hypermerge_joiner_for_task = hypermerge_joiner.clone();
    task::spawn(async move {
        connect(
            hypermerge_joiner_for_task,
            proto_initiator,
            joiner_state_event_sender,
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
                    let (value, id) = hypermerge_joiner.get(ROOT, "texts").await.unwrap().unwrap();
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

async fn connect(
    mut hypermerge: Hypermerge<RandomAccessMemory>,
    mut protocol: MemoryProtocol,
    state_event_sender: Sender<StateEvent>,
) -> anyhow::Result<()> {
    let (mut sync_event_sender, mut sync_event_receiver): (
        Sender<SynchronizeEvent>,
        Receiver<SynchronizeEvent>,
    ) = unbounded();
    let mut hypermerge_for_task = hypermerge.clone();
    task::spawn(async move {
        hypermerge_for_task
            .connect_document(state_event_sender, &mut sync_event_receiver)
            .await
            .expect("Connect should not thorw error");
    });
    hypermerge
        .connect_protocol(&mut protocol, &mut sync_event_sender)
        .await?;
    Ok(())
}
