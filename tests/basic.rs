#![allow(dead_code, unused_imports)]

use async_channel::{unbounded, Receiver, Sender};
use async_std::net::TcpStream;
use async_std::prelude::*;
use async_std::task;
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
    let (creator_state_event_sender, creator_state_event_receiver): (
        Sender<StateEvent>,
        Receiver<StateEvent>,
    ) = unbounded();
    let mut repo_joiner = Repo::new_memory().await;
    let (joiner_state_event_sender, joiner_state_event_receiver): (
        Sender<StateEvent>,
        Receiver<StateEvent>,
    ) = unbounded();
    let doc_url = repo_creator.create_doc_memory(vec![("version", 1)]).await;

    // Set watching for the same props
    repo_creator.watch_root_props(&doc_url, vec!["text"]).await;
    repo_joiner.watch_root_props(&doc_url, vec!["text"]).await;

    let doc_url_for_task = doc_url.clone();
    task::spawn(async move {
        connect_repo(
            repo_creator,
            proto_responder,
            doc_url_for_task,
            joiner_state_event_sender,
        )
        .await
        .unwrap();
    });

    let doc_url_for_task = doc_url.clone();
    task::spawn(async move {
        repo_joiner.register_doc_memory(&doc_url_for_task).await;
        connect_repo(
            repo_joiner,
            proto_initiator,
            doc_url_for_task,
            creator_state_event_sender,
        )
        .await
        .unwrap();
    });

    let mut combined =
        futures_lite::stream::race(creator_state_event_receiver, joiner_state_event_receiver);
    while let Some(event) = combined.next().await {
        // TODO: Assert state events
    }
    Ok(())
}

async fn connect_repo(
    mut repo: Repo<RandomAccessMemory>,
    mut protocol: MemoryProtocol,
    doc_url: String,
    state_event_sender: Sender<StateEvent>,
) -> anyhow::Result<()> {
    let (mut sync_event_sender, mut sync_event_receiver): (
        Sender<SynchronizeEvent>,
        Receiver<SynchronizeEvent>,
    ) = unbounded();
    let doc_url_for_task = doc_url.clone();
    let mut repo_for_task = repo.clone();
    task::spawn(async move {
        repo_for_task
            .connect_document(
                &doc_url_for_task,
                state_event_sender,
                &mut sync_event_receiver,
            )
            .await
            .expect("Connect should not thorw error");
    });
    repo.connect_protocol(&doc_url, &mut protocol, &mut sync_event_sender)
        .await?;
    Ok(())
}
