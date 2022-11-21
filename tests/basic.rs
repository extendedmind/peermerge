#![allow(dead_code, unused_imports)]

use async_std::net::TcpStream;
use async_std::prelude::*;
use async_std::task;
use futures_lite::io::{AsyncRead, AsyncWrite};
use hypercore_protocol::{discovery_key, Channel, Event, Message, Protocol, ProtocolBuilder};
use hypercore_protocol::{schema::*, DiscoveryKey};
use hypermerge::Repo;
use std::io;

mod _util;
use _util::*;

#[async_std::test]
async fn basic_two_writers() -> anyhow::Result<()> {
    let (mut proto_responder, mut proto_initiator) = create_pair_memory().await?;

    let mut repo_creator = Repo::new_memory().await;
    let mut repo_joiner = Repo::new_memory().await;

    let doc_url = repo_creator.create_doc_memory(vec![("version", 1)]).await;
    let doc_url_for_responder = doc_url.clone();
    task::spawn(async move {
        repo_creator
            .connect(&doc_url_for_responder, &mut proto_responder)
            .await
            .expect("Connect should not thorw error");
    });
    repo_joiner.register_doc_memory(&doc_url).await;
    repo_joiner
        .connect(&doc_url, &mut proto_initiator)
        .await
        .unwrap();

    Ok(())
}
