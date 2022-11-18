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
async fn open_close_channels() -> anyhow::Result<()> {
    let (mut proto_a, mut proto_b) = create_pair_memory().await?;

    let mut repo_a = Repo::new_memory().await;
    let doc_url = repo_a.create_doc_memory(vec![("version", 1)]).await;

    let mut repo_b = Repo::new_memory().await;
    repo_a.connect(&doc_url, &mut proto_a).await;
    repo_b.connect(&doc_url, &mut proto_b).await;

    Ok(())
}
