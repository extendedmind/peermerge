//! Hypercore-specific types and helpers

mod common;
mod feed;
mod messaging;
mod persistence;
mod protocol;
mod wrapper;

use common::PeerState;
use feed::{on_doc_feed, on_feed};
use messaging::on_message;

#[cfg(not(target_arch = "wasm32"))]
pub(crate) use persistence::{
    create_new_read_disk_hypercore, create_new_write_disk_hypercore, open_disk_hypercore,
};
pub(crate) use persistence::{create_new_read_memory_hypercore, create_new_write_memory_hypercore};
pub(crate) use protocol::on_protocol;
pub(crate) use wrapper::HypercoreWrapper;
