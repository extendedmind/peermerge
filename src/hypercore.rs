//! Hypercore-specific types and helpers

mod common;
mod messaging;
mod peer;
mod persistence;
mod protocol;
mod wrapper;

use common::PeerState;
use messaging::on_message;
use peer::{on_doc_peer, on_peer};

pub(crate) use persistence::{
    create_new_read_memory_hypercore, create_new_write_memory_hypercore,
    discovery_key_from_public_key, generate_keys,
};
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use persistence::{create_new_write_disk_hypercore, open_read_disk_hypercore};
pub(crate) use protocol::on_protocol;
pub(crate) use wrapper::HypercoreWrapper;
