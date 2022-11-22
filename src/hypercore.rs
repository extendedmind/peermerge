//! Hypercore-specific types and helpers

mod common;
mod messaging;
mod peer;
mod persistence;
#[cfg(not(target_arch = "wasm32"))]
mod tcp;
mod wrapper;

use common::PeerState;
use messaging::on_message;
use peer::on_peer;

#[cfg(not(target_arch = "wasm32"))]
pub(crate) use persistence::{create_new_read_disk_hypercore, create_new_write_disk_hypercore};
pub(crate) use persistence::{
    create_new_read_memory_hypercore, create_new_write_memory_hypercore,
    discovery_key_from_public_key, generate_keys,
};
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use tcp::{tcp_client, tcp_server};
pub(crate) use wrapper::HypercoreWrapper;
