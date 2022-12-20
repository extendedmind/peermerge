//! Hypercore-specific types and helpers

mod common;
mod messaging;
mod peer;
mod persistence;
mod protocol;
#[cfg(not(target_arch = "wasm32"))]
mod tcp;
mod wrapper;

use common::PeerState;
use messaging::on_message;
use peer::{on_doc_peer, on_peer};

#[cfg(not(target_arch = "wasm32"))]
pub(crate) use persistence::{
    create_new_read_disk_hypercore, create_new_write_disk_hypercore, get_path_from_discovery_key,
};
pub(crate) use persistence::{
    create_new_read_memory_hypercore, create_new_write_memory_hypercore,
    discovery_key_from_public_key, generate_keys, keys_from_public_key,
};
pub(crate) use protocol::on_protocol;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use tcp::{tcp_client, tcp_server};
pub(crate) use wrapper::HypercoreWrapper;
