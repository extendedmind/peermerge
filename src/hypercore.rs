//! Hypercore-specific types and helpers

mod messaging;
mod peer;
mod persistence;
mod tcp;
mod wrapper;

use messaging::on_message;
use peer::PeerState;
pub(crate) use persistence::{
    create_new_read_disk_hypercore, create_new_read_memory_hypercore,
    create_new_write_disk_hypercore, create_new_write_memory_hypercore,
    discovery_key_from_public_key, generate_keys,
};
pub(crate) use tcp::{tcp_client, tcp_server};
pub(crate) use wrapper::HypercoreWrapper;
