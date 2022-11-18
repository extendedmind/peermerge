//! Hypercore-specific types and helpers

mod messaging;
mod peer;
mod persistence;
mod tcp;
mod wrapper;

use messaging::on_message;
use peer::PeerState;
pub(crate) use persistence::{
    create_new_disk_hypercore, create_new_memory_hypercore, generate_keys,
};
pub(crate) use tcp::{tcp_client, tcp_server};
pub(crate) use wrapper::HypercoreWrapper;
