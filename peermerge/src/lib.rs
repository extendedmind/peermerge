mod automerge;
mod common;
mod core;
mod hypercore;
mod repository;

pub use crate::common::cipher::doc_url_encrypted;
pub use crate::common::StateEvent;
pub use crate::core::Peermerge;
pub use crate::repository::PeermergeRepository;
pub use ::automerge::*;
pub use hypercore_protocol::{Protocol, ProtocolBuilder};
#[cfg(not(target_arch = "wasm32"))]
pub use random_access_disk::RandomAccessDisk;
pub use random_access_memory::RandomAccessMemory;
pub use random_access_storage::RandomAccess;
