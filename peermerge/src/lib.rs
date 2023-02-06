mod automerge;
mod common;
mod core;
mod feed;
mod repository;

pub use crate::common::cipher::doc_url_encrypted;
pub use crate::common::{StateEvent, StateEventContent};
pub use crate::core::Peermerge;
pub use crate::repository::PeermergeRepository;
pub use ::automerge::*;
pub use feed::{Protocol, ProtocolBuilder};
#[cfg(not(target_arch = "wasm32"))]
pub use random_access_disk::RandomAccessDisk;
pub use random_access_memory::RandomAccessMemory;
pub use random_access_storage::RandomAccess;
