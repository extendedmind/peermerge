// First approximation:
//
// 1. PeerA creates a repo and a doc1 to it => docId1 (=hypercore publicKey where the first message
//    is an initial automerge save())
// 2. PeerB creates a repo, and uses docId1 to create a channel to PeerA
// 3. PeerB syncs all data from the doc1.
// 4. PeerB creates a write hypercore for doc1 and sends an HypermergeMsg with the public key.
// 5. PeerA creates a read-only hypercore for PeerB's hypercore.

mod automerge;
mod common;
mod core;
mod hypercore;
mod repository;

pub use crate::common::StateEvent;
pub use crate::core::Hypermerge;
pub use crate::repository::Repository;
pub use ::automerge::*;
