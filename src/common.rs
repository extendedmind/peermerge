//! Common module to contain types and structs needed
//! for communication between automerge and hypercore.
pub(crate) mod encoding;
mod event;
pub(crate) mod message;
pub(crate) mod state;
pub(crate) mod storage;

pub use event::{StateEvent, SynchronizeEvent};
