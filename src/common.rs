//! Common module to contain types and structs needed
//! for communication between automerge and hypercore.
mod event;
pub(crate) mod state;

pub use event::{StateEvent, SynchronizeEvent};
