//! Common module to contain types and structs needed
//! for communication between automerge and hypercore.
pub(crate) mod cipher;
pub(crate) mod constants;
pub(crate) mod encoding;
pub(crate) mod entry;
mod error;
mod event;
pub(crate) mod keys;
pub(crate) mod message;
pub(crate) mod state;
pub(crate) mod storage;
mod types;
pub(crate) mod utils;

pub use cipher::get_doc_url_info;
pub use error::PeermergeError;
pub use event::{PeerEvent, PeerEventContent, StateEvent, StateEventContent};
pub use types::{DocUrlInfo, DocumentInfo, DocumentSharingInfo, FeedType, NameDescription};
