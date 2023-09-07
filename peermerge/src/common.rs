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
pub(crate) use event::{FeedEvent, FeedEventContent};

pub use cipher::{decode_base64_nopad, encode_base64_nopad, get_document_info, new_uuid_v4};
pub use error::PeermergeError;
pub use event::{StateEvent, StateEventContent};
pub use types::{
    AccessType, DocumentInfo, DocumentSharingInfo, DynamicDocumentInfo, FeedType, NameDescription,
    StaticDocumentInfo, UrlDocumentInfo,
};
