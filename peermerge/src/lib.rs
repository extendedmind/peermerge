mod autocommit;
mod common;
mod core;
mod document;
mod feeds;
mod options;

#[macro_use]
extern crate derive_builder;

// Custom types and traits
pub type DocumentId = [u8; 32];
pub type PeerId = [u8; 16];
use futures::{AsyncRead, AsyncWrite};
pub trait IO: AsyncWrite + AsyncRead + Send + Unpin + 'static {}
impl<T> IO for T where T: AsyncWrite + AsyncRead + Send + Unpin + 'static {}

// Crate exports
pub use crate::autocommit::AutomergeDoc;
pub use crate::common::{
    decode_base64_nopad, encode_base64_nopad, get_document_info, new_uuid_v4, AccessType,
    DocumentInfo, DocumentSharingInfo, DynamicDocumentInfo, FeedType, NameDescription,
    PeermergeError, StateEvent, StateEventContent, StaticDocumentInfo,
};
pub use crate::core::Peermerge;
#[cfg(not(target_arch = "wasm32"))]
pub use feeds::FeedDiskPersistence;
pub use feeds::{FeedMemoryPersistence, FeedPersistence};
pub use feeds::{Protocol, ProtocolBuilder};
pub use options::*;

// Related crates' re-exports
pub use automerge;
#[cfg(not(target_arch = "wasm32"))]
pub use random_access_disk::RandomAccessDisk;
pub use random_access_memory::RandomAccessMemory;
pub use random_access_storage::RandomAccess;
pub use uuid;
