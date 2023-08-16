mod automerge;
mod builder;
mod common;
mod core;
mod document;
mod feed;

#[macro_use]
extern crate derive_builder;

// Custom types and traits
pub type DocumentId = [u8; 32];
pub type PeerId = [u8; 16];
use futures::{AsyncRead, AsyncWrite};
pub trait IO: AsyncWrite + AsyncRead + Send + Unpin + 'static {}
impl<T> IO for T where T: AsyncWrite + AsyncRead + Send + Unpin + 'static {}

// Crate exports
pub use crate::automerge::AutomergeDoc;
pub use crate::common::{
    decode_base64_nopad, encode_base64_nopad, get_doc_url_info, new_uuid_v4, DocUrlInfo,
    DocumentInfo, DocumentSharingInfo, FeedType, NameDescription, PeermergeError, StateEvent,
    StateEventContent,
};
pub use crate::core::Peermerge;
pub use builder::*;
#[cfg(not(target_arch = "wasm32"))]
pub use feed::FeedDiskPersistence;
pub use feed::{FeedMemoryPersistence, FeedPersistence};
pub use feed::{Protocol, ProtocolBuilder};

// Related crates' re-exports
pub use ::automerge::*; // TODO: Enumerate what's actually needed
#[cfg(not(target_arch = "wasm32"))]
pub use random_access_disk::RandomAccessDisk;
pub use random_access_memory::RandomAccessMemory;
pub use random_access_storage::RandomAccess;
pub use uuid;
