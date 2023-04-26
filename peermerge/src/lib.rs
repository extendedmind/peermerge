mod automerge;
mod common;
mod core;
mod document;
mod feed;

// Custom types and traits
pub type DocumentId = [u8; 32];
use futures::{AsyncRead, AsyncWrite};
pub trait IO: AsyncWrite + AsyncRead + Send + Unpin + 'static {}
impl<T> IO for T where T: AsyncWrite + AsyncRead + Send + Unpin + 'static {}

// Crate exports
pub use crate::common::{
    get_doc_url_info, DocUrlInfo, DocumentInfo, FeedType, NameDescription, PeermergeError,
    StateEvent, StateEventContent,
};
pub use crate::core::Peermerge;
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
