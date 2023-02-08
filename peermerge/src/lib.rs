mod automerge;
mod common;
mod core;
mod document;
mod feed;

pub use crate::common::cipher::doc_url_encrypted;
pub use crate::common::{StateEvent, StateEventContent};
pub use crate::core::Peermerge;
pub use ::automerge::*;
pub use feed::{Protocol, ProtocolBuilder};
use futures::{AsyncRead, AsyncWrite};
#[cfg(not(target_arch = "wasm32"))]
pub use random_access_disk::RandomAccessDisk;
pub use random_access_memory::RandomAccessMemory;
pub use random_access_storage::RandomAccess;
pub trait IO: AsyncWrite + AsyncRead + Send + Unpin + 'static {}
impl<T> IO for T where T: AsyncWrite + AsyncRead + Send + Unpin + 'static {}
#[cfg(not(target_arch = "wasm32"))]
pub use feed::FeedDiskPersistence;
pub use feed::{FeedMemoryPersistence, FeedPersistence};

pub type DocumentId = [u8; 32];
