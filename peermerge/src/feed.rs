use std::fmt::Debug;

use futures::{AsyncRead, AsyncWrite};
pub trait IO: AsyncWrite + AsyncRead + Send + Unpin + 'static {}

mod hypercore;

use random_access_storage::RandomAccess;

pub(crate) use ::hypercore_protocol::Message;
pub use ::hypercore_protocol::{Protocol, ProtocolBuilder};
pub(crate) use hypercore::HypercoreWrapper as Feed;

pub trait FeedPersistence:
    RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send + 'static
{
}

impl<T> FeedPersistence for T where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send + 'static
{
}

pub(crate) use random_access_memory::RandomAccessMemory as FeedMemoryPersistence;

#[cfg(not(target_arch = "wasm32"))]
pub(crate) use random_access_disk::RandomAccessDisk as FeedDiskPersistence;

// FIXME: temporary export, this should be in repository
pub(crate) use hypercore::on_protocol;

#[cfg(not(target_arch = "wasm32"))]
pub(crate) use hypercore::{
    create_new_read_disk_hypercore as create_new_read_disk_feed,
    create_new_write_disk_hypercore as create_new_write_disk_feed,
    open_disk_hypercore as open_disk_feed,
};
pub(crate) use hypercore::{
    create_new_read_memory_hypercore as create_new_read_memory_feed,
    create_new_write_memory_hypercore as create_new_write_memory_feed,
};

// TODO: Expose p2panda's versions of Feed, FeedPersistence, FeedMemoryPersistence, FeedDiskPersistence, ProtocolBuilder, Protocol<IO> and create/open functions below.
