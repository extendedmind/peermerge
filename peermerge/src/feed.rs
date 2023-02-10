use random_access_storage::RandomAccess;
use std::fmt::Debug;

mod common;
mod hypercore;

pub(crate) use common::{get_feed, get_feed_discovery_keys};

pub use ::hypercore_protocol::{Protocol, ProtocolBuilder};
pub(crate) use hypercore::on_protocol;
pub(crate) use hypercore::HypercoreWrapper as Feed;
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
#[cfg(not(target_arch = "wasm32"))]
pub use random_access_disk::RandomAccessDisk as FeedDiskPersistence;
pub use random_access_memory::RandomAccessMemory as FeedMemoryPersistence;

pub trait FeedPersistence:
    RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send + 'static
{
}

impl<T> FeedPersistence for T where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send + 'static
{
}

// TODO: Expose p2panda's versions of
// Feed, FeedPersistence, FeedMemoryPersistence, FeedDiskPersistence, ProtocolBuilder, Protocol<IO>
// and create/open_*_feed functions below.
