use futures::{AsyncRead, AsyncWrite};
pub trait IO: AsyncWrite + AsyncRead + Send + Unpin + 'static {}

mod hypercore;

pub(crate) use ::hypercore_protocol::Message;
pub use ::hypercore_protocol::{Protocol, ProtocolBuilder};
pub(crate) use hypercore::HypercoreWrapper as Feed;

// FIXME: temporary exports, these should be in repository

pub(crate) use hypercore::on_protocol;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use hypercore::{
    create_new_read_disk_hypercore, create_new_write_disk_hypercore, open_disk_hypercore,
};
pub(crate) use hypercore::{create_new_read_memory_hypercore, create_new_write_memory_hypercore};

// TODO: Expose p2panda's versions of Feed, ProtocolBuilder, Protocol<IO> and Message below.
