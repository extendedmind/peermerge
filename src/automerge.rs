//! Auomerge-specific helpers

mod diff;
mod init;
mod materialize;

pub(crate) use diff::{apply_changes_autocommit, put_object_autocommit};
pub(crate) use init::init_doc_with_root_scalars;
pub(crate) use materialize::materialize_root_property;
