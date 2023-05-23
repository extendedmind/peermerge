//! Auomerge-specific helpers

mod edit;
mod init;

use automerge::AutoCommit;
pub(crate) use edit::*;
pub(crate) use init::*;

pub(crate) type AutomergeDoc = AutoCommit;
