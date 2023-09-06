//! Automerge-specific helpers

mod edit;
mod init;
mod meta;

use automerge::AutoCommit;
pub(crate) use edit::*;
pub(crate) use init::*;
pub(crate) use meta::*;

pub type AutomergeDoc = AutoCommit;
