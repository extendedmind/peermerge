//! Auomerge-specific helpers

mod edit;
mod init;

use automerge::{transaction::Observed, AutoCommitWithObs, VecOpObserver};
pub(crate) use edit::*;
pub(crate) use init::*;

pub(crate) type AutomergeDoc = AutoCommitWithObs<Observed<VecOpObserver>>;
