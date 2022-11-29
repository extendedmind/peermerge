use automerge::{
    transaction::{CommitOptions, Transactable},
    ActorId, AutoCommit, Automerge, AutomergeError, Change, Prop, ScalarValue, ROOT,
};

use crate::common::entry::{Entry, EntryType};

/// Convenience method to initialize an Automerge document with root scalars
pub fn init_doc_with_root_scalars<P: Into<Prop>, V: Into<ScalarValue>>(
    discovery_key: &[u8; 32],
    root_props: Vec<(P, V)>,
) -> Vec<u8> {
    let mut doc = Automerge::new();
    doc.set_actor(ActorId::from(discovery_key));
    doc.transact_with::<_, _, AutomergeError, _>(
        |_| CommitOptions::default().with_message("init".to_owned()),
        |tx| {
            for root_prop in root_props {
                tx.put(ROOT, root_prop.0, root_prop.1).unwrap();
            }
            Ok(())
        },
    )
    .unwrap();
    doc.save()
}

pub(crate) fn init_doc_from_entries(discovery_key: &[u8; 32], entries: Vec<Entry>) -> Vec<u8> {
    let mut doc = AutoCommit::load(&entries[0].data).unwrap();
    doc.set_actor(ActorId::from(discovery_key));
    let changes: Vec<Change> = entries
        .iter()
        .skip(1)
        .filter(|entry| entry.entry_type != EntryType::InitPeer)
        .map(|entry| Change::from_bytes(entry.data.clone()).unwrap())
        .collect();
    doc.apply_changes(changes).unwrap();
    doc.save()
}
