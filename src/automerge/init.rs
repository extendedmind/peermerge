use automerge::{
    transaction::{CommitOptions, Transactable},
    ActorId, AutoCommit, Automerge, AutomergeError, Change, Prop, ScalarValue, VecOpObserver, ROOT,
};

use crate::common::entry::{Entry, EntryType};

use super::AutomergeDoc;

/// Convenience method to initialize an Automerge document with root scalars
pub fn init_doc_with_root_scalars<P: Into<Prop>, V: Into<ScalarValue>>(
    discovery_key: &[u8; 32],
    root_props: Vec<(P, V)>,
) -> (AutomergeDoc, Vec<u8>) {
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
    let data = doc.save();
    let doc: AutoCommit = AutoCommit::load(&data).unwrap();
    let mut doc = doc.with_observer(VecOpObserver::default());
    (doc, data)
}

pub(crate) fn init_doc_from_data(discovery_key: &[u8; 32], data: &Vec<u8>) -> AutomergeDoc {
    let mut doc = AutoCommit::load(data).unwrap();
    let mut doc = doc.with_observer(VecOpObserver::default());
    doc.set_actor(ActorId::from(discovery_key));
    doc
}

pub(crate) fn init_doc_from_entries(
    discovery_key: &[u8; 32],
    entries: Vec<Entry>,
) -> (AutomergeDoc, Vec<u8>) {
    let mut doc = init_doc_from_data(discovery_key, &entries[0].data);
    let changes: Vec<Change> = entries
        .iter()
        .skip(1)
        .filter(|entry| entry.entry_type != EntryType::InitPeer)
        .map(|entry| Change::from_bytes(entry.data.clone()).unwrap())
        .collect();

    let result = doc.apply_changes(changes).unwrap();
    let data = doc.save();
    (doc, data)
}
