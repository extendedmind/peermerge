use automerge::{
    transaction::{CommitOptions, Transactable},
    ActorId, AutoCommit, Automerge, AutomergeError, Prop, ScalarValue, VecOpObserver, ROOT,
};
use std::collections::HashMap;

use super::{apply_entries_autocommit, ApplyEntriesFeedChange, AutomergeDoc, UnappliedEntries};
use crate::common::entry::{Entry, EntryType};

/// Convenience method to initialize an Automerge document with root scalars
pub(crate) fn init_automerge_doc_with_root_scalars<P: Into<Prop>, V: Into<ScalarValue>>(
    peer_name: &str,
    discovery_key: &[u8; 32],
    root_props: Vec<(P, V)>,
) -> (AutomergeDoc, Vec<u8>) {
    let mut actor_id: Vec<u8> = peer_name.as_bytes().to_vec();
    actor_id.extend_from_slice(discovery_key);
    let mut automerge_doc = Automerge::new().with_actor(ActorId::from(actor_id));
    automerge_doc
        .transact_with::<_, _, AutomergeError, _>(
            |_| CommitOptions::default().with_message("init".to_owned()),
            |tx| {
                for root_prop in root_props {
                    tx.put(ROOT, root_prop.0, root_prop.1).unwrap();
                }
                Ok(())
            },
        )
        .unwrap();
    let data = automerge_doc.save();
    let automerge_doc: AutoCommit = AutoCommit::load(&data).unwrap();
    let automerge_doc = automerge_doc.with_observer(VecOpObserver::default());
    (automerge_doc, data)
}

pub(crate) fn init_automerge_doc_from_entries(
    writer_name: &str,
    write_discovery_key: &[u8; 32],
    synced_discovery_key: &[u8; 32],
    init_entry: Entry,
    unapplied_entries: &mut UnappliedEntries,
) -> anyhow::Result<(
    AutomergeDoc,
    Vec<u8>,
    HashMap<[u8; 32], ApplyEntriesFeedChange>,
)> {
    assert!(init_entry.entry_type == EntryType::InitDoc);
    let contiguous_length = 1;
    let mut automerge_doc =
        init_automerge_doc_from_data(writer_name, write_discovery_key, &init_entry.data);
    let mut result = apply_entries_autocommit(
        &mut automerge_doc,
        &synced_discovery_key,
        contiguous_length,
        vec![],
        unapplied_entries,
    )?;
    result.insert(
        synced_discovery_key.clone(),
        ApplyEntriesFeedChange::new(contiguous_length),
    );
    let data = automerge_doc.save();
    Ok((automerge_doc, data, result))
}

pub(crate) fn init_automerge_doc_from_data(
    peer_name: &str,
    discovery_key: &[u8; 32],
    data: &Vec<u8>,
) -> AutomergeDoc {
    let mut actor_id: Vec<u8> = peer_name.as_bytes().to_vec();
    actor_id.extend_from_slice(discovery_key);
    let automerge_doc = AutoCommit::load(data).unwrap();
    let automerge_doc = automerge_doc
        .with_actor(ActorId::from(actor_id))
        .with_observer(VecOpObserver::default());
    automerge_doc
}
