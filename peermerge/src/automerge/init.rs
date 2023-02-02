use std::collections::HashMap;

use automerge::{
    transaction::{CommitOptions, Transactable},
    ActorId, AutoCommit, Automerge, AutomergeError, Prop, ScalarValue, VecOpObserver, ROOT,
};

use crate::common::entry::{Entry, EntryType};

use super::{apply_entries_autocommit, AutomergeDoc, UnappliedEntries};

/// Convenience method to initialize an Automerge document with root scalars
pub fn init_doc_with_root_scalars<P: Into<Prop>, V: Into<ScalarValue>>(
    peer_name: &str,
    discovery_key: &[u8; 32],
    root_props: Vec<(P, V)>,
) -> (AutomergeDoc, Vec<u8>) {
    let mut actor_id: Vec<u8> = peer_name.as_bytes().to_vec();
    actor_id.extend_from_slice(discovery_key);
    let mut doc = Automerge::new().with_actor(ActorId::from(actor_id));
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
    let doc = doc.with_observer(VecOpObserver::default());
    (doc, data)
}

pub(crate) fn init_doc_from_entries(
    write_peer_name: &str,
    write_discovery_key: &[u8; 32],
    synced_discovery_key: &[u8; 32],
    mut entries: Vec<Entry>,
    unapplied_entries: &mut UnappliedEntries,
) -> anyhow::Result<(
    AutomergeDoc,
    Vec<u8>,
    HashMap<[u8; 32], (u64, Option<String>)>,
)> {
    let contiguous_length = entries.len() as u64;
    let init_entry = entries.remove(0);
    assert!(init_entry.entry_type == EntryType::InitDoc);
    let peer_name = init_entry.peer_name.unwrap();
    let mut doc = init_doc_from_data(write_peer_name, write_discovery_key, &init_entry.data);
    let mut result = apply_entries_autocommit(
        &mut doc,
        &synced_discovery_key,
        contiguous_length,
        entries,
        unapplied_entries,
    )?;
    if let Some(value) = result.get_mut(synced_discovery_key) {
        value.1 = Some(peer_name);
    } else {
        result.insert(
            synced_discovery_key.clone(),
            (contiguous_length, Some(peer_name)),
        );
    }
    let data = doc.save();
    Ok((doc, data, result))
}

pub(crate) fn init_doc_from_data(
    peer_name: &str,
    discovery_key: &[u8; 32],
    data: &Vec<u8>,
) -> AutomergeDoc {
    let mut actor_id: Vec<u8> = peer_name.as_bytes().to_vec();
    actor_id.extend_from_slice(discovery_key);
    let doc = AutoCommit::load(data).unwrap();
    let doc = doc
        .with_actor(ActorId::from(actor_id))
        .with_observer(VecOpObserver::default());
    doc
}
