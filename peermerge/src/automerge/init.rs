use automerge::{
    transaction::{CommitOptions, Transaction},
    ActorId, AutoCommit, Automerge, AutomergeError,
};
use std::collections::HashMap;

use super::{apply_entries_autocommit, ApplyEntriesFeedChange, AutomergeDoc, UnappliedEntries};
use crate::{
    common::constants::PEERMERGE_VERSION,
    common::entry::{Entry, EntryType},
    PeermergeError,
};

/// Convenience method to initialize an Automerge document with root scalars
pub(crate) fn init_automerge_doc<F, O>(
    peer_name: &str,
    discovery_key: &[u8; 32],
    init_cb: F,
) -> Result<(AutomergeDoc, O, Vec<u8>), PeermergeError>
where
    F: FnOnce(&mut Transaction) -> Result<O, AutomergeError>,
{
    let mut actor_id: Vec<u8> = peer_name.as_bytes().to_vec();
    actor_id.extend_from_slice(discovery_key);

    let mut automerge_doc = Automerge::new().with_actor(ActorId::from(actor_id));
    let result = automerge_doc
        .transact_with::<_, _, AutomergeError, _>(
            |_| CommitOptions::default().with_message(format!("init:{PEERMERGE_VERSION}")),
            init_cb,
        )
        .unwrap()
        .result;

    let data = automerge_doc.save();
    let automerge_doc: AutoCommit = AutoCommit::load(&data).unwrap();
    Ok((automerge_doc, result, data))
}

pub(crate) fn init_automerge_doc_from_entries(
    writer_name: &str,
    write_discovery_key: &[u8; 32],
    synced_discovery_key: &[u8; 32],
    init_entry: Entry,
    unapplied_entries: &mut UnappliedEntries,
) -> Result<
    (
        AutomergeDoc,
        Vec<u8>,
        HashMap<[u8; 32], ApplyEntriesFeedChange>,
    ),
    PeermergeError,
> {
    assert!(init_entry.entry_type == EntryType::InitDoc);
    let contiguous_length = 1;
    let mut automerge_doc =
        init_automerge_doc_from_data(writer_name, write_discovery_key, &init_entry.data);
    let mut result = apply_entries_autocommit(
        &mut automerge_doc,
        synced_discovery_key,
        contiguous_length,
        vec![],
        unapplied_entries,
    )?;
    result.insert(
        *synced_discovery_key,
        ApplyEntriesFeedChange::new(contiguous_length),
    );
    let data = save_automerge_doc(&mut automerge_doc);
    Ok((automerge_doc, data, result))
}

pub(crate) fn init_automerge_doc_from_data(
    peer_name: &str,
    discovery_key: &[u8; 32],
    data: &[u8],
) -> AutomergeDoc {
    let mut actor_id: Vec<u8> = peer_name.as_bytes().to_vec();
    actor_id.extend_from_slice(discovery_key);
    let automerge_doc = AutoCommit::load(data).unwrap();
    let mut doc = automerge_doc.with_actor(ActorId::from(actor_id));
    // Update the diff to the head
    doc.update_diff_cursor();
    doc
}

pub(crate) fn save_automerge_doc(automerge_doc: &mut AutomergeDoc) -> Vec<u8> {
    automerge_doc.save()
}
