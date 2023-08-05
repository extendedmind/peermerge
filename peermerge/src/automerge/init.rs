use automerge::{
    transaction::{CommitOptions, Transaction},
    ActorId, AutoCommit, Automerge, AutomergeError,
};
use std::collections::HashMap;

use super::{apply_entries_autocommit, ApplyEntriesFeedChange, AutomergeDoc, UnappliedEntries};
use crate::{
    common::constants::{MAX_DATA_CHUNK_BYTES, PEERMERGE_VERSION},
    common::entry::{Entry, EntryContent},
    PeermergeError,
};

/// Convenience method to initialize an Automerge document with root scalars
pub(crate) fn init_automerge_doc<F, O>(
    peer_name: &str,
    discovery_key: &[u8; 32],
    init_cb: F,
) -> Result<(AutomergeDoc, O, Vec<u8>, Vec<Vec<u8>>), PeermergeError>
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
    let data_parts = split_data_into_parts(&data);
    let automerge_doc: AutoCommit = AutoCommit::load(&data).unwrap();
    Ok((automerge_doc, result, data, data_parts))
}

pub(crate) fn init_automerge_doc_from_entries(
    writer_name: &str,
    write_discovery_key: &[u8; 32],
    synced_discovery_key: &[u8; 32],
    init_entries: Vec<Entry>,
    unapplied_entries: &mut UnappliedEntries,
) -> Result<
    (
        AutomergeDoc,
        Vec<u8>,
        HashMap<[u8; 32], ApplyEntriesFeedChange>,
    ),
    PeermergeError,
> {
    let mut part_count: u64 = 0;
    let mut full_data: Vec<u8> = vec![];
    let init_entries_len = init_entries.len() as u64;
    for (i, entry) in init_entries.into_iter().enumerate() {
        if i == 0 {
            match entry.content {
                EntryContent::InitDoc { doc_entry_count } => part_count = doc_entry_count.into(),
                _ => panic!("Initial entries need to start with InitDoc"),
            };
        } else if i > 0 {
            match entry.content {
                EntryContent::DocPart { index, data } => {
                    assert_eq!(index, i as u32 - 1);
                    full_data.extend(data);
                }
                _ => panic!("Initial entries need to continue with DocParts"),
            }
        }
    }
    let contiguous_length: u64 = 1 + part_count;
    assert_eq!(init_entries_len, contiguous_length);
    let mut automerge_doc =
        init_automerge_doc_from_data(writer_name, write_discovery_key, &full_data);
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

fn split_data_into_parts(data: &Vec<u8>) -> Vec<Vec<u8>> {
    data.chunks(MAX_DATA_CHUNK_BYTES)
        .map(|s| s.into())
        .collect()
}
