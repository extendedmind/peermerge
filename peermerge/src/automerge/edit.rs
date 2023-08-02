use automerge::{AutomergeError, Change, ChangeHash, ObjId};
use std::collections::{HashMap, HashSet, VecDeque};

use super::AutomergeDoc;
use crate::{
    common::entry::{Entry, EntryType},
    NameDescription, PeermergeError,
};

#[derive(Debug)]
pub(crate) struct ApplyEntriesFeedChange {
    pub(crate) length: u64,
    pub(crate) peer_header: Option<NameDescription>,
}

impl ApplyEntriesFeedChange {
    pub(crate) fn new(length: u64) -> Self {
        Self {
            length,
            peer_header: None,
        }
    }

    pub(crate) fn new_with_peer_header(length: u64, peer_header: NameDescription) -> Self {
        Self {
            length,
            peer_header: Some(peer_header),
        }
    }

    pub(crate) fn set_length(&mut self, length: u64) {
        self.length = length;
    }

    pub(crate) fn set_peer_header(&mut self, peer_header: NameDescription) {
        self.peer_header = Some(peer_header);
    }
}

#[derive(Debug)]
pub(crate) struct UnappliedEntries {
    data: HashMap<[u8; 32], (u64, VecDeque<Entry>, Vec<ObjId>)>,
    pub(crate) reserved_ids: HashSet<ObjId>,
}

impl UnappliedEntries {
    pub(crate) fn new() -> Self {
        Self {
            data: HashMap::new(),
            reserved_ids: HashSet::new(),
        }
    }

    pub(crate) fn current_length(&self, discovery_key: &[u8; 32]) -> u64 {
        if let Some(value) = self.data.get(discovery_key) {
            value.0
        } else {
            0
        }
    }

    pub(crate) fn add(
        &mut self,
        discovery_key: &[u8; 32],
        length: u64,
        entry: Entry,
        reserved: Vec<ObjId>,
    ) {
        if let Some(value) = self.data.get_mut(discovery_key) {
            value.0 = length;
            value.1.push_back(entry);
            value.1.len() - 1
        } else {
            self.data
                .insert(*discovery_key, (length, VecDeque::from([entry]), reserved));
            0
        };
    }

    pub(crate) fn consolidate(
        &mut self,
        automerge_doc: &mut AutomergeDoc,
        changes_to_apply: &mut Vec<Change>,
        result: &mut HashMap<[u8; 32], ApplyEntriesFeedChange>,
    ) {
        let mut hashes: HashSet<ChangeHash> = changes_to_apply
            .iter()
            .map(|change| change.hash())
            .collect();

        // We need to loop multiple times to be sure all of the unapplied changes
        // that can be inserted, are inserted.
        loop {
            let mut changed = false;
            for kv in self.data.iter_mut() {
                let discovery_key = kv.0;
                let value = kv.1;
                if value
                    .2
                    .iter()
                    .any(|reserved_obj| self.reserved_ids.contains(reserved_obj))
                {
                    // This change is targeting a reserved object, don't try to apply
                    continue;
                } else if !value.2.is_empty() {
                    value.2 = vec![];
                }
                // The data structure stores in value.0 the length after all entries are applied.
                // When iterating the entries, start from the first.
                let original_start_index = value.0 - value.1.len() as u64;
                let mut new_length = original_start_index;
                for entry in value.1.iter() {
                    match entry.entry_type {
                        EntryType::Change => {
                            let change = entry.change.as_ref().unwrap();
                            if change.deps().iter().all(|dep| {
                                if hashes.contains(dep) {
                                    true
                                } else if automerge_doc.get_change_by_hash(dep).is_some() {
                                    // For the next rounds to be faster, let's push this
                                    // to the hashes array to avoid searching the doc again
                                    hashes.insert(*dep);
                                    true
                                } else {
                                    false
                                }
                            }) {
                                new_length += 1;
                                changes_to_apply.push(change.clone());
                                hashes.insert(change.hash());
                                if let Some(result_value) = result.get_mut(discovery_key) {
                                    result_value.set_length(new_length);
                                } else {
                                    result.insert(
                                        *discovery_key,
                                        ApplyEntriesFeedChange::new(new_length),
                                    );
                                }
                                changed = true;
                            } else {
                                // Only try to insert in order per hypercore, don't apply in between changes, as they
                                // will eventually be insertable.
                                break;
                            }
                        }
                        EntryType::InitPeer => {
                            new_length += 1;
                            let peer_header = NameDescription {
                                name: entry.name.as_ref().unwrap().to_string(),
                                description: entry.description.clone(),
                            };
                            if let Some(result_value) = result.get_mut(discovery_key) {
                                result_value.set_length(new_length);
                                result_value.set_peer_header(peer_header);
                            } else {
                                result.insert(
                                    *discovery_key,
                                    ApplyEntriesFeedChange::new_with_peer_header(
                                        new_length,
                                        peer_header,
                                    ),
                                );
                            }
                            changed = true;
                        }
                        _ => panic!("Unexpected entry {entry:?}"),
                    }
                }

                // Remove from the unapplied changes queue all that were now inserted
                for _ in original_start_index..new_length {
                    value.1.pop_front();
                }
            }

            if !changed {
                self.data.retain(|_, v| !v.1.is_empty());
                break;
            }
        }
    }
}

/// Applies new entries to document taking all unapplied entries in given param. Returns what
/// should be persisted. Returns for all of the affected discovery keys that were changed, the length
/// where the cursor should be moved to and/or peer name change.
pub(crate) fn apply_entries_autocommit(
    automerge_doc: &mut AutomergeDoc,
    discovery_key: &[u8; 32],
    contiguous_length: u64,
    entries: Vec<Entry>,
    unapplied_entries: &mut UnappliedEntries,
) -> Result<HashMap<[u8; 32], ApplyEntriesFeedChange>, PeermergeError> {
    let mut result: HashMap<[u8; 32], ApplyEntriesFeedChange> = HashMap::new();
    let mut changes_to_apply: Vec<Change> = vec![];
    let len = entries.len() as u64;
    let mut length = contiguous_length - len;
    for entry in entries {
        length += 1;
        match entry.entry_type {
            EntryType::Change => {
                let change = entry.change.as_ref().unwrap();
                let reserved: Vec<ObjId> = if !unapplied_entries.reserved_ids.is_empty() {
                    change
                        .decode()
                        .operations
                        .iter()
                        .filter_map(|op| {
                            // TODO: This is hack to convert automerge::legacy::ObjectId id to ObjId
                            // A better way to do this would be to use splice_text to a certain
                            // point in history so that we could just always apply changes without
                            // a need to reserve/unreserve the object id.
                            let obj_id_as_string: &str = &op.obj.to_string();
                            if let Ok((obj, _)) = automerge_doc.import(obj_id_as_string) {
                                if unapplied_entries.reserved_ids.contains(&obj) {
                                    Some(obj)
                                } else {
                                    None
                                }
                            } else {
                                // Converting the legacy ObjectId can fail if the string id has bad actor.
                                // This could cause a bug, but again, this is a hopefully temporary hack.
                                None
                            }
                        })
                        .collect()
                } else {
                    vec![]
                };
                if reserved.is_empty()
                    && change
                        .deps()
                        .iter()
                        .all(|dep| automerge_doc.get_change_by_hash(dep).is_some())
                {
                    changes_to_apply.push(change.clone());
                    if let Some(result_value) = result.get_mut(discovery_key) {
                        result_value.set_length(length);
                    } else {
                        result.insert(*discovery_key, ApplyEntriesFeedChange::new(length));
                    }
                } else {
                    // All of the deps of this change are not in the doc or the object this
                    // change targets is reserved, add to unapplied entries.
                    unapplied_entries.add(discovery_key, length, entry, reserved);
                };
            }
            EntryType::InitPeer => {
                let peer_header = NameDescription {
                    name: entry.name.unwrap(),
                    description: entry.description,
                };
                if let Some(result_value) = result.get_mut(discovery_key) {
                    result_value.set_length(length);
                    result_value.set_peer_header(peer_header);
                } else {
                    result.insert(
                        *discovery_key,
                        ApplyEntriesFeedChange::new_with_peer_header(length, peer_header),
                    );
                }
            }
            _ => panic!("Unexpected entry {entry:?}"),
        }
    }

    // Consolidate unapplied entries and add them to changes and result
    unapplied_entries.consolidate(automerge_doc, &mut changes_to_apply, &mut result);

    if !changes_to_apply.is_empty() {
        automerge_doc.apply_changes(changes_to_apply)?;
    }
    Ok(result)
}

/// Tries to apply unapplied entries in given param. Returns what should be persisted.
/// Returns for all of the affected discovery keys that were changed, the length
/// where the cursor should be moved to and/or peer name change.
pub(crate) fn apply_unapplied_entries_autocommit(
    automerge_doc: &mut AutomergeDoc,
    unapplied_entries: &mut UnappliedEntries,
) -> Result<HashMap<[u8; 32], ApplyEntriesFeedChange>, PeermergeError> {
    let mut result: HashMap<[u8; 32], ApplyEntriesFeedChange> = HashMap::new();
    let mut changes_to_apply: Vec<Change> = vec![];

    // Consolidate unapplied entries and add them to changes and result
    unapplied_entries.consolidate(automerge_doc, &mut changes_to_apply, &mut result);
    if !changes_to_apply.is_empty() {
        automerge_doc.apply_changes(changes_to_apply)?;
    }
    Ok(result)
}

pub(crate) fn transact_autocommit<F, O>(
    automerge_doc: &mut AutomergeDoc,
    cb: F,
) -> Result<(Option<Entry>, O), PeermergeError>
where
    F: FnOnce(&mut AutomergeDoc) -> Result<O, AutomergeError>,
{
    let result = cb(automerge_doc).unwrap();
    let entry = automerge_doc
        .get_last_local_change()
        .map(|change| Entry::new_change(change.clone()));
    Ok((entry, result))
}

pub(crate) fn read_autocommit<F, O>(
    automerge_doc: &AutomergeDoc,
    cb: F,
) -> Result<O, PeermergeError>
where
    F: FnOnce(&AutomergeDoc) -> Result<O, AutomergeError>,
{
    let result = cb(automerge_doc).unwrap();
    Ok(result)
}

#[cfg(test)]
mod tests {
    use automerge::{transaction::Transactable, ObjType, Prop, ReadDoc, ScalarValue, ROOT};

    use super::*;
    use crate::{
        automerge::{init_automerge_doc, init_automerge_doc_from_data},
        common::keys::generate_keys,
    };

    fn assert_int_value(doc: &AutomergeDoc, key: &ObjId, prop: &str, expected: i64) {
        let value = doc.get(key, prop).unwrap().unwrap();
        let actual = value.0.to_scalar().unwrap();
        assert_eq!(actual, &ScalarValue::Int(expected));
    }

    fn put_object_autocommit<O: AsRef<ObjId>, P: Into<Prop>>(
        automerge_doc: &mut AutomergeDoc,
        obj: O,
        prop: P,
        object: ObjType,
    ) -> Result<(Entry, ObjId), PeermergeError> {
        let id = automerge_doc.put_object(obj, prop, object)?;
        let change = automerge_doc.get_last_local_change().unwrap().clone();
        Ok((Entry::new_change(change), id))
    }

    fn put_scalar_autocommit<O: AsRef<ObjId>, P: Into<Prop>, V: Into<ScalarValue>>(
        automerge_doc: &mut AutomergeDoc,
        obj: O,
        prop: P,
        value: V,
    ) -> Result<Entry, PeermergeError> {
        automerge_doc.put(obj, prop, value)?;
        let change = automerge_doc.get_last_local_change().unwrap().clone();
        Ok(Entry::new_change(change))
    }

    #[test]
    fn automerge_edit_apply_entries() -> anyhow::Result<()> {
        let peer_name = "test";
        let int_prop = "number";
        let int_value = 1;
        let (_, doc_discovery_key) = generate_keys();
        let (_, peer_1_discovery_key) = generate_keys();
        let (_, peer_2_discovery_key) = generate_keys();
        let (mut doc, _, data) = init_automerge_doc(peer_name, &doc_discovery_key, |tx| {
            tx.put(ROOT, "version", 1)
        })
        .unwrap();

        // Let's create a tree of depth 5
        let (entry_1, key_1) =
            put_object_autocommit(&mut doc, ROOT, "level_1", automerge::ObjType::Map)?;
        let (entry_2, key_2) =
            put_object_autocommit(&mut doc, &key_1, "level_2", automerge::ObjType::Map)?;
        let (entry_3, key_3) =
            put_object_autocommit(&mut doc, &key_2, "level_3", automerge::ObjType::Map)?;
        let (entry_4, key_4) =
            put_object_autocommit(&mut doc, &key_3, "level_4", automerge::ObjType::Map)?;
        let (entry_5, key_5) =
            put_object_autocommit(&mut doc, &key_4, "level_5", automerge::ObjType::Map)?;
        let entry_scalar = put_scalar_autocommit(&mut doc, &key_5, int_prop, int_value)?;

        // Different consolidations
        let mut unapplied_entries = UnappliedEntries::new();

        // All in order, should result in no unapplied entries
        apply_entries_autocommit(
            &mut doc,
            &doc_discovery_key,
            7,
            vec![
                entry_1.clone(),
                entry_2.clone(),
                entry_3.clone(),
                entry_4.clone(),
                entry_5.clone(),
                entry_scalar.clone(),
            ],
            &mut unapplied_entries,
        )?;
        assert_eq!(unapplied_entries.data.len(), 0);
        assert_int_value(&doc, &key_5, int_prop, int_value);

        // In chunks
        doc = init_automerge_doc_from_data(peer_name, &doc_discovery_key, &data);
        apply_entries_autocommit(
            &mut doc,
            &doc_discovery_key,
            3,
            vec![entry_1.clone(), entry_2.clone()],
            &mut unapplied_entries,
        )?;
        assert_eq!(unapplied_entries.data.len(), 0);
        apply_entries_autocommit(
            &mut doc,
            &doc_discovery_key,
            5,
            vec![entry_3.clone(), entry_4.clone()],
            &mut unapplied_entries,
        )?;
        assert_eq!(unapplied_entries.data.len(), 0);
        apply_entries_autocommit(
            &mut doc,
            &doc_discovery_key,
            7,
            vec![entry_5.clone(), entry_scalar.clone()],
            &mut unapplied_entries,
        )?;
        assert_eq!(unapplied_entries.data.len(), 0);
        assert_int_value(&doc, &key_5, int_prop, int_value);

        // Missing first, should first result in all going to unapplied entries, then consolidate
        doc = init_automerge_doc_from_data(peer_name, &doc_discovery_key, &data);
        apply_entries_autocommit(
            &mut doc,
            &doc_discovery_key,
            6,
            vec![
                entry_2.clone(),
                entry_3.clone(),
                entry_4.clone(),
                entry_5.clone(),
                entry_scalar.clone(),
            ],
            &mut unapplied_entries,
        )?;
        assert_eq!(unapplied_entries.data.len(), 1);
        assert_eq!(
            unapplied_entries
                .data
                .get(&doc_discovery_key)
                .unwrap()
                .1
                .len(),
            5
        );
        apply_entries_autocommit(
            &mut doc,
            &peer_1_discovery_key,
            1,
            vec![entry_1.clone()],
            &mut unapplied_entries,
        )?;
        assert_eq!(unapplied_entries.data.len(), 0);
        assert_int_value(&doc, &key_5, int_prop, int_value);

        // Mixture of two peers having every other change
        doc = init_automerge_doc_from_data(peer_name, &doc_discovery_key, &data);
        apply_entries_autocommit(
            &mut doc,
            &peer_1_discovery_key,
            4,
            vec![entry_2, entry_4, entry_scalar],
            &mut unapplied_entries,
        )?;
        assert_eq!(unapplied_entries.data.len(), 1);
        assert_eq!(
            unapplied_entries
                .data
                .get(&peer_1_discovery_key)
                .unwrap()
                .1
                .len(),
            3
        );
        apply_entries_autocommit(
            &mut doc,
            &peer_2_discovery_key,
            3,
            vec![entry_3, entry_5],
            &mut unapplied_entries,
        )?;
        assert_eq!(unapplied_entries.data.len(), 2);
        assert_eq!(
            unapplied_entries
                .data
                .get(&peer_1_discovery_key)
                .unwrap()
                .1
                .len(),
            3
        );
        assert_eq!(
            unapplied_entries
                .data
                .get(&peer_2_discovery_key)
                .unwrap()
                .1
                .len(),
            2
        );
        apply_entries_autocommit(
            &mut doc,
            &doc_discovery_key,
            2,
            vec![entry_1],
            &mut unapplied_entries,
        )?;
        assert_eq!(unapplied_entries.data.len(), 0);
        assert_int_value(&doc, &key_5, int_prop, int_value);

        Ok(())
    }
}
