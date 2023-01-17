use automerge::{transaction::Transactable, Change, ChangeHash, ObjId, ObjType, Prop, ScalarValue};
use std::collections::{HashMap, HashSet, VecDeque};

use super::AutomergeDoc;
use crate::common::entry::{Entry, EntryType};

#[derive(Debug)]
pub(crate) struct UnappliedEntries {
    data: HashMap<[u8; 32], (u64, VecDeque<Entry>)>,
}

impl UnappliedEntries {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    pub fn current_length(&self, discovery_key: &[u8; 32]) -> u64 {
        if let Some(value) = self.data.get(discovery_key) {
            value.0
        } else {
            0
        }
    }

    pub fn add(&mut self, discovery_key: &[u8; 32], index: u64, entry: Entry) {
        if let Some(value) = self.data.get_mut(discovery_key) {
            value.0 = index;
            value.1.push_back(entry);
            value.1.len() - 1
        } else {
            self.data
                .insert(discovery_key.clone(), (index, VecDeque::from([entry])));
            0
        };
    }

    pub fn consolidate(
        &mut self,
        doc: &mut AutomergeDoc,
        changes_to_apply: &mut Vec<Change>,
        result: &mut HashMap<[u8; 32], (u64, Option<String>)>,
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
                let mut new_length = value.0;
                for entry in value.1.iter() {
                    match entry.entry_type {
                        EntryType::Change => {
                            let change = entry.change.as_ref().unwrap();
                            if change.deps().iter().all(|dep| {
                                if hashes.contains(dep) {
                                    true
                                } else if doc.get_change_by_hash(dep).is_some() {
                                    // For the next rounds to be faster, let's push this
                                    // to the hashes array to avoid searching the doc again
                                    hashes.insert(*dep);
                                    true
                                } else {
                                    false
                                }
                            }) {
                                changes_to_apply.push(change.clone());
                                hashes.insert(change.hash());
                                if let Some(result_value) = result.get_mut(discovery_key) {
                                    result_value.0 = new_length;
                                } else {
                                    result.insert(discovery_key.clone(), (new_length, None));
                                }
                                changed = true;
                                new_length += 1;
                            } else {
                                // Only try to insert in order per hypercore, don't apply in between changes, as they
                                // will eventually be insertable.
                                break;
                            }
                        }
                        EntryType::InitPeer => {
                            let peer_name = entry.peer_name.as_ref().unwrap();
                            if let Some(value) = result.get_mut(discovery_key) {
                                value.0 = new_length;
                                value.1 = Some(peer_name.to_string());
                            } else {
                                result.insert(
                                    discovery_key.clone(),
                                    (new_length, Some(peer_name.to_string())),
                                );
                            }
                            changed = true;
                            new_length += 1;
                        }
                        _ => panic!("Unexpected entry {:?}", entry),
                    }
                }

                // Remove from the unapplied changes queue all that were now inserted
                for _ in value.0..new_length {
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
    doc: &mut AutomergeDoc,
    discovery_key: &[u8; 32],
    contiguous_length: u64,
    entries: Vec<Entry>,
    unapplied_entries: &mut UnappliedEntries,
) -> anyhow::Result<HashMap<[u8; 32], (u64, Option<String>)>> {
    let mut result: HashMap<[u8; 32], (u64, Option<String>)> = HashMap::new();
    let mut changes_to_apply: Vec<Change> = vec![];
    let len = entries.len() as u64;
    let mut length = contiguous_length - len;
    for entry in entries {
        length += 1;
        match entry.entry_type {
            EntryType::Change => {
                let change = entry.change.as_ref().unwrap();
                if change
                    .deps()
                    .iter()
                    .all(|dep| doc.get_change_by_hash(dep).is_some())
                {
                    changes_to_apply.push(change.clone());
                    if let Some(value) = result.get_mut(discovery_key) {
                        value.0 = length;
                    } else {
                        result.insert(discovery_key.clone(), (length, None));
                    }
                } else {
                    // All of the deps of this change are not in the doc, add to unapplied
                    // entries.
                    unapplied_entries.add(discovery_key, length, entry);
                };
            }
            EntryType::InitPeer => {
                let peer_name = entry.peer_name.unwrap();
                if let Some(value) = result.get_mut(discovery_key) {
                    value.0 = length;
                    value.1 = Some(peer_name);
                } else {
                    result.insert(discovery_key.clone(), (length, Some(peer_name)));
                }
            }
            _ => panic!("Unexpected entry {:?}", entry),
        }
    }

    // Consolidate unapplied entries and add them to changes and result
    unapplied_entries.consolidate(doc, &mut changes_to_apply, &mut result);

    if !changes_to_apply.is_empty() {
        doc.apply_changes(changes_to_apply)?;
    }
    Ok(result)
}

pub(crate) fn put_object_autocommit<O: AsRef<ObjId>, P: Into<Prop>>(
    doc: &mut AutomergeDoc,
    obj: O,
    prop: P,
    object: ObjType,
) -> anyhow::Result<(Entry, ObjId)> {
    let id = doc.put_object(obj, prop, object)?;
    let change = doc.get_last_local_change().unwrap().clone();
    Ok((Entry::new_change(change), id))
}

pub(crate) fn put_scalar_autocommit<O: AsRef<ObjId>, P: Into<Prop>, V: Into<ScalarValue>>(
    doc: &mut AutomergeDoc,
    obj: O,
    prop: P,
    value: V,
) -> anyhow::Result<Entry> {
    doc.put(obj, prop, value)?;
    let change = doc.get_last_local_change().unwrap().clone();
    Ok(Entry::new_change(change))
}

pub(crate) fn splice_text<O: AsRef<ObjId>>(
    doc: &mut AutomergeDoc,
    obj: O,
    index: usize,
    delete: usize,
    text: &str,
) -> anyhow::Result<Entry> {
    doc.splice_text(obj, index, delete, text)?;
    let change = doc.get_last_local_change().unwrap().clone();
    Ok(Entry::new_change(change))
}

#[cfg(test)]
mod tests {
    use automerge::ROOT;

    use super::*;
    use crate::{
        automerge::{init_doc_from_data, init_doc_with_root_scalars},
        hypercore::generate_keys,
    };

    fn assert_int_value(doc: &AutomergeDoc, key: &ObjId, prop: &str, expected: i64) {
        let value = doc.get(key, prop).unwrap().unwrap();
        let actual = value.0.to_scalar().unwrap();
        assert_eq!(actual, &ScalarValue::Int(expected));
    }

    #[test]
    fn automerge_edit_apply_entries() -> anyhow::Result<()> {
        let peer_name = "test";
        let int_prop = "number";
        let int_value = 1;
        let (_, doc_discovery_key) = generate_keys();
        let (_, peer_1_discovery_key) = generate_keys();
        let (_, peer_2_discovery_key) = generate_keys();
        let (mut doc, data) =
            init_doc_with_root_scalars(peer_name, &doc_discovery_key, vec![("version", 1)]);

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
        doc = init_doc_from_data(&peer_name, &doc_discovery_key, &data);
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
        doc = init_doc_from_data(&peer_name, &doc_discovery_key, &data);
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
        doc = init_doc_from_data(&peer_name, &doc_discovery_key, &data);
        apply_entries_autocommit(
            &mut doc,
            &peer_1_discovery_key,
            4,
            vec![entry_2.clone(), entry_4.clone(), entry_scalar.clone()],
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
            vec![entry_3.clone(), entry_5.clone()],
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
            vec![entry_1.clone()],
            &mut unapplied_entries,
        )?;
        assert_eq!(unapplied_entries.data.len(), 0);
        assert_int_value(&doc, &key_5, int_prop, int_value);

        Ok(())
    }
}
