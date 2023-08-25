use automerge::{AutomergeError, Change, ChangeHash, ObjId};
use std::collections::{HashMap, HashSet, VecDeque};

use super::AutomergeDoc;
use crate::{
    common::entry::{split_change_into_entries, Entry, EntryContent, ShrunkEntries},
    feed::FeedDiscoveryKey,
    PeermergeError,
};

#[derive(Debug)]
pub(crate) struct ApplyEntriesFeedChange {
    pub(crate) length: u64,
}

impl ApplyEntriesFeedChange {
    pub(crate) fn new(length: u64) -> Self {
        Self { length }
    }

    pub(crate) fn set_length(&mut self, length: u64) {
        self.length = length;
    }
}

#[derive(Debug)]
pub(crate) struct UnappliedEntries {
    data: HashMap<FeedDiscoveryKey, (u64, VecDeque<Entry>, Vec<ObjId>)>,
    pub(crate) reserved_ids: HashSet<ObjId>,
}

impl UnappliedEntries {
    pub(crate) fn new() -> Self {
        Self {
            data: HashMap::new(),
            reserved_ids: HashSet::new(),
        }
    }

    pub(crate) fn add(
        &mut self,
        discovery_key: &FeedDiscoveryKey,
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
        meta_automerge_doc: &mut AutomergeDoc,
        user_automerge_doc: &mut AutomergeDoc,
        meta_changes_to_apply: &mut Vec<Change>,
        user_changes_to_apply: &mut Vec<Change>,
        result: &mut HashMap<[u8; 32], ApplyEntriesFeedChange>,
    ) {
        let mut meta_hashes: HashSet<ChangeHash> = meta_changes_to_apply
            .iter()
            .map(|change| change.hash())
            .collect();
        let mut user_hashes: HashSet<ChangeHash> = user_changes_to_apply
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
                    match &entry.content {
                        EntryContent::Change { meta, change, .. } => {
                            let change = change.as_ref().unwrap();
                            if change.deps().iter().all(|dep| {
                                if user_hashes.contains(dep) {
                                    true
                                } else if user_automerge_doc.get_change_by_hash(dep).is_some() {
                                    // For the next rounds to be faster, let's push this
                                    // to the hashes array to avoid searching the doc again
                                    user_hashes.insert(*dep);
                                    true
                                } else {
                                    false
                                }
                            }) {
                                new_length += 1;
                                if *meta {
                                    meta_changes_to_apply.push(*change.clone());
                                    meta_hashes.insert(change.hash());
                                } else {
                                    user_changes_to_apply.push(*change.clone());
                                    user_hashes.insert(change.hash());
                                }
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
                        EntryContent::InitPeer {
                            meta_doc_data,
                            user_doc_data,
                            ..
                        } => {
                            let mut changed_meta_automerge_doc =
                                AutomergeDoc::load(meta_doc_data).unwrap();
                            meta_automerge_doc
                                .merge(&mut changed_meta_automerge_doc)
                                .unwrap();
                            if let Some(user_doc_data) = user_doc_data {
                                let mut changed_user_automerge_doc =
                                    AutomergeDoc::load(user_doc_data).unwrap();
                                user_automerge_doc
                                    .merge(&mut changed_user_automerge_doc)
                                    .unwrap();
                            }
                            new_length += 1;
                            if let Some(result_value) = result.get_mut(discovery_key) {
                                result_value.set_length(new_length);
                            } else {
                                result.insert(
                                    *discovery_key,
                                    ApplyEntriesFeedChange::new(new_length),
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

#[derive(Debug)]
pub(crate) struct DocsChangeResult {
    pub(crate) meta_changed: bool,
    pub(crate) user_changed: bool,
}

/// Applies new entries to documents taking all unapplied entries in given param. Returns what
/// should be persisted. Returns for all of the affected discovery keys that were changed and the
/// length where the cursor should be moved to.
pub(crate) fn apply_entries_autocommit(
    meta_automerge_doc: &mut AutomergeDoc,
    user_automerge_doc: &mut AutomergeDoc,
    discovery_key: &FeedDiscoveryKey,
    contiguous_length: u64,
    shrunk_entries: ShrunkEntries,
    unapplied_entries: &mut UnappliedEntries,
) -> Result<HashMap<[u8; 32], ApplyEntriesFeedChange>, PeermergeError> {
    let mut result: HashMap<[u8; 32], ApplyEntriesFeedChange> = HashMap::new();
    let mut meta_changes_to_apply: Vec<Change> = vec![];
    let mut user_changes_to_apply: Vec<Change> = vec![];
    let len = shrunk_entries.entries.len() as u64;
    let mut length = contiguous_length - len;
    for entry in shrunk_entries.entries.into_iter() {
        length += 1;
        match entry.content {
            EntryContent::Change {
                meta, ref change, ..
            } => {
                let change = change.as_ref().unwrap();
                let reserved: Vec<ObjId> = if !meta && !unapplied_entries.reserved_ids.is_empty() {
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
                            if let Ok((obj, _)) = user_automerge_doc.import(obj_id_as_string) {
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
                        .all(|dep| user_automerge_doc.get_change_by_hash(dep).is_some())
                {
                    if meta {
                        meta_changes_to_apply.push(*change.clone());
                    } else {
                        user_changes_to_apply.push(*change.clone());
                    }
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
            EntryContent::InitPeer {
                meta_doc_data,
                user_doc_data,
                ..
            } => {
                // Just merge the data into the current docs. This does not mean that
                // all entries are applied, but they do now end up in what comes out
                // of save() so they aren't lost.
                let mut changed_meta_automerge_doc = AutomergeDoc::load(&meta_doc_data).unwrap();
                meta_automerge_doc
                    .merge(&mut changed_meta_automerge_doc)
                    .unwrap();
                if let Some(user_doc_data) = user_doc_data {
                    let mut changed_user_automerge_doc =
                        AutomergeDoc::load(&user_doc_data).unwrap();
                    user_automerge_doc
                        .merge(&mut changed_user_automerge_doc)
                        .unwrap();
                }

                if let Some(result_value) = result.get_mut(discovery_key) {
                    result_value.set_length(length);
                } else {
                    result.insert(*discovery_key, ApplyEntriesFeedChange::new(length));
                }
            }
            _ => {
                // Because of shrink_entries, there will never be other entries here
                panic!("Unexpected entry {entry:?}");
            }
        }
    }

    // Consolidate unapplied entries and add them to changes and result
    unapplied_entries.consolidate(
        meta_automerge_doc,
        user_automerge_doc,
        &mut meta_changes_to_apply,
        &mut user_changes_to_apply,
        &mut result,
    );

    if !user_changes_to_apply.is_empty() {
        user_automerge_doc.apply_changes(user_changes_to_apply)?;
    }
    Ok(result)
}

/// Tries to apply unapplied entries in given param. Returns what should be persisted.
/// Returns for all of the affected discovery keys that were changed, the length
/// where the cursor should be moved to and/or peer name change.
pub(crate) fn apply_unapplied_entries_autocommit(
    meta_automerge_doc: &mut AutomergeDoc,
    user_automerge_doc: &mut AutomergeDoc,
    unapplied_entries: &mut UnappliedEntries,
) -> Result<HashMap<[u8; 32], ApplyEntriesFeedChange>, PeermergeError> {
    let mut result: HashMap<[u8; 32], ApplyEntriesFeedChange> = HashMap::new();
    let mut meta_changes_to_apply: Vec<Change> = vec![];
    let mut user_changes_to_apply: Vec<Change> = vec![];

    // Consolidate unapplied entries and add them to changes and result
    unapplied_entries.consolidate(
        meta_automerge_doc,
        user_automerge_doc,
        &mut meta_changes_to_apply,
        &mut user_changes_to_apply,
        &mut result,
    );
    if !user_changes_to_apply.is_empty() {
        user_automerge_doc.apply_changes(user_changes_to_apply)?;
    }
    Ok(result)
}

pub(crate) fn transact_mut_autocommit<F, O>(
    meta: bool,
    automerge_doc: &mut AutomergeDoc,
    max_entry_data_size_bytes: usize,
    cb: F,
) -> Result<(Vec<Entry>, O), PeermergeError>
where
    F: FnOnce(&mut AutomergeDoc) -> Result<O, AutomergeError>,
{
    let result = cb(automerge_doc).unwrap();
    let entries: Vec<Entry> = automerge_doc
        .get_last_local_change()
        .map(|change| split_change_into_entries(meta, change.clone(), max_entry_data_size_bytes))
        .unwrap_or_else(Vec::new);
    Ok((entries, result))
}

pub(crate) fn transact_autocommit<F, O>(
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
        automerge::{init_automerge_doc_from_data, init_automerge_docs},
        common::{
            constants::DEFAULT_MAX_ENTRY_DATA_SIZE_BYTES, entry::shrink_entries,
            keys::generate_keys,
        },
        uuid::Uuid,
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
        let mut change = automerge_doc.get_last_local_change().unwrap().clone();
        Ok((Entry::new_change(false, 0, change.bytes().to_vec()), id))
    }

    fn put_scalar_autocommit<O: AsRef<ObjId>, P: Into<Prop>, V: Into<ScalarValue>>(
        automerge_doc: &mut AutomergeDoc,
        obj: O,
        prop: P,
        value: V,
    ) -> Result<Entry, PeermergeError> {
        automerge_doc.put(obj, prop, value)?;
        let mut change = automerge_doc.get_last_local_change().unwrap().clone();
        Ok(Entry::new_change(false, 0, change.bytes().to_vec()))
    }

    #[test]
    fn automerge_edit_apply_entries() -> anyhow::Result<()> {
        let uuid = Uuid::new_v4();
        let peer_id = uuid.as_bytes();
        let int_prop = "number";
        let int_value = 1;
        let (_, doc_discovery_key) = generate_keys();
        let (_, peer_1_discovery_key) = generate_keys();
        let (_, peer_2_discovery_key) = generate_keys();
        let (result, _, _) = init_automerge_docs(
            doc_discovery_key,
            peer_id,
            false,
            DEFAULT_MAX_ENTRY_DATA_SIZE_BYTES,
            |tx| tx.put(ROOT, "version", 1),
        )
        .unwrap();

        let mut meta_doc = result.meta_automerge_doc;
        let mut user_doc = result.user_automerge_doc;

        // Let's create a tree of depth 5
        let (entry_1, key_1) =
            put_object_autocommit(&mut user_doc, ROOT, "level_1", automerge::ObjType::Map)?;
        let (entry_2, key_2) =
            put_object_autocommit(&mut user_doc, &key_1, "level_2", automerge::ObjType::Map)?;
        let (entry_3, key_3) =
            put_object_autocommit(&mut user_doc, &key_2, "level_3", automerge::ObjType::Map)?;
        let (entry_4, key_4) =
            put_object_autocommit(&mut user_doc, &key_3, "level_4", automerge::ObjType::Map)?;
        let (entry_5, key_5) =
            put_object_autocommit(&mut user_doc, &key_4, "level_5", automerge::ObjType::Map)?;
        let entry_scalar = put_scalar_autocommit(&mut user_doc, &key_5, int_prop, int_value)?;

        // Different consolidations
        let mut unapplied_entries = UnappliedEntries::new();

        // All in order, should result in no unapplied entries
        apply_entries_autocommit(
            &mut meta_doc,
            &mut user_doc,
            &doc_discovery_key,
            7,
            shrink_entries(vec![
                entry_1.clone(),
                entry_2.clone(),
                entry_3.clone(),
                entry_4.clone(),
                entry_5.clone(),
                entry_scalar.clone(),
            ]),
            &mut unapplied_entries,
        )?;
        assert_eq!(unapplied_entries.data.len(), 0);
        assert_int_value(&user_doc, &key_5, int_prop, int_value);

        // In chunks
        user_doc = init_automerge_doc_from_data(peer_id, &result.user_doc_data);
        apply_entries_autocommit(
            &mut meta_doc,
            &mut user_doc,
            &doc_discovery_key,
            3,
            shrink_entries(vec![entry_1.clone(), entry_2.clone()]),
            &mut unapplied_entries,
        )?;
        assert_eq!(unapplied_entries.data.len(), 0);
        apply_entries_autocommit(
            &mut meta_doc,
            &mut user_doc,
            &doc_discovery_key,
            5,
            shrink_entries(vec![entry_3.clone(), entry_4.clone()]),
            &mut unapplied_entries,
        )?;
        assert_eq!(unapplied_entries.data.len(), 0);
        apply_entries_autocommit(
            &mut meta_doc,
            &mut user_doc,
            &doc_discovery_key,
            7,
            shrink_entries(vec![entry_5.clone(), entry_scalar.clone()]),
            &mut unapplied_entries,
        )?;
        assert_eq!(unapplied_entries.data.len(), 0);
        assert_int_value(&user_doc, &key_5, int_prop, int_value);

        // Missing first, should first result in all going to unapplied entries, then consolidate
        user_doc = init_automerge_doc_from_data(peer_id, &result.user_doc_data);
        apply_entries_autocommit(
            &mut meta_doc,
            &mut user_doc,
            &doc_discovery_key,
            6,
            shrink_entries(vec![
                entry_2.clone(),
                entry_3.clone(),
                entry_4.clone(),
                entry_5.clone(),
                entry_scalar.clone(),
            ]),
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
            &mut meta_doc,
            &mut user_doc,
            &peer_1_discovery_key,
            1,
            shrink_entries(vec![entry_1.clone()]),
            &mut unapplied_entries,
        )?;
        assert_eq!(unapplied_entries.data.len(), 0);
        assert_int_value(&user_doc, &key_5, int_prop, int_value);

        // Mixture of two peers having every other change
        user_doc = init_automerge_doc_from_data(peer_id, &result.user_doc_data);
        apply_entries_autocommit(
            &mut meta_doc,
            &mut user_doc,
            &peer_1_discovery_key,
            4,
            shrink_entries(vec![entry_2, entry_4, entry_scalar]),
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
            &mut meta_doc,
            &mut user_doc,
            &peer_2_discovery_key,
            3,
            shrink_entries(vec![entry_3, entry_5]),
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
            &mut meta_doc,
            &mut user_doc,
            &doc_discovery_key,
            2,
            shrink_entries(vec![entry_1]),
            &mut unapplied_entries,
        )?;
        assert_eq!(unapplied_entries.data.len(), 0);
        assert_int_value(&user_doc, &key_5, int_prop, int_value);

        Ok(())
    }
}
