use std::convert::TryFrom;

use automerge::{transaction::Transactable, Change, ObjId, ObjType, Prop, ScalarValue};

use super::AutomergeDoc;
use crate::common::entry::{Entry, EntryType};

pub(crate) fn apply_changes_autocommit(
    doc: &mut AutomergeDoc,
    changes: Vec<Entry>,
) -> anyhow::Result<()> {
    let changes: Vec<Change> = changes
        .iter()
        .filter(|entry| entry.entry_type != EntryType::InitPeer)
        .map(|entry| Change::try_from(&entry.data[..]).unwrap())
        .collect();
    Ok(doc.apply_changes(changes).unwrap())
}

pub(crate) fn put_object_autocommit<O: AsRef<ObjId>, P: Into<Prop>>(
    doc: &mut AutomergeDoc,
    obj: O,
    prop: P,
    object: ObjType,
) -> anyhow::Result<(Entry, ObjId)> {
    let id = doc.put_object(obj, prop, object)?;
    let change = doc
        .get_last_local_change()
        .unwrap()
        .clone()
        .bytes()
        .to_vec();
    Ok((Entry::new_change(change), id))
}

pub(crate) fn put_scalar_autocommit<O: AsRef<ObjId>, P: Into<Prop>, V: Into<ScalarValue>>(
    doc: &mut AutomergeDoc,
    obj: O,
    prop: P,
    value: V,
) -> anyhow::Result<Entry> {
    doc.put(obj, prop, value)?;
    let change = doc
        .get_last_local_change()
        .unwrap()
        .clone()
        .bytes()
        .to_vec();
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
    let change = doc
        .get_last_local_change()
        .unwrap()
        .clone()
        .bytes()
        .to_vec();
    Ok(Entry::new_change(change))
}
