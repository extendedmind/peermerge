use automerge::{transaction::Transactable, ObjId, ObjType, Prop};

use super::AutomergeDoc;
use crate::common::entry::Entry;

pub(crate) fn apply_changes_autocommit(doc: &mut AutomergeDoc, changes: Vec<Entry>) {
    // doc.apply_changes();
}

pub(crate) fn put_object_autocommit<O: AsRef<ObjId>, P: Into<Prop>>(
    doc: &mut AutomergeDoc,
    obj: O,
    prop: P,
    object: ObjType,
) -> anyhow::Result<Entry> {
    doc.put_object(obj, prop, object).unwrap();
    let change = doc
        .get_last_local_change()
        .unwrap()
        .clone()
        .bytes()
        .to_vec();
    Ok(Entry::new_change(change))
}
