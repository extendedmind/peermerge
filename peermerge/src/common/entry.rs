use automerge::Change;

use super::constants::PEERMERGE_VERSION;

/// Content of an entry stored to a hypercore.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum EntryContent {
    /// First message of the doc feed.
    InitDoc {
        /// Number of DocParts coming after this entry. This is > 0
        /// if meta_doc + user_doc > max_chunk_bytes.
        doc_part_count: u32,
        /// Data for meta document. All of metad doc will come first,
        /// and only after that the user document.
        meta_doc_data: Vec<u8>,
        /// Data for user document. This comes after all of meta document
        /// is ready.
        user_doc_data: Option<Vec<u8>>,
    },
    /// First message of a write peer feed.
    InitPeer {
        /// Number of DocPart entries coming after this entry. This is > 0
        /// if meta_doc + user_doc > max_chunk_bytes.
        doc_part_count: u32,
        /// Data for meta document. All of meta doc will come first,
        /// and only after that the user document.
        meta_doc_data: Vec<u8>,
        /// Data for user document. This comes after all of meta document
        /// is ready.
        user_doc_data: Option<Vec<u8>>,
    },
    DocPart {
        /// Index related to doc_part_count in previous InitDoc or InitPeer
        /// entry.
        index: u32,
        /// Data for meta document. All of meta doc will come first,
        /// and only after that the user document.
        meta_doc_data: Option<Vec<u8>>,
        /// Data for user document. This comes after all of meta document
        /// is ready.
        user_doc_data: Option<Vec<u8>>,
    },
    Change {
        /// If true, this is a meta document change, if false, user document.
        meta: bool,
        /// Number of ChangePart entries coming after this entry. This is > 0
        /// if data > max_chunk_bytes.
        part_count: u32,
        /// Data of the change.
        data: Vec<u8>,
        /// Size of the data. Needed when draining the content of the data
        /// field in data_to_change to know that this has happened.
        data_len: usize,
        // Transient change stored into entry on take_change(). None before
        // data_to_change is called.
        change: Option<Box<Change>>,
    },
    ChangePart {
        /// Index related to part_count in previous Change entry.
        index: u32,
        /// Continuation to the data of the change.
        data: Vec<u8>,
    },
}

impl EntryContent {
    pub(crate) fn new_change(meta: bool, part_count: u32, data: Vec<u8>) -> Self {
        let data_len = data.len();
        EntryContent::Change {
            meta,
            part_count,
            data,
            data_len,
            change: None,
        }
    }

    /// Effieciency method to move data into the change field
    pub(crate) fn data_to_change(&mut self) {
        if let EntryContent::Change { data, change, .. } = self {
            let drained_data: Vec<u8> = data.drain(..).collect();
            *change = Some(Box::new(Change::from_bytes(drained_data).unwrap()));
        }
    }
}

/// A document is stored in pieces to hypercores.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Entry {
    pub(crate) version: u8,
    pub(crate) content: EntryContent,
}
impl Entry {
    pub(crate) fn new_init_doc(
        doc_part_count: u32,
        meta_doc_data: Vec<u8>,
        user_doc_data: Option<Vec<u8>>,
    ) -> Self {
        Self::new(
            PEERMERGE_VERSION,
            EntryContent::InitDoc {
                doc_part_count,
                meta_doc_data,
                user_doc_data,
            },
        )
    }

    pub(crate) fn new_init_peer(
        doc_part_count: u32,
        meta_doc_data: Vec<u8>,
        user_doc_data: Option<Vec<u8>>,
    ) -> Self {
        Self::new(
            PEERMERGE_VERSION,
            EntryContent::InitPeer {
                doc_part_count,
                meta_doc_data,
                user_doc_data,
            },
        )
    }

    pub(crate) fn new_doc_part(
        index: u32,
        meta_doc_data: Option<Vec<u8>>,
        user_doc_data: Option<Vec<u8>>,
    ) -> Self {
        Self::new(
            PEERMERGE_VERSION,
            EntryContent::DocPart {
                index,
                meta_doc_data,
                user_doc_data,
            },
        )
    }

    pub(crate) fn new_change(meta: bool, part_count: u32, data: Vec<u8>) -> Self {
        Self::new(
            PEERMERGE_VERSION,
            EntryContent::new_change(meta, part_count, data),
        )
    }

    pub(crate) fn new_change_part(index: u32, data: Vec<u8>) -> Self {
        Self::new(PEERMERGE_VERSION, EntryContent::ChangePart { index, data })
    }

    pub(crate) fn new(version: u8, content: EntryContent) -> Self {
        Self { version, content }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ShrunkEntries {
    // Count of entries shrunk from original entries
    pub(crate) shrunk_count: u64,
    // Entries without DocPart or ChangePart
    pub(crate) entries: Vec<Entry>,
}

pub(crate) fn split_datas_into_entries(
    meta_doc_data: &Vec<u8>,
    user_doc_data: &Option<Vec<u8>>,
    is_init_doc: bool,
    max_chunk_bytes: usize,
) -> Vec<Entry> {
    let mut entries = vec![];
    // First split the meta doc into chunks
    let mut meta_chunks: Vec<Vec<u8>> = meta_doc_data
        .chunks(max_chunk_bytes)
        .map(|s| s.into())
        .collect();

    // Collect all meta-only entry contents
    let mut entry_contents: Vec<(Option<Vec<u8>>, Option<Vec<u8>>)> = vec![];
    for _ in 0..(meta_chunks.len() - 1) {
        let meta_chunk = meta_chunks
            .drain(0..1)
            .collect::<Vec<Vec<u8>>>()
            .into_iter()
            .next()
            .unwrap();
        entry_contents.push((Some(meta_chunk), None));
    }
    let last_meta_chunk: Vec<u8> = meta_chunks
        .drain(0..1)
        .collect::<Vec<Vec<u8>>>()
        .into_iter()
        .next()
        .unwrap();

    if let Some(user_doc_data) = user_doc_data {
        // Split user data into appropriate chunks
        let remaining_space = max_chunk_bytes - last_meta_chunk.len();
        let rest_of_user_doc_data: Option<Vec<u8>> = if remaining_space > 0 {
            if remaining_space < user_doc_data.len() {
                let mut first_user_chunk = user_doc_data.to_vec();
                let rest_of_user_doc_data = first_user_chunk.split_off(remaining_space);
                entry_contents.push((Some(last_meta_chunk), Some(first_user_chunk)));
                Some(rest_of_user_doc_data)
            } else {
                // The entire user doc fits into the same entry as the last meta doc
                entry_contents.push((Some(last_meta_chunk), Some(user_doc_data.to_vec())));
                None
            }
        } else {
            // The chunks fit exactly, add all user chunks into separate entries,
            // and the last meta chunk also to entry contents
            entry_contents.push((Some(last_meta_chunk), None));
            Some(user_doc_data.to_vec())
        };
        if let Some(rest_of_user_doc_data) = rest_of_user_doc_data {
            let user_entry_contents: Vec<(Option<Vec<u8>>, Option<Vec<u8>>)> =
                rest_of_user_doc_data
                    .chunks(max_chunk_bytes)
                    .map(|s| {
                        let user_doc_data: Vec<u8> = s.into();
                        (None, Some(user_doc_data))
                    })
                    .collect();
            entry_contents.extend(user_entry_contents);
        }
    } else {
        entry_contents.push((Some(last_meta_chunk), None));
    }

    let (first_meta_chunk, first_user_chunk) = &entry_contents
        .drain(0..1)
        .collect::<Vec<(Option<Vec<u8>>, Option<Vec<u8>>)>>()
        .into_iter()
        .next()
        .unwrap();

    let doc_part_count: u32 = entry_contents.len().try_into().unwrap();
    if is_init_doc {
        // Init doc
        entries.push(Entry::new_init_doc(
            doc_part_count,
            first_meta_chunk.clone().unwrap(),
            first_user_chunk.clone(),
        ));
    } else {
        // Init peer
        entries.push(Entry::new_init_peer(
            doc_part_count,
            first_meta_chunk.clone().unwrap(),
            first_user_chunk.clone(),
        ));
    }
    for (index, (meta_chunk, user_chunk)) in entry_contents.into_iter().enumerate() {
        entries.push(Entry::new_doc_part(
            index.try_into().unwrap(),
            meta_chunk,
            user_chunk,
        ));
    }
    entries
}

pub(crate) fn split_change_into_entries(
    meta: bool,
    mut change: Change,
    max_chunk_bytes: usize,
) -> Vec<Entry> {
    let data = change.bytes();
    let data_chunks: Vec<Vec<u8>> = data.chunks(max_chunk_bytes).map(|s| s.into()).collect();
    let part_count: u32 = (data_chunks.len() - 1).try_into().unwrap();
    let mut entries: Vec<Entry> = Vec::with_capacity(data_chunks.len());
    for (i, chunk) in data_chunks.into_iter().enumerate() {
        if i == 0 {
            entries.push(Entry::new_change(meta, part_count, chunk));
        } else {
            entries.push(Entry::new_change_part((i - 1).try_into().unwrap(), chunk));
        }
    }
    entries
}

pub(crate) fn shrink_entries(mut entries: Vec<Entry>) -> ShrunkEntries {
    let mut current_master_entry_index: Option<usize> = None;
    let mut is_init_ongoing: bool = false;
    let mut is_change_ongoing: bool = false;
    let mut expected_part_count: u32 = 0;
    let mut current_part_count: u32 = 0;
    let mut shrunk_count: u64 = 0;
    let original_entries_len = entries.len();
    for i in 0..original_entries_len {
        let entries_len = entries.len();
        let master_entry_index = if let Some(master_entry_index) = current_master_entry_index {
            master_entry_index
        } else {
            let index = i - shrunk_count as usize;
            match &entries[index].content {
                EntryContent::InitDoc { doc_part_count, .. } => {
                    expected_part_count = *doc_part_count;
                    is_init_ongoing = true;
                }
                EntryContent::InitPeer { doc_part_count, .. } => {
                    expected_part_count = *doc_part_count;
                    is_init_ongoing = true;
                }
                EntryContent::Change { part_count, .. } => {
                    expected_part_count = *part_count;
                    is_change_ongoing = true;
                }
                _ => panic!("Invalid entries read"),
            }
            current_master_entry_index = Some(index);
            index
        };

        if entries_len > master_entry_index + 1 {
            if matches!(
                &entries[master_entry_index + 1].content,
                EntryContent::DocPart { .. } | EntryContent::ChangePart { .. }
            ) {
                // Drain an entry from after the InitDoc/InitPeer/Change entry
                let part_entry = entries
                    .drain(master_entry_index + 1..master_entry_index + 2)
                    .collect::<Vec<Entry>>()
                    .into_iter()
                    .next()
                    .unwrap();
                let master_entry = entries.get_mut(master_entry_index).unwrap();
                current_part_count += 1;
                match part_entry.content {
                    EntryContent::DocPart {
                        index,
                        meta_doc_data: meta_doc_data_part,
                        user_doc_data: user_doc_data_part,
                    } => {
                        assert_eq!(index, current_part_count - 1);
                        assert!(!is_change_ongoing);
                        assert!(is_init_ongoing);
                        match &mut master_entry.content {
                            EntryContent::InitDoc {
                                meta_doc_data,
                                user_doc_data,
                                ..
                            } => {
                                if let Some(data) = meta_doc_data_part {
                                    meta_doc_data.extend(data);
                                }
                                if let Some(data) = user_doc_data_part {
                                    if let Some(user_doc_data) = user_doc_data.as_mut() {
                                        user_doc_data.extend(data);
                                    } else {
                                        *user_doc_data = Some(data.to_vec());
                                    }
                                }
                            }
                            EntryContent::InitPeer {
                                meta_doc_data,
                                user_doc_data,
                                ..
                            } => {
                                if let Some(data) = meta_doc_data_part {
                                    meta_doc_data.extend(data);
                                }
                                if let Some(data) = user_doc_data_part {
                                    if let Some(user_doc_data) = user_doc_data.as_mut() {
                                        user_doc_data.extend(data);
                                    } else {
                                        *user_doc_data = Some(data.to_vec());
                                    }
                                }
                            }
                            _ => panic!("Should never happen"),
                        }
                    }
                    EntryContent::ChangePart {
                        index,
                        data: data_part,
                    } => {
                        assert_eq!(index, current_part_count - 1);
                        assert!(is_change_ongoing);
                        assert!(!is_init_ongoing);
                        match &mut master_entry.content {
                            EntryContent::Change { data, .. } => {
                                data.extend(data_part);
                            }
                            _ => panic!("Should never happen"),
                        }
                    }
                    _ => panic!("Should never happen"),
                }
                shrunk_count += 1;
                if current_part_count == expected_part_count {
                    current_master_entry_index = None;
                    is_init_ongoing = false;
                    is_change_ongoing = false;
                    current_part_count = 0;
                    expected_part_count = 0;
                }
            } else {
                if is_change_ongoing {
                    // The next one isn't a ChangePart but something else, move data to master
                    // for the Change master entry
                    let change_entry = entries.get_mut(master_entry_index).unwrap();
                    change_entry.content.data_to_change();
                }
                current_master_entry_index = None;
                is_init_ongoing = false;
                is_change_ongoing = false;
                current_part_count = 0;
                expected_part_count = 0;
            }
        } else {
            if is_change_ongoing {
                // There aren't more entries, but the last one was change, move data to master
                let change_entry = entries.get_mut(master_entry_index).unwrap();
                change_entry.content.data_to_change();
            }
            // Stop looping to avoid overflow
            break;
        }
    }
    ShrunkEntries {
        entries,
        shrunk_count,
    }
}

#[cfg(test)]
mod tests {

    use automerge::{transaction::Transactable, ObjId, Prop, ScalarValue, ROOT};

    use crate::{AutomergeDoc, PeermergeError};

    use super::*;

    fn put_scalar_autocommit<O: AsRef<ObjId>, P: Into<Prop>, V: Into<ScalarValue>>(
        automerge_doc: &mut AutomergeDoc,
        obj: O,
        prop: P,
        value: V,
    ) -> Result<Change, PeermergeError> {
        automerge_doc.put(obj, prop, value)?;
        let change = automerge_doc.get_last_local_change().unwrap().clone();
        Ok(change)
    }

    #[test]
    fn entries_split_and_shrink() -> anyhow::Result<()> {
        let mini_meta_doc_data: Vec<u8> = vec![0; 4];
        let mini_user_doc_data: Vec<u8> = vec![1; 4];

        let small_meta_doc_data: Vec<u8> = vec![2; 16];
        let small_user_doc_data: Vec<u8> = vec![3; 16];
        let big_user_doc_data: Vec<u8> = vec![3; 250];

        let mut doc: AutomergeDoc = AutomergeDoc::new();
        let medium_change: Change = put_scalar_autocommit(
            &mut doc,
            ROOT,
            "medium",
            (0..5).map(|_| "X").collect::<String>(),
        )?;
        let big_change: Change = put_scalar_autocommit(
            &mut doc,
            ROOT,
            "big",
            (0..200).map(|_| "X").collect::<String>(),
        )?;
        let big_change_2: Change = put_scalar_autocommit(
            &mut doc,
            ROOT,
            "big2",
            (0..200).map(|_| "Y").collect::<String>(),
        )?;

        // Meta and user fit exactly
        let entries = split_datas_into_entries(
            &mini_meta_doc_data,
            &Some(mini_user_doc_data.clone()),
            true,
            8,
        );
        assert_eq!(
            entries,
            vec![Entry::new_init_doc(
                0,
                mini_meta_doc_data.clone(),
                Some(mini_user_doc_data.clone()),
            )],
        );
        assert_eq!(
            shrink_entries(entries.clone()),
            ShrunkEntries {
                entries,
                shrunk_count: 0
            }
        );

        // Both exactly to two different parts
        let entries = split_datas_into_entries(
            &mini_meta_doc_data,
            &Some(mini_user_doc_data.clone()),
            true,
            4,
        );
        assert_eq!(
            entries,
            vec![
                Entry::new_init_doc(1, mini_meta_doc_data.clone(), None),
                Entry::new_doc_part(0, None, Some(mini_user_doc_data.clone()))
            ],
        );
        assert_eq!(
            shrink_entries(entries),
            ShrunkEntries {
                entries: vec![Entry::new_init_doc(
                    1,
                    mini_meta_doc_data.clone(),
                    Some(mini_user_doc_data.clone())
                )],
                shrunk_count: 1
            },
        );

        // Meta overflows
        let entries = split_datas_into_entries(
            &small_meta_doc_data,
            &Some(mini_user_doc_data.clone()),
            false,
            10,
        );
        assert_eq!(
            entries,
            vec![
                Entry::new_init_peer(1, small_meta_doc_data[..10].to_vec(), None),
                Entry::new_doc_part(
                    0,
                    Some(small_meta_doc_data[10..].to_vec()),
                    Some(mini_user_doc_data.clone())
                )
            ],
        );
        assert_eq!(
            shrink_entries(entries),
            ShrunkEntries {
                entries: vec![Entry::new_init_peer(
                    1,
                    small_meta_doc_data.clone(),
                    Some(mini_user_doc_data.clone())
                )],
                shrunk_count: 1
            },
        );

        // User overflows
        let entries = split_datas_into_entries(
            &mini_meta_doc_data,
            &Some(small_user_doc_data.clone()),
            false,
            10,
        );
        assert_eq!(
            entries,
            vec![
                Entry::new_init_peer(
                    1,
                    mini_meta_doc_data.clone(),
                    Some(small_user_doc_data[..6].to_vec())
                ),
                Entry::new_doc_part(0, None, Some(small_user_doc_data[6..].to_vec()))
            ],
        );
        assert_eq!(
            shrink_entries(entries),
            ShrunkEntries {
                entries: vec![Entry::new_init_peer(
                    1,
                    mini_meta_doc_data.clone(),
                    Some(small_user_doc_data.clone())
                )],
                shrunk_count: 1
            },
        );

        // Both overflow
        let entries = split_datas_into_entries(
            &small_meta_doc_data,
            &Some(small_user_doc_data.clone()),
            false,
            5,
        );
        assert_eq!(
            entries,
            vec![
                Entry::new_init_peer(6, small_meta_doc_data[..5].to_vec(), None),
                Entry::new_doc_part(0, Some(small_meta_doc_data[5..10].to_vec()), None),
                Entry::new_doc_part(1, Some(small_meta_doc_data[10..15].to_vec()), None),
                Entry::new_doc_part(
                    2,
                    Some(small_meta_doc_data[15..16].to_vec()),
                    Some(small_user_doc_data[0..4].to_vec())
                ),
                Entry::new_doc_part(3, None, Some(small_user_doc_data[4..9].to_vec())),
                Entry::new_doc_part(4, None, Some(small_user_doc_data[9..14].to_vec())),
                Entry::new_doc_part(5, None, Some(small_user_doc_data[14..16].to_vec())),
            ],
        );
        assert_eq!(
            shrink_entries(entries),
            ShrunkEntries {
                entries: vec![Entry::new_init_peer(
                    6,
                    small_meta_doc_data,
                    Some(small_user_doc_data)
                )],
                shrunk_count: 6
            },
        );

        // Changes after init, no overflow
        let mut entries = split_datas_into_entries(
            &mini_meta_doc_data,
            &Some(mini_user_doc_data.clone()),
            true,
            8,
        );
        entries.extend(split_change_into_entries(
            false,
            medium_change.clone(),
            100, // this change is 65 bytes at the time of writing
        ));
        assert_eq!(entries.len(), 2);
        let shrunk_entries = shrink_entries(entries);
        assert_eq!(shrunk_entries.shrunk_count, 0);
        if let EntryContent::Change { change, .. } = &shrunk_entries.entries[1].content {
            assert!(change.is_some())
        } else {
            panic!("Invalid entry {:?}", shrunk_entries.entries[1]);
        }

        // User overflowing init, change fits
        let mut entries = split_datas_into_entries(
            &mini_meta_doc_data,
            &Some(big_user_doc_data.clone()),
            true,
            100,
        );
        entries.extend(split_change_into_entries(
            false,
            medium_change.clone(),
            100, // this change is 65 bytes at the time of writing
        ));
        assert_eq!(entries.len(), 4);
        let shrunk_entries = shrink_entries(entries);
        assert_eq!(shrunk_entries.shrunk_count, 2);
        if let EntryContent::Change { change, .. } = &shrunk_entries.entries[1].content {
            assert!(change.is_some())
        } else {
            panic!("Invalid entry {:?}", shrunk_entries.entries[1]);
        }

        // User and change overflowing
        let mut entries = split_datas_into_entries(
            &mini_meta_doc_data,
            &Some(big_user_doc_data.clone()),
            true,
            100,
        );
        entries.extend(split_change_into_entries(
            false,
            big_change.clone(),
            100, // this change is 65 bytes at the time of writing
        ));
        assert_eq!(entries.len(), 5);

        let shrunk_entries = shrink_entries(entries);
        assert_eq!(shrunk_entries.shrunk_count, 3);
        if let EntryContent::Change { change, .. } = &shrunk_entries.entries[1].content {
            assert!(change.is_some())
        } else {
            panic!("Invalid entry {:?}", shrunk_entries.entries[1]);
        }

        // User and two changes overflowing with smaller change in between
        let mut entries =
            split_datas_into_entries(&mini_meta_doc_data, &Some(big_user_doc_data), true, 100);
        entries.extend(split_change_into_entries(
            false, big_change, 100, // this change is 65 bytes at the time of writing
        ));
        entries.extend(split_change_into_entries(
            false,
            medium_change,
            100, // this change is 65 bytes at the time of writing
        ));
        entries.extend(split_change_into_entries(
            false,
            big_change_2,
            100, // this change is 65 bytes at the time of writing
        ));
        assert_eq!(entries.len(), 8);

        let shrunk_entries = shrink_entries(entries);
        assert_eq!(shrunk_entries.shrunk_count, 4);
        if let EntryContent::Change { change, .. } = &shrunk_entries.entries[1].content {
            assert!(change.is_some())
        } else {
            panic!("Invalid entry {:?}", shrunk_entries.entries[1]);
        }
        if let EntryContent::Change { change, .. } = &shrunk_entries.entries[2].content {
            assert!(change.is_some())
        } else {
            panic!("Invalid entry {:?}", shrunk_entries.entries[1]);
        }
        if let EntryContent::Change { change, .. } = &shrunk_entries.entries[3].content {
            assert!(change.is_some())
        } else {
            panic!("Invalid entry {:?}", shrunk_entries.entries[1]);
        }

        Ok(())
    }
}
