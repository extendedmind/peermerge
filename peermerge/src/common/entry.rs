use automerge::Change;

use super::constants::PEERMERGE_VERSION;

/// Content of an entry stored to a hypercore.
#[derive(Debug, Clone)]
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
        /// message.
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
        change: Box<Change>,
        data: Vec<u8>,
    },
}

impl EntryContent {
    pub(crate) fn new_change(meta: bool, data: Vec<u8>) -> Self {
        let change = Change::from_bytes(data.clone()).unwrap();
        EntryContent::Change {
            meta,
            change: Box::new(change),
            data,
        }
    }
}

/// A document is stored in pieces to hypercores.
#[derive(Debug, Clone)]
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

    pub(crate) fn new_change(meta: bool, mut change: Change) -> Self {
        let data = change.bytes().to_vec();
        Self::new(
            PEERMERGE_VERSION,
            EntryContent::Change {
                meta,
                data,
                change: Box::new(change),
            },
        )
    }

    pub(crate) fn new(version: u8, content: EntryContent) -> Self {
        Self { version, content }
    }
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
            // The chunks fit exactly, add all user chunks into separate entries
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

pub(crate) fn shrink_entries(mut entries: Vec<Entry>) -> (Vec<Entry>, u64) {
    if !is_init_entries(&entries) {
        return (entries, 0);
    }
    let mut total_part_count: u32 = 0;
    let mut part_count: u32 = 0;
    let mut full_meta_doc_data: Vec<u8> = vec![];
    let mut full_user_doc_data: Vec<u8> = vec![];
    let original_entries_len = entries.len();
    for i in 0..original_entries_len {
        if i == 0 {
            match &entries[0].content {
                EntryContent::InitDoc {
                    doc_part_count,
                    meta_doc_data,
                    user_doc_data,
                } => {
                    total_part_count = *doc_part_count;
                    full_meta_doc_data.extend(meta_doc_data);
                    if let Some(data) = user_doc_data {
                        full_user_doc_data.extend(data);
                    }
                }
                EntryContent::InitPeer {
                    doc_part_count,
                    meta_doc_data,
                    user_doc_data,
                } => {
                    total_part_count = *doc_part_count;
                    full_meta_doc_data.extend(meta_doc_data);
                    if let Some(data) = user_doc_data {
                        full_user_doc_data.extend(data);
                    }
                }
                _ => panic!("Should never happen"),
            };
        } else if entries.len() > 1 {
            let doc_part: bool = matches!(&entries[1].content, EntryContent::DocPart { .. });
            if doc_part {
                let part_entry = entries
                    .drain(i..i + 1)
                    .collect::<Vec<Entry>>()
                    .into_iter()
                    .next()
                    .unwrap();
                let init_entry = entries.get_mut(0).unwrap();
                match part_entry.content {
                    EntryContent::DocPart {
                        index,
                        meta_doc_data: meta_doc_data_part,
                        user_doc_data: user_doc_data_part,
                    } => {
                        part_count += 1;
                        assert_eq!(index, i as u32 - 1);
                        match &mut init_entry.content {
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
                    _ => panic!("Should never happen"),
                }
            }
        } else {
            break;
        }
    }
    assert_eq!(total_part_count, part_count);
    (entries, total_part_count.into())
}

fn is_init_entries(entries: &Vec<Entry>) -> bool {
    if entries.is_empty() {
        return false;
    }
    matches!(
        entries[0].content,
        EntryContent::InitDoc { .. } | EntryContent::InitPeer { .. }
    )
}
