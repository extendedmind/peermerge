use automerge::{
    transaction::{CommitOptions, Transactable, Transaction},
    ActorId, AutoCommit, Automerge, AutomergeError, ReadDoc, ROOT,
};
use std::collections::HashMap;

use super::{apply_entries_autocommit, ApplyEntriesFeedChange, AutomergeDoc, UnappliedEntries};
use crate::{
    common::entry::{Entry, EntryContent},
    common::{
        constants::{MAX_DATA_CHUNK_BYTES, PEERMERGE_VERSION},
        entry::split_datas_into_entries,
    },
    encode_base64_nopad,
    feed::FeedDiscoveryKey,
    DocumentId, NameDescription, PeerId, PeermergeError,
};

pub(crate) struct InitAutomergeDocsResult {
    pub(crate) meta_automerge_doc: AutomergeDoc,
    pub(crate) user_automerge_doc: AutomergeDoc,
    pub(crate) meta_doc_data: Vec<u8>,
    pub(crate) user_doc_data: Vec<u8>,
}

pub(crate) fn init_automerge_docs<F, O>(
    document_id: DocumentId,
    write_peer_id: &PeerId,
    child: bool,
    init_cb: F,
) -> Result<(InitAutomergeDocsResult, O, Vec<Entry>), PeermergeError>
where
    F: FnOnce(&mut Transaction) -> Result<O, AutomergeError>,
{
    let actor_id = generate_actor_id(write_peer_id);

    let mut meta_automerge_doc = Automerge::new().with_actor(actor_id.clone());
    meta_automerge_doc
        .transact_with::<_, _, AutomergeError, _>(
            |_| CommitOptions::default().with_message(format!("init:{PEERMERGE_VERSION}")),
            |tx| {
                // Use short keys because this doc needs to be stored to the doc URL.
                tx.put(ROOT, "v", PEERMERGE_VERSION as i32)?;
                tx.put(ROOT, "i", document_id.to_vec())?;
                // Document header
                tx.put_object(ROOT, "h", automerge::ObjType::Map)?;
                // Peers map
                tx.put_object(ROOT, "p", automerge::ObjType::Map)?;
                if child {
                    // pArents map
                    tx.put_object(ROOT, "a", automerge::ObjType::Map)?;
                }
                Ok(())
            },
        )
        .unwrap();
    let meta_doc_data = meta_automerge_doc.save();
    let meta_automerge_doc: AutoCommit = AutoCommit::load(&meta_doc_data).unwrap();

    let mut user_automerge_doc = Automerge::new().with_actor(actor_id);
    let result = user_automerge_doc
        .transact_with::<_, _, AutomergeError, _>(
            |_| CommitOptions::default().with_message(format!("init:{PEERMERGE_VERSION}")),
            init_cb,
        )
        .unwrap()
        .result;
    let user_doc_data = user_automerge_doc.save();
    let user_automerge_doc: AutoCommit = AutoCommit::load(&user_doc_data).unwrap();

    let entries = split_datas_into_entries(
        &meta_doc_data,
        &Some(user_doc_data.clone()),
        true,
        MAX_DATA_CHUNK_BYTES,
    );
    Ok((
        InitAutomergeDocsResult {
            meta_automerge_doc,
            user_automerge_doc,
            meta_doc_data,
            user_doc_data,
        },
        result,
        entries,
    ))
}

pub(crate) fn init_first_peer(
    meta_automerge_doc: &mut AutomergeDoc,
    peer_id: &PeerId,
    peer_header: &NameDescription,
    document_type: &str,
    document_header: &Option<NameDescription>,
) -> Result<Vec<Entry>, PeermergeError> {
    let peers_id = meta_automerge_doc
        .get(ROOT, "p")
        .unwrap()
        .map(|result| result.1)
        .unwrap();
    let peer_key = encode_base64_nopad(peer_id);
    let peer_header_id =
        meta_automerge_doc.put_object(&peers_id, peer_key, automerge::ObjType::Map)?;
    meta_automerge_doc.put(&peer_header_id, "n", &peer_header.name)?;
    if let Some(description) = &peer_header.description {
        meta_automerge_doc.put(&peer_header_id, "d", description)?;
    }
    let document_header_id = meta_automerge_doc
        .get(ROOT, "h")
        .unwrap()
        .map(|result| result.1)
        .unwrap();
    meta_automerge_doc.put(&document_header_id, "t", document_type)?;
    if let Some(document_header) = &document_header {
        meta_automerge_doc.put(&document_header_id, "n", &document_header.name)?;
        if let Some(description) = &document_header.description {
            meta_automerge_doc.put(&document_header_id, "d", description)?;
        }
    }
    let meta_doc_data = save_automerge_doc(meta_automerge_doc);
    meta_automerge_doc.update_diff_cursor();
    let entries = split_datas_into_entries(&meta_doc_data, &None, false, MAX_DATA_CHUNK_BYTES);
    Ok(entries)
}

pub(crate) fn init_peer(
    meta_automerge_doc: &mut AutomergeDoc,
    user_automerge_doc: Option<&mut AutomergeDoc>,
    peer_id: &PeerId,
    peer_header: &Option<NameDescription>,
) -> Result<Vec<Entry>, PeermergeError> {
    let peers_id = meta_automerge_doc
        .get(ROOT, "p")
        .unwrap()
        .map(|result| result.1)
        .unwrap();
    let peer_key = encode_base64_nopad(peer_id);
    let mut peer_id_keys = meta_automerge_doc.keys(&peers_id);
    if !peer_id_keys.any(|key| key == peer_key) {
        if let Some(peer_header) = peer_header {
            let peer_header_id =
                meta_automerge_doc.put_object(&peers_id, peer_key, automerge::ObjType::Map)?;
            meta_automerge_doc.put(&peer_header_id, "n", &peer_header.name)?;
            if let Some(description) = &peer_header.description {
                meta_automerge_doc.put(&peer_header_id, "d", description)?;
            }
            meta_automerge_doc.update_diff_cursor();
        } else {
            panic!("Need to be able to set a name to missing peer");
        }
    }
    let meta_doc_data = save_automerge_doc(meta_automerge_doc);
    let user_doc_data: Option<Vec<u8>> = if let Some(user_automerge_doc) = user_automerge_doc {
        Some(save_automerge_doc(user_automerge_doc))
    } else {
        None
    };
    meta_automerge_doc.update_diff_cursor();
    let entries =
        split_datas_into_entries(&meta_doc_data, &user_doc_data, false, MAX_DATA_CHUNK_BYTES);
    Ok(entries)
}

pub(crate) struct BootstrapAutomergeUserDocResult {
    pub(crate) user_automerge_doc: AutomergeDoc,
    pub(crate) user_doc_data: Vec<u8>,
    pub(crate) meta_doc_data: Vec<u8>,
}

pub(crate) fn bootstrap_automerge_user_doc_from_entries(
    meta_automerge_doc: &mut AutomergeDoc,
    write_peer_id: &PeerId,
    synced_discovery_key: &FeedDiscoveryKey,
    synced_contiguous_length: u64,
    mut entries: Vec<Entry>,
    entries_original_offset: u64,
    unapplied_entries: &mut UnappliedEntries,
) -> Result<
    (
        BootstrapAutomergeUserDocResult,
        HashMap<[u8; 32], ApplyEntriesFeedChange>,
    ),
    PeermergeError,
> {
    assert!(!entries.is_empty());
    let init_entry = entries
        .drain(0..1)
        .collect::<Vec<Entry>>()
        .into_iter()
        .next()
        .unwrap();
    let (doc_part_count, meta_doc_data, user_doc_data) = match init_entry.content {
        EntryContent::InitDoc {
            doc_part_count,
            meta_doc_data,
            user_doc_data,
        } => (
            doc_part_count,
            meta_doc_data,
            user_doc_data.expect("User doc needs to exist in initial doc entries"),
        ),
        EntryContent::InitPeer {
            doc_part_count,
            meta_doc_data,
            user_doc_data,
        } => (
            doc_part_count,
            meta_doc_data,
            user_doc_data.expect("User doc needs to exist in initial peer entries"),
        ),
        _ => panic!("Invalid init entries"),
    };
    assert_eq!(entries_original_offset, doc_part_count as u64);
    let actor_id = generate_actor_id(write_peer_id);
    let mut user_automerge_doc =
        init_automerge_doc_from_data_with_actor_id(actor_id, &user_doc_data);
    let mut changed_meta_automerge_doc = AutomergeDoc::load(&meta_doc_data).unwrap();
    meta_automerge_doc
        .merge(&mut changed_meta_automerge_doc)
        .unwrap();

    let mut result = apply_entries_autocommit(
        meta_automerge_doc,
        &mut user_automerge_doc,
        synced_discovery_key,
        synced_contiguous_length,
        entries,
        unapplied_entries,
    )?;
    if !result.contains_key(synced_discovery_key) {
        // If there weren't other entries after the init entries that caused the
        // feed change to go further, then push the feed change at least pass
        // the original values.
        result.insert(
            *synced_discovery_key,
            ApplyEntriesFeedChange::new(1 + entries_original_offset),
        );
    }
    let meta_doc_data = save_automerge_doc(meta_automerge_doc);
    let user_doc_data = save_automerge_doc(&mut user_automerge_doc);
    Ok((
        BootstrapAutomergeUserDocResult {
            user_automerge_doc,
            meta_doc_data,
            user_doc_data,
        },
        result,
    ))
}

pub(crate) fn init_automerge_doc_from_data(write_peer_id: &PeerId, data: &[u8]) -> AutomergeDoc {
    let actor_id = generate_actor_id(write_peer_id);
    init_automerge_doc_from_data_with_actor_id(actor_id, data)
}

pub(crate) fn save_automerge_doc(automerge_doc: &mut AutomergeDoc) -> Vec<u8> {
    automerge_doc.save()
}

fn init_automerge_doc_from_data_with_actor_id(actor_id: ActorId, data: &[u8]) -> AutomergeDoc {
    let automerge_doc = AutoCommit::load(data).unwrap();
    let mut doc = automerge_doc.with_actor(actor_id);
    // Update the diff to the head
    doc.update_diff_cursor();
    doc
}

fn generate_actor_id(write_peer_id: &PeerId) -> ActorId {
    ActorId::from(write_peer_id.to_vec())
}
