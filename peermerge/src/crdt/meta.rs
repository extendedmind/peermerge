use automerge::{
    transaction::{CommitOptions, Transactable},
    ActorId, AutoCommit, Automerge, AutomergeError, ReadDoc, ROOT,
};

use crate::{
    common::constants::PEERMERGE_VERSION, encode_base64_nopad, AutomergeDoc, DocumentId,
    NameDescription, PeerId, PeermergeError,
};

// Use short keys because theys keys are stored to the doc URL.
const VERSION_KEY: &str = "v";
const DOCUMENT_HEADER_KEY: &str = "h";
const DOCUMENT_TYPE_KEY: &str = "t";
const DOCUMENT_ID_KEY: &str = "i";
const PEERS_MAP_KEY: &str = "p";
const PARENTS_MAP_KEY: &str = "a";
const NAME_KEY: &str = "n";
const DESCRIPTION_KEY: &str = "d";
const DOCUMENTS_MAP_KEY: &str = "o";
const DOCUMENT_SECRET_KEY: &str = "s";

pub(super) fn init_meta_automerge_doc(
    actor_id: &ActorId,
    document_id: DocumentId,
    child: bool,
) -> (AutomergeDoc, Vec<u8>) {
    let mut meta_automerge_doc = Automerge::new().with_actor(actor_id.clone());
    meta_automerge_doc
        .transact_with::<_, _, AutomergeError, _>(
            |_| CommitOptions::default().with_message(format!("init:{PEERMERGE_VERSION}")),
            |tx| {
                tx.put(ROOT, VERSION_KEY, PEERMERGE_VERSION as i32)?;
                tx.put(ROOT, DOCUMENT_ID_KEY, document_id.to_vec())?;
                // Document header
                tx.put_object(ROOT, DOCUMENT_HEADER_KEY, automerge::ObjType::Map)?;
                // Peers map
                tx.put_object(ROOT, PEERS_MAP_KEY, automerge::ObjType::Map)?;
                // Child documents. NB: Even though initially grand-children are not allowed,
                // there is no reason to make it technically impossible to implement in the
                // future.
                tx.put_object(ROOT, DOCUMENTS_MAP_KEY, automerge::ObjType::Map)?;
                if child {
                    // Parents map
                    let parents_id =
                        tx.put_object(ROOT, PARENTS_MAP_KEY, automerge::ObjType::Map)?;
                    // Parent documents map
                    tx.put_object(&parents_id, DOCUMENTS_MAP_KEY, automerge::ObjType::Map)?;
                    // Parent peers map
                    tx.put_object(&parents_id, PEERS_MAP_KEY, automerge::ObjType::Map)?;
                }
                Ok(())
            },
        )
        .unwrap();
    let meta_doc_data = meta_automerge_doc.save();
    let meta_automerge_doc: AutoCommit = AutoCommit::load(&meta_doc_data).unwrap();
    (meta_automerge_doc, meta_doc_data)
}

pub(super) fn save_first_peer(
    meta_automerge_doc: &mut AutomergeDoc,
    peer_id: &PeerId,
    peer_header: &NameDescription,
    document_type: &str,
    document_header: &Option<NameDescription>,
    parent_id: Option<DocumentId>,
    parent_header: Option<NameDescription>,
) -> Result<(), PeermergeError> {
    let peers_id = meta_automerge_doc
        .get(ROOT, PEERS_MAP_KEY)
        .unwrap()
        .map(|result| result.1)
        .unwrap();
    let peer_key = encode_base64_nopad(peer_id);
    let peer_header_id =
        meta_automerge_doc.put_object(&peers_id, &peer_key, automerge::ObjType::Map)?;
    meta_automerge_doc.put(&peer_header_id, NAME_KEY, &peer_header.name)?;
    if let Some(description) = &peer_header.description {
        meta_automerge_doc.put(&peer_header_id, DESCRIPTION_KEY, description)?;
    }
    let document_header_id = meta_automerge_doc
        .get(ROOT, DOCUMENT_HEADER_KEY)
        .unwrap()
        .map(|result| result.1)
        .unwrap();
    meta_automerge_doc.put(&document_header_id, DOCUMENT_TYPE_KEY, document_type)?;
    if let Some(document_header) = &document_header {
        meta_automerge_doc.put(&document_header_id, NAME_KEY, &document_header.name)?;
        if let Some(description) = &document_header.description {
            meta_automerge_doc.put(&document_header_id, DESCRIPTION_KEY, description)?;
        }
    }
    if let Some(parent_id) = parent_id {
        let parent_header =
            parent_header.expect("Parent header must always be present when parent id is");
        let parents_id = meta_automerge_doc
            .get(ROOT, PARENTS_MAP_KEY)
            .unwrap()
            .map(|result| result.1)
            .unwrap();
        let parents_documents_id = meta_automerge_doc
            .get(&parents_id, DOCUMENTS_MAP_KEY)
            .unwrap()
            .map(|result| result.1)
            .unwrap();
        let parent_key = encode_base64_nopad(&parent_id);
        let parent_document_id = meta_automerge_doc.put_object(
            &parents_documents_id,
            parent_key,
            automerge::ObjType::Map,
        )?;
        meta_automerge_doc.put(&parent_document_id, NAME_KEY, &parent_header.name)?;
        if let Some(description) = &parent_header.description {
            meta_automerge_doc.put(&parent_document_id, DESCRIPTION_KEY, description)?;
        }
        let parents_peers_id = meta_automerge_doc
            .get(&parents_id, PEERS_MAP_KEY)
            .unwrap()
            .map(|result| result.1)
            .unwrap();
        let parent_peer_id =
            meta_automerge_doc.put_object(&parents_peers_id, &peer_key, automerge::ObjType::Map)?;
        meta_automerge_doc.put(&parent_peer_id, DOCUMENT_ID_KEY, parent_id.to_vec())?;
    }
    Ok(())
}

pub(crate) fn save_child_document(
    meta_automerge_doc: &mut AutomergeDoc,
    child_document_id: DocumentId,
    child_document_secret: Vec<u8>,
) -> Result<(), PeermergeError> {
    let child_documents_id = meta_automerge_doc
        .get(ROOT, DOCUMENTS_MAP_KEY)
        .unwrap()
        .map(|result| result.1)
        .unwrap();
    let child_key = encode_base64_nopad(&child_document_id);
    let child_document_id =
        meta_automerge_doc.put_object(&child_documents_id, child_key, automerge::ObjType::Map)?;
    meta_automerge_doc.put(
        &child_document_id,
        DOCUMENT_SECRET_KEY,
        child_document_secret,
    )?;
    Ok(())
}

pub(crate) fn read_document_type_and_header(
    meta_automerge_doc: &AutomergeDoc,
) -> Option<(String, Option<NameDescription>)> {
    let document_header_id = meta_automerge_doc
        .get(ROOT, DOCUMENT_HEADER_KEY)
        .unwrap()
        .map(|result| result.1)
        .unwrap();

    let document_header_keys: Vec<_> = meta_automerge_doc.keys(&document_header_id).collect();
    if document_header_keys
        .iter()
        .any(|key| key == DOCUMENT_TYPE_KEY)
    {
        let document_type: String = meta_automerge_doc
            .get(&document_header_id, DOCUMENT_TYPE_KEY)
            .unwrap()
            .and_then(|result| result.0.to_scalar().cloned())
            .unwrap()
            .into_string()
            .unwrap();
        let document_header: Option<NameDescription> = meta_automerge_doc
            .get(&document_header_id, NAME_KEY)
            .unwrap()
            .and_then(|result| result.0.to_scalar().cloned())
            .map(|name_scalar_value| {
                let name = name_scalar_value.into_string().unwrap();
                let description: Option<String> = meta_automerge_doc
                    .get(&document_header_id, DESCRIPTION_KEY)
                    .unwrap()
                    .and_then(|result| result.0.to_scalar().cloned())
                    .map(|description_scalar_value| {
                        description_scalar_value.into_string().unwrap()
                    });
                NameDescription { name, description }
            });
        Some((document_type, document_header))
    } else {
        None
    }
}

pub(crate) fn read_peer_header(
    meta_automerge_doc: &AutomergeDoc,
    peer_id: &PeerId,
) -> Option<NameDescription> {
    let peers_id = meta_automerge_doc
        .get(ROOT, PEERS_MAP_KEY)
        .unwrap()
        .map(|result| result.1)
        .unwrap();
    let peer_key = encode_base64_nopad(peer_id);
    let mut peers_id_keys = meta_automerge_doc.keys(&peers_id);
    if peers_id_keys.any(|key| key == peer_key) {
        let peer_id = meta_automerge_doc
            .get(&peers_id, peer_key)
            .unwrap()
            .map(|result| result.1)
            .unwrap();
        if let Some(name_scalar_value) = meta_automerge_doc
            .get(&peer_id, NAME_KEY)
            .unwrap()
            .and_then(|result| result.0.to_scalar().cloned())
        {
            let name: String = name_scalar_value.into_string().unwrap();
            let description: Option<String> = meta_automerge_doc
                .get(&peer_id, DESCRIPTION_KEY)
                .unwrap()
                .and_then(|result| result.0.to_scalar().cloned())
                .map(|description_scalar_value| description_scalar_value.into_string().unwrap());
            Some(NameDescription { name, description })
        } else {
            None
        }
    } else {
        None
    }
}
