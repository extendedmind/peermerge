use automerge::{
    transaction::{CommitOptions, Transactable},
    ActorId, AutoCommit, Automerge, AutomergeError, ReadDoc, ROOT,
};

use crate::{
    common::constants::PEERMERGE_VERSION, encode_base64_nopad, AutomergeDoc, DocumentId,
    NameDescription, PeerId, PeermergeError,
};

const VERSION_KEY: &str = "v";
const DOCUMENT_HEADER_KEY: &str = "h";
const DOCUMENT_ID_KEY: &str = "i";
const PEERS_MAP_KEY: &str = "p";
const PARENTS_MAP_KEY: &str = "a";
const NAME_KEY: &str = "n";
const DESCRIPTION_KEY: &str = "d";

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
                // Use short keys because this doc needs to be stored to the doc URL.
                tx.put(ROOT, VERSION_KEY, PEERMERGE_VERSION as i32)?;
                tx.put(ROOT, DOCUMENT_ID_KEY, document_id.to_vec())?;
                // Document header
                tx.put_object(ROOT, DOCUMENT_HEADER_KEY, automerge::ObjType::Map)?;
                // Peers map
                tx.put_object(ROOT, PEERS_MAP_KEY, automerge::ObjType::Map)?;
                if child {
                    // Parents map
                    tx.put_object(ROOT, PARENTS_MAP_KEY, automerge::ObjType::Map)?;
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
) -> Result<(), PeermergeError> {
    let peers_id = meta_automerge_doc
        .get(ROOT, PEERS_MAP_KEY)
        .unwrap()
        .map(|result| result.1)
        .unwrap();
    let peer_key = encode_base64_nopad(peer_id);
    let peer_header_id =
        meta_automerge_doc.put_object(&peers_id, peer_key, automerge::ObjType::Map)?;
    meta_automerge_doc.put(&peer_header_id, NAME_KEY, &peer_header.name)?;
    if let Some(description) = &peer_header.description {
        meta_automerge_doc.put(&peer_header_id, DESCRIPTION_KEY, description)?;
    }
    let document_header_id = meta_automerge_doc
        .get(ROOT, DOCUMENT_HEADER_KEY)
        .unwrap()
        .map(|result| result.1)
        .unwrap();
    meta_automerge_doc.put(&document_header_id, "t", document_type)?;
    if let Some(document_header) = &document_header {
        meta_automerge_doc.put(&document_header_id, NAME_KEY, &document_header.name)?;
        if let Some(description) = &document_header.description {
            meta_automerge_doc.put(&document_header_id, DESCRIPTION_KEY, description)?;
        }
    }
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
    if document_header_keys.iter().any(|key| key == "t") {
        let document_type: String = meta_automerge_doc
            .get(&document_header_id, "t")
            .unwrap()
            .and_then(|result| result.0.to_scalar().cloned())
            .unwrap()
            .into_string()
            .unwrap();

        let document_header: Option<NameDescription> =
            if document_header_keys.iter().any(|key| key == NAME_KEY) {
                let name: String = meta_automerge_doc
                    .get(&document_header_id, NAME_KEY)
                    .unwrap()
                    .and_then(|result| result.0.to_scalar().cloned())
                    .unwrap()
                    .into_string()
                    .unwrap();
                let description: Option<String> = if document_header_keys
                    .iter()
                    .any(|key| key == DESCRIPTION_KEY)
                {
                    let description: String = meta_automerge_doc
                        .get(&document_header_id, DESCRIPTION_KEY)
                        .unwrap()
                        .and_then(|result| result.0.to_scalar().cloned())
                        .unwrap()
                        .into_string()
                        .unwrap();
                    Some(description)
                } else {
                    None
                };
                Some(NameDescription { name, description })
            } else {
                None
            };
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
        let peer_keys: Vec<_> = meta_automerge_doc.keys(&peer_id).collect();

        if peer_keys.iter().any(|key| key == NAME_KEY) {
            let name: String = meta_automerge_doc
                .get(&peer_id, NAME_KEY)
                .unwrap()
                .and_then(|result| result.0.to_scalar().cloned())
                .unwrap()
                .into_string()
                .unwrap();
            let description: Option<String> = if peer_keys.iter().any(|key| key == DESCRIPTION_KEY)
            {
                let description: String = meta_automerge_doc
                    .get(&peer_id, DESCRIPTION_KEY)
                    .unwrap()
                    .and_then(|result| result.0.to_scalar().cloned())
                    .unwrap()
                    .into_string()
                    .unwrap();
                Some(description)
            } else {
                None
            };
            Some(NameDescription { name, description })
        } else {
            None
        }
    } else {
        None
    }
}
