use automerge::ObjId;
use compact_encoding::{CompactEncoding, State};
#[cfg(not(target_arch = "wasm32"))]
use random_access_disk::RandomAccessDisk;
use random_access_memory::RandomAccessMemory;
use random_access_storage::RandomAccess;
use std::{fmt::Debug, path::PathBuf};

use crate::{
    automerge::{AutomergeDoc, UnappliedEntries},
    common::state::{DocumentState, PeermergeState},
    DocumentId, NameDescription, PeermergeError,
};

use super::{
    keys::discovery_key_from_public_key,
    state::{DocumentContent, DocumentPeerState},
};
#[derive(Debug)]
pub(crate) struct PeermergeStateWrapper<T>
where
    T: RandomAccess + Debug + Send,
{
    pub(crate) state: PeermergeState,
    storage: T,
}

impl<T> PeermergeStateWrapper<T>
where
    T: RandomAccess + Debug + Send,
{
    pub(crate) async fn add_document_id_to_state(&mut self, document_id: &DocumentId) {
        self.state.document_ids.push(document_id.clone());
        write_repo_state(&self.state, &mut self.storage).await;
    }

    pub(crate) fn state(&self) -> &PeermergeState {
        &self.state
    }
}

impl PeermergeStateWrapper<RandomAccessMemory> {
    pub(crate) async fn new_memory(peer_header: &NameDescription) -> Self {
        let state = PeermergeState::new(peer_header, vec![]);
        let mut storage = RandomAccessMemory::default();
        write_repo_state(&state, &mut storage).await;
        Self { state, storage }
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl PeermergeStateWrapper<RandomAccessDisk> {
    pub(crate) async fn new_disk(peer_header: &NameDescription, data_root_dir: &PathBuf) -> Self {
        let state = PeermergeState::new(peer_header, vec![]);
        let state_path = get_peermerge_state_path(data_root_dir);
        let mut storage = RandomAccessDisk::builder(state_path).build().await.unwrap();
        write_repo_state(&state, &mut storage).await;
        Self { state, storage }
    }

    pub(crate) async fn open_disk(data_root_dir: &PathBuf) -> Result<Option<Self>, PeermergeError> {
        let state_path = get_peermerge_state_path(data_root_dir);
        if state_path.exists() {
            let mut storage = RandomAccessDisk::builder(state_path).build().await.unwrap();
            let len = storage.len().await.expect("Could not get file length");
            let buffer = storage
                .read(0, len)
                .await
                .expect("Could not read file content");
            let mut dec_state = State::from_buffer(&buffer);
            let state: PeermergeState = dec_state.decode(&buffer)?;
            Ok(Some(Self { state, storage }))
        } else {
            Ok(None)
        }
    }
}

fn get_peermerge_state_path(data_root_dir: &PathBuf) -> PathBuf {
    data_root_dir.join(PathBuf::from("peermerge_state.bin"))
}

#[derive(Debug)]
pub(crate) struct DocStateWrapper<T>
where
    T: RandomAccess + Debug + Send,
{
    state: DocumentState,
    unapplied_entries: UnappliedEntries,
    storage: T,
}

impl<T> DocStateWrapper<T>
where
    T: RandomAccess + Debug + Send,
{
    pub(crate) async fn add_peer_public_keys_to_state(
        &mut self,
        public_keys: Vec<[u8; 32]>,
    ) -> bool {
        let added = add_peer_public_keys_to_document_state(&mut self.state, public_keys);
        if added {
            write_document_state(&self.state, &mut self.storage).await;
        }
        added
    }

    pub(crate) fn content_and_unapplied_entries_mut(
        &mut self,
    ) -> Option<(&mut DocumentContent, &mut UnappliedEntries)> {
        if let Some(content) = self.state.content.as_mut() {
            let unapplied_entries = &mut self.unapplied_entries;
            Some((content, unapplied_entries))
        } else {
            None
        }
    }

    pub(crate) fn unappliend_entries_mut(&mut self) -> &mut UnappliedEntries {
        &mut self.unapplied_entries
    }

    pub(crate) async fn set_content_and_new_peer_headers(
        &mut self,
        content: DocumentContent,
        new_peer_headers: Vec<([u8; 32], NameDescription)>,
    ) {
        self.state.content = Some(content);
        for (discovery_key, peer_header) in new_peer_headers {
            self.set_peer_header(&discovery_key, peer_header);
        }
        write_document_state(&self.state, &mut self.storage).await;
    }

    pub(crate) fn peer_name(&self, discovery_key: &[u8; 32]) -> Option<String> {
        self.state
            .peers
            .iter()
            .find(|peer| &peer.discovery_key == discovery_key)
            .and_then(|peer| {
                peer.peer_header
                    .as_ref()
                    .map(|header| header.name.clone())
                    .clone()
            })
    }

    pub(crate) async fn persist_content_and_new_peer_headers(
        &mut self,
        new_peer_names: Vec<([u8; 32], NameDescription)>,
    ) {
        for (discovery_key, peer_header) in new_peer_names {
            self.set_peer_header(&discovery_key, peer_header);
        }
        write_document_state(&self.state, &mut self.storage).await;
    }

    pub(crate) async fn set_cursor(&mut self, discovery_key: &[u8; 32], length: u64) {
        if let Some(content) = self.state.content.as_mut() {
            content.set_cursor(discovery_key, length);
            write_document_state(&self.state, &mut self.storage).await;
        } else {
            unimplemented!("This shouldn't happen")
        }
    }

    pub(crate) fn write_discovery_key(&self) -> [u8; 32] {
        discovery_key_from_public_key(
            &self
                .state
                .write_public_key
                .expect("TODO: read-only hypercore"),
        )
    }

    pub(crate) fn state(&self) -> &DocumentState {
        &self.state
    }

    pub(crate) fn automerge_doc(&self) -> Option<&AutomergeDoc> {
        self.state
            .content
            .as_ref()
            .and_then(|content| content.automerge_doc.as_ref())
    }

    pub(crate) fn automerge_doc_mut(&mut self) -> Option<&mut AutomergeDoc> {
        self.state
            .content
            .as_mut()
            .and_then(|content| content.automerge_doc.as_mut())
    }

    pub(crate) fn watch(&mut self, ids: Vec<ObjId>) {
        self.state.watched_ids = ids;
    }

    fn set_peer_header(&mut self, discovery_key: &[u8; 32], peer_header: NameDescription) -> bool {
        let changed = if let Some(mut doc_peer_state) = self
            .state
            .peers
            .iter_mut()
            .find(|peer| &peer.discovery_key == discovery_key)
        {
            let changed = doc_peer_state.peer_header.as_ref() != Some(&peer_header);
            if changed {
                doc_peer_state.peer_header = Some(peer_header)
            }
            changed
        } else {
            panic!(
                "Could not find a pre-existing peer with discovery key {:?} to set header {:?}",
                discovery_key, peer_header
            );
        };
        changed
    }
}

impl DocStateWrapper<RandomAccessMemory> {
    pub(crate) async fn new_memory(state: DocumentState) -> Self {
        let storage = RandomAccessMemory::default();
        Self {
            state,
            storage,
            unapplied_entries: UnappliedEntries::new(),
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl DocStateWrapper<RandomAccessDisk> {
    pub(crate) async fn new_disk(state: DocumentState, data_root_dir: &PathBuf) -> Self {
        let state_path = get_document_state_path(data_root_dir);
        let mut storage = RandomAccessDisk::builder(state_path).build().await.unwrap();
        write_document_state(&state, &mut storage).await;
        Self {
            state,
            storage,
            unapplied_entries: UnappliedEntries::new(),
        }
    }

    pub(crate) async fn open_disk(data_root_dir: &PathBuf) -> Result<Self, PeermergeError> {
        let state_path = get_document_state_path(data_root_dir);
        let mut storage = RandomAccessDisk::builder(state_path).build().await.unwrap();
        let len = storage.len().await.expect("Could not get file length");
        let buffer = storage
            .read(0, len)
            .await
            .expect("Could not read file content");
        let mut dec_state = State::from_buffer(&buffer);
        let state: DocumentState = dec_state.decode(&buffer)?;
        Ok(Self {
            state,
            storage,
            unapplied_entries: UnappliedEntries::new(),
        })
    }
}

fn get_document_state_path(data_root_dir: &PathBuf) -> PathBuf {
    data_root_dir.join(PathBuf::from("document_state.bin"))
}

async fn write_repo_state<T>(repo_state: &PeermergeState, storage: &mut T)
where
    T: RandomAccess + Debug + Send,
{
    let mut enc_state = State::new();
    enc_state
        .preencode(repo_state)
        .expect("Pre-encoding repo state should not fail");
    let mut buffer = enc_state.create_buffer();
    enc_state
        .encode(repo_state, &mut buffer)
        .expect("Encoding repo state should not fail");
    storage.write(0, &buffer).await.unwrap();
}

fn add_peer_public_keys_to_document_state(
    document_state: &mut DocumentState,
    public_keys: Vec<[u8; 32]>,
) -> bool {
    // Need to check if another thread has already added these keys to the state
    let need_to_add = document_state
        .peers
        .iter()
        .filter(|peer_state| public_keys.contains(&peer_state.public_key))
        .count()
        == 0;
    if need_to_add {
        let new_peers: Vec<DocumentPeerState> = public_keys
            .iter()
            .map(|public_key| DocumentPeerState::new(public_key.clone(), None))
            .collect();
        document_state.peers.extend(new_peers);
    }
    need_to_add
}

async fn write_document_state<T>(document_state: &DocumentState, storage: &mut T)
where
    T: RandomAccess + Debug + Send,
{
    let mut enc_state = State::new();
    enc_state
        .preencode(document_state)
        .expect("Pre-encoding document state should not fail");
    let mut buffer = enc_state.create_buffer();
    enc_state
        .encode(document_state, &mut buffer)
        .expect("Encoding document state should not fail");
    storage.write(0, &buffer).await.unwrap();
}
