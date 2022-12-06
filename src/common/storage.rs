use automerge::ObjId;
use hypercore_protocol::hypercore::compact_encoding::{CompactEncoding, State};
#[cfg(not(target_arch = "wasm32"))]
use random_access_disk::RandomAccessDisk;
use random_access_memory::RandomAccessMemory;
use random_access_storage::RandomAccess;
use std::{fmt::Debug, path::PathBuf};

use crate::{
    automerge::AutomergeDoc,
    common::state::{DocState, RepoState},
    hypercore::discovery_key_from_public_key,
};

use super::state::{DocContent, DocPeerState};

#[derive(Debug)]
pub(crate) struct RepoStateWrapper<T>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send,
{
    pub(crate) state: RepoState,
    storage: T,
}

impl<T> RepoStateWrapper<T>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send,
{
    pub async fn add_public_key_to_state(&mut self, public_key: &[u8; 32]) {
        self.state.doc_public_keys.push(public_key.clone());
        write_repo_state(&self.state, &mut self.storage).await;
    }

    pub fn state(&self) -> &RepoState {
        &self.state
    }
}

impl RepoStateWrapper<RandomAccessMemory> {
    pub async fn new_memory() -> Self {
        let state = RepoState::default();
        let mut storage = RandomAccessMemory::default();
        write_repo_state(&state, &mut storage).await;
        Self { state, storage }
    }
}

impl RepoStateWrapper<RandomAccessDisk> {
    pub async fn new_disk(data_root_dir: &PathBuf) -> Self {
        let state = RepoState::default();
        let state_path = data_root_dir.join(PathBuf::from("hypermerge_state.bin"));
        let mut storage = RandomAccessDisk::builder(state_path).build().await.unwrap();
        write_repo_state(&state, &mut storage).await;
        Self { state, storage }
    }
}

#[derive(Debug)]
pub(crate) struct DocStateWrapper<T>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send,
{
    state: DocState,
    storage: T,
}

impl<T> DocStateWrapper<T>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send,
{
    pub async fn add_peer_public_keys_to_state(&mut self, public_keys: Vec<[u8; 32]>) {
        let new_peers: Vec<DocPeerState> = public_keys
            .iter()
            .map(|public_key| DocPeerState {
                public_key: public_key.clone(),
                synced: false,
            })
            .collect();
        self.state.peers.extend(new_peers);
        write_doc_state(&self.state, &mut self.storage).await;
    }

    pub async fn set_synced_to_state(&mut self, public_key: [u8; 32], synced: bool) -> bool {
        let changed = if let Some(peer) = self
            .state
            .peers
            .iter_mut()
            .find(|peer| peer.public_key == public_key)
        {
            let changed = peer.synced != synced;
            peer.synced = synced;
            changed
        } else {
            self.state.peers.push(DocPeerState { public_key, synced });
            true
        };
        write_doc_state(&self.state, &mut self.storage).await;
        changed
    }

    pub fn peers_synced(&self) -> Option<usize> {
        if self.state.peers.iter().all(|peer| peer.synced) {
            Some(self.state.peers.len())
        } else {
            None
        }
    }

    pub fn content_mut(&mut self) -> Option<&mut DocContent> {
        self.state.content.as_mut()
    }

    pub async fn set_content(&mut self, content: DocContent) {
        self.state.content = Some(content);
        write_doc_state(&self.state, &mut self.storage).await;
    }

    pub async fn persist_content(&mut self) {
        write_doc_state(&self.state, &mut self.storage).await;
    }

    pub async fn set_cursor(&mut self, discovery_key: &[u8; 32], length: u64) {
        if let Some(content) = self.state.content.as_mut() {
            content.set_cursor(discovery_key, length);
            write_doc_state(&self.state, &mut self.storage).await;
        } else {
            unimplemented!("This shouldn't happen")
        }
    }

    pub fn write_discovery_key(&self) -> [u8; 32] {
        discovery_key_from_public_key(&self.state.public_key.expect("TODO: read-only hypercore"))
    }

    pub fn state(&self) -> &DocState {
        &self.state
    }

    pub fn doc(&self) -> Option<&AutomergeDoc> {
        self.state
            .content
            .as_ref()
            .and_then(|content| content.doc.as_ref())
    }

    pub fn doc_mut(&mut self) -> Option<&mut AutomergeDoc> {
        self.state
            .content
            .as_mut()
            .and_then(|content| content.doc.as_mut())
    }

    pub fn watch(&mut self, ids: Vec<ObjId>) {
        self.state.watched_ids = ids;
    }
}

impl DocStateWrapper<RandomAccessMemory> {
    pub async fn new_memory(
        public_key: [u8; 32],
        peer_public_keys: Vec<[u8; 32]>,
        content: Option<DocContent>,
    ) -> Self {
        let peers: Vec<DocPeerState> = peer_public_keys
            .iter()
            .map(|public_key| DocPeerState::new(public_key.clone(), false))
            .collect();
        let state = DocState::new(peers, Some(public_key), content);
        let mut storage = RandomAccessMemory::default();
        write_doc_state(&state, &mut storage).await;
        Self { state, storage }
    }
}

impl DocStateWrapper<RandomAccessDisk> {
    pub async fn new_disk(
        public_key: [u8; 32],
        peer_public_keys: Vec<[u8; 32]>,
        content: Option<DocContent>,
        data_root_dir: &PathBuf,
    ) -> Self {
        let peers: Vec<DocPeerState> = peer_public_keys
            .iter()
            .map(|public_key| DocPeerState::new(public_key.clone(), false))
            .collect();
        let state = DocState::new(peers, Some(public_key), content);
        let state_path = data_root_dir.join(PathBuf::from("hypermerge_state.bin"));
        let mut storage = RandomAccessDisk::builder(state_path).build().await.unwrap();
        write_doc_state(&state, &mut storage).await;
        Self { state, storage }
    }
}

async fn write_repo_state<T>(repo_state: &RepoState, storage: &mut T)
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send,
{
    let mut enc_state = State::new();
    enc_state.preencode(repo_state);
    let mut buffer = enc_state.create_buffer();
    enc_state.encode(repo_state, &mut buffer);
    storage.write(0, &buffer).await.unwrap();
}

async fn write_doc_state<T>(doc_state: &DocState, storage: &mut T)
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send,
{
    let mut enc_state = State::new();
    enc_state.preencode(doc_state);
    let mut buffer = enc_state.create_buffer();
    enc_state.encode(doc_state, &mut buffer);
    storage.write(0, &buffer).await.unwrap();
}
