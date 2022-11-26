use hypercore_protocol::hypercore::compact_encoding::{CompactEncoding, State};
#[cfg(not(target_arch = "wasm32"))]
use random_access_disk::RandomAccessDisk;
use random_access_memory::RandomAccessMemory;
use random_access_storage::RandomAccess;
use std::{fmt::Debug, path::PathBuf};

use crate::common::state::{DocState, RepoState};

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
    pub async fn add_public_keys_to_state(&mut self, public_keys: Vec<[u8; 32]>) {
        self.state.peer_public_keys.extend(public_keys);
        write_doc_state(&self.state, &mut self.storage).await;
    }

    pub fn state(&self) -> &DocState {
        &self.state
    }
}

impl DocStateWrapper<RandomAccessMemory> {
    pub async fn new_memory(public_key: [u8; 32], peer_public_keys: Vec<[u8; 32]>) -> Self {
        let state = DocState::new(public_key, peer_public_keys);
        let mut storage = RandomAccessMemory::default();
        write_doc_state(&state, &mut storage).await;
        Self { state, storage }
    }
}

impl DocStateWrapper<RandomAccessDisk> {
    pub async fn new_disk(
        public_key: [u8; 32],
        peer_public_keys: Vec<[u8; 32]>,
        data_root_dir: &PathBuf,
    ) -> Self {
        let state = DocState::new(public_key, peer_public_keys);
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
