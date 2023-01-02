use automerge::{Change, ChangeHash, ObjId};
use hypercore_protocol::hypercore::compact_encoding::{CompactEncoding, State};
#[cfg(not(target_arch = "wasm32"))]
use random_access_disk::RandomAccessDisk;
use random_access_memory::RandomAccessMemory;
use random_access_storage::RandomAccess;
use std::{collections::HashMap, fmt::Debug, path::PathBuf};

use crate::{
    automerge::{AutomergeDoc, UnappliedEntries},
    common::state::{DocState, RepoState},
    hypercore::discovery_key_from_public_key,
};

use super::{
    entry::Entry,
    state::{DocContent, DocPeerState},
};
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

#[cfg(not(target_arch = "wasm32"))]
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
    unapplied_entries: UnappliedEntries,
    storage: T,
}

impl<T> DocStateWrapper<T>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send,
{
    pub async fn add_peer_public_keys_to_state(&mut self, public_keys: Vec<[u8; 32]>) -> bool {
        // Need to check if another thread has already added these keys to the state
        let need_to_add = self
            .state
            .peers
            .iter()
            .filter(|peer_state| public_keys.contains(&peer_state.public_key))
            .count()
            == 0;
        if need_to_add {
            let new_peers: Vec<DocPeerState> = public_keys
                .iter()
                .map(|public_key| DocPeerState::new(public_key.clone(), None))
                .collect();
            self.state.peers.extend(new_peers);
            write_doc_state(&self.state, &mut self.storage).await;
        }
        need_to_add
    }

    pub fn content_and_unapplied_entries_mut(
        &mut self,
    ) -> Option<(&mut DocContent, &mut UnappliedEntries)> {
        if let Some(content) = self.state.content.as_mut() {
            let unapplied_entries = &mut self.unapplied_entries;
            Some((content, unapplied_entries))
        } else {
            None
        }
    }

    pub fn unappliend_entries_mut(&mut self) -> &mut UnappliedEntries {
        &mut self.unapplied_entries
    }

    pub async fn set_content_and_new_peer_names(
        &mut self,
        content: DocContent,
        new_peer_names: Vec<([u8; 32], String)>,
    ) {
        self.state.content = Some(content);
        for (discovery_key, peer_name) in new_peer_names {
            self.set_peer_name(&discovery_key, &peer_name);
        }
        write_doc_state(&self.state, &mut self.storage).await;
    }

    pub fn peer_name(&self, discovery_key: &[u8; 32]) -> Option<String> {
        self.state
            .peers
            .iter()
            .find(|peer| &peer.discovery_key == discovery_key)
            .and_then(|peer| peer.name.clone())
    }

    pub async fn persist_content_and_new_peer_names(
        &mut self,
        new_peer_names: Vec<([u8; 32], String)>,
    ) {
        for (discovery_key, peer_name) in new_peer_names {
            self.set_peer_name(&discovery_key, &peer_name);
        }
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
        discovery_key_from_public_key(
            &self
                .state
                .write_public_key
                .expect("TODO: read-only hypercore"),
        )
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

    fn set_peer_name(&mut self, discovery_key: &[u8; 32], name: &str) -> bool {
        let changed = if let Some(mut doc_peer_state) = self
            .state
            .peers
            .iter_mut()
            .find(|peer| &peer.discovery_key == discovery_key)
        {
            let changed = doc_peer_state.name != Some(name.to_string());
            if changed {
                doc_peer_state.name = Some(name.to_string())
            }
            changed
        } else {
            panic!(
                "Could not find a pre-existing peer with discovery key {:?} to set name {}",
                discovery_key, name
            );
        };
        changed
    }
}

impl DocStateWrapper<RandomAccessMemory> {
    pub async fn new_memory(
        doc_public_key: [u8; 32],
        write_public_key: Option<[u8; 32]>,
        content: Option<DocContent>,
    ) -> Self {
        let state = DocState::new(doc_public_key, vec![], write_public_key, content);
        let mut storage = RandomAccessMemory::default();
        write_doc_state(&state, &mut storage).await;
        Self {
            state,
            storage,
            unapplied_entries: UnappliedEntries::new(),
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl DocStateWrapper<RandomAccessDisk> {
    pub async fn new_disk(
        doc_public_key: [u8; 32],
        write_public_key: Option<[u8; 32]>,
        content: Option<DocContent>,
        data_root_dir: &PathBuf,
    ) -> Self {
        let state = DocState::new(doc_public_key, vec![], write_public_key, content);
        let state_path = data_root_dir.join(PathBuf::from("hypermerge_state.bin"));
        let mut storage = RandomAccessDisk::builder(state_path).build().await.unwrap();
        write_doc_state(&state, &mut storage).await;
        Self {
            state,
            storage,
            unapplied_entries: UnappliedEntries::new(),
        }
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
