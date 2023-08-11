use automerge::{ObjId, Patch};
use compact_encoding::{CompactEncoding, State};
#[cfg(not(target_arch = "wasm32"))]
use random_access_disk::RandomAccessDisk;
use random_access_memory::RandomAccessMemory;
use random_access_storage::RandomAccess;
use std::{fmt::Debug, path::PathBuf};

use crate::{
    automerge::{AutomergeDoc, DocsChangeResult, UnappliedEntries},
    common::state::{DocumentState, PeermergeState},
    feed::FeedDiscoveryKey,
    DocumentId, NameDescription, PeermergeError,
};

use super::{
    keys::discovery_key_from_public_key,
    state::{DocumentContent, DocumentFeedInfo, DocumentFeedsState},
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
        self.state.document_ids.push(*document_id);
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
    watched_ids: Option<Vec<ObjId>>,
    storage: T,
}

impl<T> DocStateWrapper<T>
where
    T: RandomAccess + Debug + Send,
{
    pub(crate) async fn process_incoming_feeds(
        &mut self,
        incoming_feeds: &[DocumentFeedInfo],
    ) -> (bool, Vec<DocumentFeedInfo>, Vec<DocumentFeedInfo>) {
        let (changed, replaced_feeds, feeds_to_create) =
            self.state.feeds_state.merge_incoming_feeds(incoming_feeds);
        if changed {
            write_document_state(&self.state, &mut self.storage).await;
        }
        (changed, replaced_feeds, feeds_to_create)
    }

    pub(crate) fn content_feeds_state_and_unapplied_entries_mut(
        &mut self,
    ) -> Option<(
        &mut DocumentContent,
        &mut DocumentFeedsState,
        &mut UnappliedEntries,
    )> {
        if let Some(content) = self.state.content.as_mut() {
            Some((
                content,
                &mut self.state.feeds_state,
                &mut self.unapplied_entries,
            ))
        } else {
            None
        }
    }

    pub(crate) fn feeds_state_and_unappliend_entries_mut(
        &mut self,
    ) -> (&mut DocumentFeedsState, &mut UnappliedEntries) {
        (&mut self.state.feeds_state, &mut self.unapplied_entries)
    }

    pub(crate) fn filter_watched_patches(&self, patches: &mut Vec<Patch>) {
        if let Some(ids) = &self.watched_ids {
            patches.retain(|patch| ids.contains(&patch.obj));
        }
    }

    pub(crate) async fn set_content(&mut self, content: DocumentContent) {
        self.state.content = Some(content);
        write_document_state(&self.state, &mut self.storage).await;
    }

    pub(crate) async fn persist_content(&mut self) {
        write_document_state(&self.state, &mut self.storage).await;
    }

    pub(crate) async fn set_cursor_and_save_data(
        &mut self,
        discovery_key: &FeedDiscoveryKey,
        length: u64,
        change_result: DocsChangeResult,
    ) {
        if let Some(content) = self.state.content.as_mut() {
            content.set_cursor_and_save_data(*discovery_key, length, change_result);
            write_document_state(&self.state, &mut self.storage).await;
        } else {
            unimplemented!("This shouldn't happen")
        }
    }

    pub(crate) fn write_discovery_key(&self) -> FeedDiscoveryKey {
        discovery_key_from_public_key(
            &self
                .state
                .feeds_state
                .write_feed
                .clone()
                .expect("TODO: read-only hypercore")
                .public_key,
        )
    }

    pub(crate) fn state(&self) -> &DocumentState {
        &self.state
    }

    pub(crate) fn user_automerge_doc(&self) -> Option<&AutomergeDoc> {
        self.state
            .content
            .as_ref()
            .and_then(|content| content.user_automerge_doc.as_ref())
    }

    pub(crate) fn user_automerge_doc_mut(&mut self) -> Option<&mut AutomergeDoc> {
        self.state
            .content
            .as_mut()
            .and_then(|content| content.user_automerge_doc.as_mut())
    }

    pub(crate) fn watch(&mut self, ids: Option<Vec<ObjId>>) {
        self.watched_ids = ids;
    }

    pub(crate) fn reserve_object<O: AsRef<ObjId>>(&mut self, obj: O) {
        let obj = obj.as_ref();
        if !self.unapplied_entries.reserved_ids.contains(obj) {
            self.unapplied_entries.reserved_ids.insert(obj.clone());
        }
    }

    pub(crate) fn unreserve_object<O: AsRef<ObjId>>(&mut self, obj: O) {
        self.unapplied_entries.reserved_ids.remove(obj.as_ref());
    }
}

impl DocStateWrapper<RandomAccessMemory> {
    pub(crate) async fn new_memory(state: DocumentState) -> Self {
        let storage = RandomAccessMemory::default();
        Self {
            state,
            storage,
            unapplied_entries: UnappliedEntries::new(),
            watched_ids: None,
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
            watched_ids: None,
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
            watched_ids: None,
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
