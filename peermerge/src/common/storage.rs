use automerge::{ObjId, Patch};
use compact_encoding::{CompactEncoding, State};
#[cfg(not(target_arch = "wasm32"))]
use random_access_disk::RandomAccessDisk;
use random_access_memory::RandomAccessMemory;
use random_access_storage::RandomAccess;
use std::{fmt::Debug, path::PathBuf};

use crate::{
    common::state::{DocumentState, PeermergeState},
    crdt::{
        add_child_document, get_child_document_decoded_url, AutomergeDoc, DocsChangeResult,
        UnappliedEntries,
    },
    document::DocumentSettings,
    feeds::FeedDiscoveryKey,
    DocumentId, NameDescription, PeerId, PeermergeError,
};

use super::{
    cipher::{encode_document_secret_to_bytes, DecodedDocUrl, DocumentSecret},
    entry::Entry,
    keys::{discovery_key_from_public_key, document_id_from_discovery_key},
    state::{
        ChangeDocumentFeedsStateResult, ChildDocumentInfo, ChildDocumentStatus, DocumentContent,
        DocumentFeedInfo, DocumentFeedsState,
    },
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
    pub(crate) async fn new_memory(
        peer_header: &NameDescription,
        document_write_settings: DocumentSettings,
    ) -> Self {
        let state = PeermergeState::new(peer_header, vec![], document_write_settings);
        let mut storage = RandomAccessMemory::default();
        write_repo_state(&state, &mut storage).await;
        Self { state, storage }
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl PeermergeStateWrapper<RandomAccessDisk> {
    pub(crate) async fn new_disk(
        peer_header: &NameDescription,
        data_root_dir: &PathBuf,
        document_write_settings: DocumentSettings,
    ) -> Result<Self, PeermergeError> {
        let state = PeermergeState::new(peer_header, vec![], document_write_settings);
        let state_path = get_peermerge_state_path(data_root_dir);
        let mut storage = RandomAccessDisk::builder(state_path.clone())
            .build()
            .await
            .map_err(|err| PeermergeError::InvalidOperation {
                context: format!("Could not create file to path {state_path:?}, {err}"),
            })?;
        write_repo_state(&state, &mut storage).await;
        Ok(Self { state, storage })
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
    pub(crate) async fn merge_new_remote_feeds(
        &mut self,
        new_remote_feeds: &[DocumentFeedInfo],
    ) -> Result<ChangeDocumentFeedsStateResult, PeermergeError> {
        let result = self.state.feeds_state.merge_new_feeds(new_remote_feeds)?;
        if result.changed {
            write_document_state(&self.state, &mut self.storage).await;
        }
        Ok(result)
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

    pub(crate) async fn set_verified(
        &mut self,
        discovery_key: &FeedDiscoveryKey,
        peer_id: &Option<PeerId>,
    ) -> bool {
        let changed = self.state.feeds_state.verify_feed(discovery_key, peer_id);
        if changed {
            write_document_state(&self.state, &mut self.storage).await;
        }
        changed
    }

    pub(crate) fn filter_watched_patches(&self, patches: &mut Vec<Patch>) {
        if let Some(ids) = &self.watched_ids {
            patches.retain(|patch| ids.contains(&patch.obj));
        }
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

    pub(crate) fn state_mut(&mut self) -> &mut DocumentState {
        &mut self.state
    }

    pub(crate) async fn add_created_child_document(
        &mut self,
        child_document_info: ChildDocumentInfo,
        child_document_url: &str,
        child_document_secret: DocumentSecret,
        max_entry_data_size_bytes: usize,
    ) -> Result<Vec<Entry>, PeermergeError> {
        let entries = if let Some(content) = self.state.content.as_mut() {
            if let Some(meta_automerge_doc) = content.meta_automerge_doc.as_mut() {
                let child_document_secret: Vec<u8> =
                    encode_document_secret_to_bytes(&child_document_secret).to_vec();
                let child_document_discovery_key =
                    discovery_key_from_public_key(&child_document_info.doc_public_key);
                let child_document_id =
                    document_id_from_discovery_key(&child_document_discovery_key);
                add_child_document(
                    meta_automerge_doc,
                    child_document_id,
                    child_document_url,
                    child_document_secret,
                    max_entry_data_size_bytes,
                )?
            } else {
                panic!("Can not add a child to a document without an initialized meta doc");
            }
        } else {
            panic!("Can not add a child to a document without content");
        };

        self.state.child_documents.push(child_document_info);
        Ok(entries)
    }

    /// Merge incoming child document info with that of the stored child
    /// document infos. If found returns document secret for the child document and marks
    /// the child document into ChildDocumentStatus::Creating. If child
    /// document not found, or given info already Created/Creating, returns
    /// None.
    pub(crate) async fn merge_child_document_and_persist(
        &mut self,
        child_document_info: &mut ChildDocumentInfo,
    ) -> Option<DecodedDocUrl> {
        // First check that creation isn't ongoing already from some other protocol
        let already_stored = if let Some(info) = self
            .state
            .child_documents
            .iter()
            .find(|info| *info == child_document_info)
        {
            if info.status == ChildDocumentStatus::Created
                || info.status == ChildDocumentStatus::Creating
            {
                // NB: if the status is NotCreated, we want to recheck if secret can be
                // found now, and not return.
                return None;
            }
            true
        } else {
            false
        };

        // Get a mutable reference
        let stored_child_document_info = if already_stored {
            self.state
                .child_documents
                .iter_mut()
                .find(|info| *info == child_document_info)
        } else {
            None
        };

        // Reset status so that value from remote isn't carried over here
        child_document_info.status = ChildDocumentStatus::NotCreated;

        // Search for the URL and secret from the meta doc
        let stored_decoded_url = if let Some(content) = self.state.content.as_ref() {
            if let Some(meta_automerge_doc) = content.meta_automerge_doc.as_ref() {
                let child_document_discovery_key =
                    discovery_key_from_public_key(&child_document_info.doc_public_key);
                let child_document_id =
                    document_id_from_discovery_key(&child_document_discovery_key);
                let stored_decoded_url =
                    get_child_document_decoded_url(meta_automerge_doc, child_document_id);
                if stored_decoded_url.is_some() {
                    // Found the secret immediately, mark this as creating
                    if let Some(info) = stored_child_document_info {
                        info.status = ChildDocumentStatus::Creating;
                    } else {
                        child_document_info.status = ChildDocumentStatus::Creating;
                    }
                }
                stored_decoded_url
            } else {
                None
            }
        } else {
            None
        };
        if !already_stored {
            self.state.child_documents.push(child_document_info.clone());
        }
        if !already_stored || stored_decoded_url.is_some() {
            write_document_state(&self.state, &mut self.storage).await;
        }
        stored_decoded_url
    }

    pub(crate) async fn set_child_document_created_and_persist(
        &mut self,
        child_document_info: &ChildDocumentInfo,
    ) -> ChildDocumentInfo {
        // Get a mutable reference
        let stored_child_document_info = self
            .state
            .child_documents
            .iter_mut()
            .find(|info| *info == child_document_info)
            .expect("Child document must be set to Creating before persisting");
        stored_child_document_info.status = ChildDocumentStatus::Created;
        let return_info = stored_child_document_info.clone();
        write_document_state(&self.state, &mut self.storage).await;
        return_info
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

    // pub(crate) fn meta_automerge_doc(&self) -> Option<&AutomergeDoc> {
    //     self.state
    //         .content
    //         .as_ref()
    //         .and_then(|content| content.meta_automerge_doc.as_ref())
    // }

    // pub(crate) fn meta_automerge_doc_mut(&mut self) -> Option<&mut AutomergeDoc> {
    //     self.state
    //         .content
    //         .as_mut()
    //         .and_then(|content| content.meta_automerge_doc.as_mut())
    // }

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
