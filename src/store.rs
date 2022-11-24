use async_std::sync::{Arc, Mutex};
use automerge::Automerge;
use hypercore_protocol::hypercore::compact_encoding::{CompactEncoding, State};
#[cfg(not(target_arch = "wasm32"))]
use random_access_disk::RandomAccessDisk;
use random_access_memory::RandomAccessMemory;
use random_access_storage::RandomAccess;
use std::{collections::HashMap, fmt::Debug, path::PathBuf};

#[cfg(not(target_arch = "wasm32"))]
use crate::hypercore::{
    create_new_read_disk_hypercore, create_new_write_disk_hypercore, get_path_from_discovery_key,
};
use crate::{
    encoding::{DocState, RepoState},
    hypercore::{
        create_new_read_memory_hypercore, create_new_write_memory_hypercore, generate_keys,
        keys_from_public_key, HypercoreWrapper,
    },
};

#[derive(Debug)]
pub(crate) struct HypercoreStore<T>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send,
{
    pub(crate) doc: Option<Automerge>,
    pub(crate) hypercores: HashMap<[u8; 32], Arc<Mutex<HypercoreWrapper<T>>>>,
    doc_state: DocState,
    doc_state_storage: T,
}

impl<T> HypercoreStore<T>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send,
{
    pub fn peer_public_keys(&self) -> Vec<[u8; 32]> {
        self.doc_state.peer_public_keys.clone()
    }

    async fn add_public_key_to_state(&mut self, public_key: &[u8; 32]) {
        self.doc_state.peer_public_keys.push(public_key.clone());
        write_doc_state(&self.doc_state, &mut self.doc_state_storage).await;
    }
}

impl HypercoreStore<RandomAccessMemory> {
    pub async fn new_memory(
        public_key: [u8; 32],
        discovery_key: [u8; 32],
        hypercore: HypercoreWrapper<RandomAccessMemory>,
        doc: Option<Automerge>,
    ) -> Self {
        let mut hypercores: HashMap<[u8; 32], Arc<Mutex<HypercoreWrapper<RandomAccessMemory>>>> =
            HashMap::new();
        hypercores.insert(discovery_key, Arc::new(Mutex::new(hypercore)));

        let mut doc_state = DocState::default();
        doc_state.peer_public_keys.push(public_key);
        let mut doc_state_storage = RandomAccessMemory::default();
        write_doc_state(&doc_state, &mut doc_state_storage).await;

        Self {
            doc,
            hypercores,
            doc_state,
            doc_state_storage,
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl HypercoreStore<RandomAccessDisk> {
    pub async fn new_disk(
        public_key: [u8; 32],
        discovery_key: [u8; 32],
        hypercore: HypercoreWrapper<RandomAccessDisk>,
        doc: Option<Automerge>,
        data_root_dir: &PathBuf,
    ) -> Self {
        let mut hypercores: HashMap<[u8; 32], Arc<Mutex<HypercoreWrapper<RandomAccessDisk>>>> =
            HashMap::new();
        hypercores.insert(discovery_key, Arc::new(Mutex::new(hypercore)));

        let mut doc_state = DocState::default();
        doc_state.peer_public_keys.push(public_key);
        let state_path = data_root_dir.join(PathBuf::from("hypercore_state.bin"));
        let mut doc_state_storage = RandomAccessDisk::builder(state_path).build().await.unwrap();
        write_doc_state(&doc_state, &mut doc_state_storage).await;

        Self {
            doc,
            hypercores,
            doc_state,
            doc_state_storage,
        }
    }
}

/// A container for hypermerge documents.
#[derive(Debug)]
pub(crate) struct DocStore<T>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send,
{
    docs: HashMap<[u8; 32], HypercoreStore<T>>,
    repo_state: RepoState,
    repo_state_storage: T,
    prefix: PathBuf,
}

impl<T> DocStore<T>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send,
{
    pub fn get_mut(&mut self, doc_url: &str) -> Option<&mut HypercoreStore<T>> {
        let public_key = to_public_key(doc_url);
        let (_, discovery_key) = keys_from_public_key(&public_key);
        self.docs.get_mut(&discovery_key)
    }

    async fn add_public_key_to_state(&mut self, public_key: &[u8; 32]) {
        self.repo_state.doc_public_keys.push(public_key.clone());
        write_repo_state(&self.repo_state, &mut self.repo_state_storage).await;
    }
}

impl DocStore<RandomAccessMemory> {
    pub async fn new_memory() -> Self {
        let docs = HashMap::new();
        let repo_state = RepoState::default();
        let mut repo_state_storage = RandomAccessMemory::default();
        write_repo_state(&repo_state, &mut repo_state_storage).await;
        Self {
            docs,
            repo_state,
            repo_state_storage,
            prefix: PathBuf::new(),
        }
    }

    pub async fn add_doc_memory(&mut self, mut doc: Automerge) -> String {
        // Generate a key pair, its discovery key and the public key string
        let (key_pair, encoded_public_key, discovery_key) = generate_keys();
        let public_key = *key_pair.public.as_bytes();

        // Create the memory hypercore
        let hypercore = create_new_write_memory_hypercore(key_pair, doc.save()).await;

        // Write the state
        self.add_public_key_to_state(&public_key).await;

        // Insert a hypercore store to the doc store
        let hypercore_store =
            HypercoreStore::new_memory(public_key, discovery_key.clone(), hypercore, Some(doc))
                .await;
        self.docs.insert(discovery_key, hypercore_store);

        // Return the doc url
        to_doc_url(encoded_public_key)
    }

    pub async fn register_doc_memory(&mut self, doc_url: &str) {
        // Process keys from doc URL
        let public_key = to_public_key(doc_url);
        let (public_key, discovery_key) = keys_from_public_key(&public_key);

        // Create the memory hypercore
        let hypercore = create_new_read_memory_hypercore(&public_key).await;

        // Write the state
        self.add_public_key_to_state(&public_key).await;

        // Insert a hypercore store to the doc store
        let hypercore_store =
            HypercoreStore::new_memory(public_key, discovery_key.clone(), hypercore, None).await;
        self.docs.insert(discovery_key, hypercore_store);
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl DocStore<RandomAccessDisk> {
    pub async fn new_disk(data_root_dir: &PathBuf) -> Self {
        let docs = HashMap::new();
        let repo_state = RepoState::default();
        let state_path = data_root_dir.join(PathBuf::from("hypermerge_state.bin"));
        let mut repo_state_storage = RandomAccessDisk::builder(state_path).build().await.unwrap();
        write_repo_state(&repo_state, &mut repo_state_storage).await;
        Self {
            docs,
            repo_state,
            repo_state_storage,
            prefix: data_root_dir.to_path_buf(),
        }
    }

    pub async fn add_doc_disk(&mut self, mut doc: Automerge) -> String {
        // Generate a key pair, its discovery key and the public key string
        let (key_pair, encoded_public_key, discovery_key) = generate_keys();
        let public_key = *key_pair.public.as_bytes();

        // Create the disk hypercore with init data
        let hypercore =
            create_new_write_disk_hypercore(&self.prefix, key_pair, &discovery_key, doc.save())
                .await;

        // Write the state
        self.add_public_key_to_state(&public_key).await;

        // Insert a hypercore store to the doc store
        let hypercore_store = HypercoreStore::new_disk(
            public_key,
            discovery_key.clone(),
            hypercore,
            Some(doc),
            &get_path_from_discovery_key(&self.prefix, &discovery_key),
        )
        .await;
        self.docs.insert(discovery_key, hypercore_store);

        // Return the doc url
        to_doc_url(encoded_public_key)
    }

    pub async fn register_doc_disk(&mut self, doc_url: &str) {
        // Process keys from doc URL
        let public_key = to_public_key(doc_url);
        let (public_key, discovery_key) = keys_from_public_key(&public_key);

        // Create the disk hypercore
        let hypercore =
            create_new_read_disk_hypercore(&self.prefix, &public_key, &discovery_key).await;

        // Write the state
        self.add_public_key_to_state(&public_key).await;

        // Insert a hypercore store to the doc store
        let hypercore_store = HypercoreStore::new_disk(
            public_key,
            discovery_key.clone(),
            hypercore,
            None,
            &get_path_from_discovery_key(&self.prefix, &discovery_key),
        )
        .await;
        self.docs.insert(discovery_key, hypercore_store);
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

fn to_doc_url(public_key: String) -> String {
    format!("hypermerge:/{}", public_key)
}

fn to_public_key(doc_url: &str) -> String {
    doc_url[12..].to_string()
}
