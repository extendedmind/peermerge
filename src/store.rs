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
    common::{
        entry::Entry,
        state::{DocContent, DocCursor},
        storage::{DocStateWrapper, RepoStateWrapper},
    },
    hypercore::{
        create_new_read_memory_hypercore, create_new_write_memory_hypercore,
        discovery_key_from_public_key, generate_keys, keys_from_public_key, HypercoreWrapper,
    },
};

/// A container for hypermerge documents.
#[derive(Debug)]
pub(crate) struct DocStore<T>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send,
{
    docs: HashMap<[u8; 32], HypercoreStore<T>>,
    repo_state: RepoStateWrapper<T>,
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
}

impl DocStore<RandomAccessMemory> {
    pub async fn new_memory() -> Self {
        let docs = HashMap::new();
        let repo_state = RepoStateWrapper::new_memory().await;
        Self {
            docs,
            repo_state,
            prefix: PathBuf::new(),
        }
    }

    pub async fn add_doc_memory(&mut self, mut doc: Automerge) -> String {
        // Generate a key pair, its discovery key and the public key string
        let (key_pair, encoded_public_key, discovery_key) = generate_keys();
        let public_key = *key_pair.public.as_bytes();

        // Create the memory hypercore
        let doc = doc.save();
        let (length, hypercore) = create_new_write_memory_hypercore(
            key_pair,
            serialize_entry(&Entry::new_init_doc(doc.clone())),
        )
        .await;
        let content = DocContent::new(doc, vec![DocCursor::new(public_key.clone(), length)]);

        // Write the state
        self.repo_state.add_public_key_to_state(&public_key).await;

        // Insert a hypercore store to the doc store
        let hypercore_store = HypercoreStore::new_memory(
            (public_key, discovery_key.clone(), hypercore),
            vec![],
            Some(content),
        )
        .await;
        self.docs.insert(discovery_key, hypercore_store);

        // Return the doc url
        to_doc_url(encoded_public_key)
    }

    pub async fn register_doc_memory(&mut self, doc_url: &str) {
        // Process keys from doc URL
        let doc_public_key = to_public_key(doc_url);
        let (doc_public_key, doc_discovery_key) = keys_from_public_key(&doc_public_key);

        // Create the doc hypercore
        let (_, doc_hypercore) = create_new_read_memory_hypercore(&doc_public_key).await;

        // Create the write hypercore
        let (write_key_pair, _, write_discovery_key) = generate_keys();
        let write_public_key = *write_key_pair.public.as_bytes();
        let (_, write_hypercore) = create_new_write_memory_hypercore(
            write_key_pair,
            serialize_entry(&Entry::new_init_peer(doc_discovery_key)),
        )
        .await;

        // Write the state
        self.repo_state
            .add_public_key_to_state(&doc_public_key)
            .await;

        // Insert a hypercore store to the doc store
        let hypercore_store = HypercoreStore::new_memory(
            (write_public_key, write_discovery_key, write_hypercore),
            vec![(doc_public_key, doc_discovery_key.clone(), doc_hypercore)],
            None,
        )
        .await;
        self.docs.insert(doc_discovery_key, hypercore_store);
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl DocStore<RandomAccessDisk> {
    pub async fn new_disk(data_root_dir: &PathBuf) -> Self {
        let docs = HashMap::new();
        let repo_state = RepoStateWrapper::new_disk(&data_root_dir).await;
        Self {
            docs,
            repo_state,
            prefix: data_root_dir.to_path_buf(),
        }
    }

    pub async fn add_doc_disk(&mut self, mut doc: Automerge) -> String {
        // Generate a key pair, its discovery key and the public key string
        let (key_pair, encoded_public_key, discovery_key) = generate_keys();
        let public_key = *key_pair.public.as_bytes();

        // Create the disk hypercore with init data
        let doc = doc.save();
        let (length, hypercore) = create_new_write_disk_hypercore(
            &self.prefix,
            key_pair,
            &discovery_key,
            serialize_entry(&Entry::new_init_doc(doc.clone())),
        )
        .await;
        let content = DocContent::new(doc, vec![DocCursor::new(public_key.clone(), length)]);

        // Write the state
        self.repo_state.add_public_key_to_state(&public_key).await;

        // Insert a hypercore store to the doc store
        let hypercore_store = HypercoreStore::new_disk(
            (public_key, discovery_key.clone(), hypercore),
            vec![],
            Some(content),
            &get_path_from_discovery_key(&self.prefix, &discovery_key),
        )
        .await;
        self.docs.insert(discovery_key, hypercore_store);

        // Return the doc url
        to_doc_url(encoded_public_key)
    }

    pub async fn register_doc_disk(&mut self, doc_url: &str) {
        // Process keys from doc URL
        let doc_public_key = to_public_key(doc_url);
        let (doc_public_key, doc_discovery_key) = keys_from_public_key(&doc_public_key);

        // Create the doc hypercore
        let (_, doc_hypercore) =
            create_new_read_disk_hypercore(&self.prefix, &doc_public_key, &doc_discovery_key).await;

        // Create the write hypercore
        let (write_key_pair, write_encoded_public_key, write_discovery_key) = generate_keys();
        let write_public_key = *write_key_pair.public.as_bytes();
        let (_, write_hypercore) = create_new_write_disk_hypercore(
            &self.prefix,
            write_key_pair,
            &write_discovery_key,
            serialize_entry(&Entry::new_init_peer(doc_discovery_key)),
        )
        .await;

        // Write the state
        self.repo_state
            .add_public_key_to_state(&doc_public_key)
            .await;

        // Insert a hypercore store to the doc store
        let hypercore_store = HypercoreStore::new_disk(
            (write_public_key, write_discovery_key, write_hypercore),
            vec![(doc_public_key, doc_discovery_key.clone(), doc_hypercore)],
            None,
            &get_path_from_discovery_key(&self.prefix, &doc_discovery_key),
        )
        .await;
        self.docs.insert(doc_discovery_key, hypercore_store);
    }
}

#[derive(Debug)]
pub(crate) struct HypercoreStore<T>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send,
{
    hypercores: Arc<Mutex<HashMap<[u8; 32], Arc<Mutex<HypercoreWrapper<T>>>>>>,
    doc_state: Arc<Mutex<DocStateWrapper<T>>>,
}

impl<T> HypercoreStore<T>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send,
{
    pub async fn public_keys(&self) -> Vec<[u8; 32]> {
        let state = self.doc_state.lock().await;
        let state = state.state();
        let mut public_keys: Vec<[u8; 32]> = state
            .peers
            .iter()
            .map(|peer| peer.public_key.clone())
            .collect();
        if let Some(public_key) = state.public_key {
            public_keys.push(public_key);
        }
        public_keys
    }

    pub fn doc_state(&self) -> Arc<Mutex<DocStateWrapper<T>>> {
        self.doc_state.clone()
    }

    pub fn hypercores(&self) -> Arc<Mutex<HashMap<[u8; 32], Arc<Mutex<HypercoreWrapper<T>>>>>> {
        self.hypercores.clone()
    }
}

impl HypercoreStore<RandomAccessMemory> {
    pub async fn new_memory(
        write_hypercore: ([u8; 32], [u8; 32], HypercoreWrapper<RandomAccessMemory>),
        peer_hypercores: Vec<([u8; 32], [u8; 32], HypercoreWrapper<RandomAccessMemory>)>,
        content: Option<DocContent>,
    ) -> Self {
        let mut hypercore_map: HashMap<[u8; 32], Arc<Mutex<HypercoreWrapper<RandomAccessMemory>>>> =
            HashMap::new();
        let (write_public_key, write_discovery_key, write_hypercore) = write_hypercore;
        hypercore_map.insert(write_discovery_key, Arc::new(Mutex::new(write_hypercore)));
        let mut peer_public_keys = vec![];
        for (peer_public_key, peer_discovery_key, peer_hypercore) in peer_hypercores {
            peer_public_keys.push(peer_public_key);
            hypercore_map.insert(peer_discovery_key, Arc::new(Mutex::new(peer_hypercore)));
        }
        let doc_state =
            DocStateWrapper::new_memory(write_public_key, peer_public_keys, content).await;
        Self {
            hypercores: Arc::new(Mutex::new(hypercore_map)),
            doc_state: Arc::new(Mutex::new(doc_state)),
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl HypercoreStore<RandomAccessDisk> {
    pub async fn new_disk(
        write_hypercore: ([u8; 32], [u8; 32], HypercoreWrapper<RandomAccessDisk>),
        peer_hypercores: Vec<([u8; 32], [u8; 32], HypercoreWrapper<RandomAccessDisk>)>,
        content: Option<DocContent>,
        data_root_dir: &PathBuf,
    ) -> Self {
        let mut hypercore_map: HashMap<[u8; 32], Arc<Mutex<HypercoreWrapper<RandomAccessDisk>>>> =
            HashMap::new();
        let (write_public_key, write_discovery_key, write_hypercore) = write_hypercore;
        hypercore_map.insert(write_discovery_key, Arc::new(Mutex::new(write_hypercore)));
        let mut peer_public_keys = vec![];
        for (peer_public_key, peer_discovery_key, peer_hypercore) in peer_hypercores {
            peer_public_keys.push(peer_public_key);
            hypercore_map.insert(peer_discovery_key, Arc::new(Mutex::new(peer_hypercore)));
        }
        let doc_state =
            DocStateWrapper::new_disk(write_public_key, peer_public_keys, content, &data_root_dir)
                .await;

        Self {
            hypercores: Arc::new(Mutex::new(hypercore_map)),
            doc_state: Arc::new(Mutex::new(doc_state)),
        }
    }
}

pub async fn create_and_insert_read_memory_hypercores(
    public_keys: Vec<[u8; 32]>,
    hypercores: Arc<Mutex<HashMap<[u8; 32], Arc<Mutex<HypercoreWrapper<RandomAccessMemory>>>>>>,
) {
    let mut hypercores = hypercores.lock().await;
    for public_key in public_keys {
        let discovery_key = discovery_key_from_public_key(&public_key);
        let (_, hypercore) = create_new_read_memory_hypercore(&public_key).await;
        hypercores.insert(discovery_key, Arc::new(Mutex::new(hypercore)));
    }
}

fn serialize_entry(entry: &Entry) -> Vec<u8> {
    let mut enc_state = State::new();
    enc_state.preencode(entry);
    let mut buffer = enc_state.create_buffer();
    enc_state.encode(entry, &mut buffer);
    buffer.to_vec()
}

fn to_doc_url(public_key: String) -> String {
    format!("hypermerge:/{}", public_key)
}

fn to_public_key(doc_url: &str) -> String {
    doc_url[12..].to_string()
}
