use async_std::sync::{Arc, Mutex};
use automerge::Automerge;
#[cfg(not(target_arch = "wasm32"))]
use random_access_disk::RandomAccessDisk;
use random_access_memory::RandomAccessMemory;
use random_access_storage::RandomAccess;
use std::{collections::HashMap, fmt::Debug, path::PathBuf};

#[cfg(not(target_arch = "wasm32"))]
use crate::hypercore::{create_new_read_disk_hypercore, create_new_write_disk_hypercore};
use crate::hypercore::{
    create_new_read_memory_hypercore, create_new_write_memory_hypercore,
    discovery_key_from_public_key, generate_keys, HypercoreWrapper,
};
/// A container for hypermerge documents.
#[derive(Debug)]
pub(crate) struct HypermergeStore<T>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send,
{
    docs: HashMap<
        [u8; 32],
        (
            Option<Arc<Mutex<Automerge>>>,
            HashMap<[u8; 32], Arc<Mutex<HypercoreWrapper<T>>>>,
        ),
    >,
    state: T,
    prefix: PathBuf,
}
impl<T> HypermergeStore<T>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send,
{
    pub fn get_root_hypercore(
        &mut self,
        doc_url: &str,
    ) -> Option<&Arc<Mutex<HypercoreWrapper<T>>>> {
        let public_key = to_public_key(doc_url);
        let discovery_key = discovery_key_from_public_key(&public_key);
        self.docs
            .get(&discovery_key)
            .and_then(|doc_state| doc_state.1.get(&discovery_key))
    }

    pub fn get_mut(
        &mut self,
        doc_url: &str,
    ) -> Option<&mut (
        Option<Arc<Mutex<Automerge>>>,
        HashMap<[u8; 32], Arc<Mutex<HypercoreWrapper<T>>>>,
    )> {
        let public_key = to_public_key(doc_url);
        let discovery_key = discovery_key_from_public_key(&public_key);
        self.docs.get_mut(&discovery_key)
    }
}

impl HypermergeStore<RandomAccessMemory> {
    pub fn new_memory() -> Self {
        let docs = HashMap::new();
        let state = RandomAccessMemory::default();
        Self {
            docs,
            state,
            prefix: PathBuf::new(),
        }
    }

    pub async fn add_doc_memory(&mut self, mut doc: Automerge) -> String {
        // First generate a key pair, its discovery key and the public key string
        let (key_pair, discovery_key, public_key) = generate_keys();

        // Second: create the memory hypercore
        let hypercore = create_new_write_memory_hypercore(key_pair, doc.save()).await;

        // Third: store the value to the map
        let mut hypercores: HashMap<[u8; 32], Arc<Mutex<HypercoreWrapper<RandomAccessMemory>>>> =
            HashMap::new();
        hypercores.insert(discovery_key.clone(), Arc::new(Mutex::new(hypercore)));
        self.docs
            .insert(discovery_key, (Some(Arc::new(Mutex::new(doc))), hypercores));

        // Return the doc url
        to_doc_url(public_key)
    }

    pub async fn register_doc_memory(&mut self, doc_url: &str) {
        // First process keys from doc URL
        let public_key = to_public_key(doc_url);
        let discovery_key = discovery_key_from_public_key(&public_key);

        // Second: create the memory hypercore
        let hypercore = create_new_read_memory_hypercore(&public_key).await;

        // Third: store the value to the map
        let mut hypercores: HashMap<[u8; 32], Arc<Mutex<HypercoreWrapper<RandomAccessMemory>>>> =
            HashMap::new();
        hypercores.insert(discovery_key.clone(), Arc::new(Mutex::new(hypercore)));
        self.docs.insert(discovery_key, (None, hypercores));
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl HypermergeStore<RandomAccessDisk> {
    pub async fn new_disk(data_root_dir: &PathBuf) -> Self {
        let docs = HashMap::new();
        let state_path = data_root_dir.join(PathBuf::from("hypermerge_state.bin"));
        let state = RandomAccessDisk::builder(state_path).build().await.unwrap();
        Self {
            docs,
            state,
            prefix: data_root_dir.to_path_buf(),
        }
    }

    pub async fn add_doc_disk(&mut self, mut doc: Automerge) -> String {
        // First generate a key pair, its discovery key and the public key string
        let (key_pair, discovery_key, public_key) = generate_keys();

        // Second: create the disk hypercore with init data
        let hypercore =
            create_new_write_disk_hypercore(&self.prefix, key_pair, &discovery_key, doc.save())
                .await;

        // Third: store the value to the map
        let mut hypercores: HashMap<[u8; 32], Arc<Mutex<HypercoreWrapper<RandomAccessDisk>>>> =
            HashMap::new();
        hypercores.insert(discovery_key.clone(), Arc::new(Mutex::new(hypercore)));
        self.docs.insert(
            discovery_key,
            (Some(Arc::new(Mutex::from(doc))), hypercores),
        );

        // Return the doc url
        to_doc_url(public_key)
    }

    pub async fn register_doc_disk(&mut self, doc_url: &str) {
        // First process keys from doc URL
        let public_key = to_public_key(doc_url);
        let discovery_key = discovery_key_from_public_key(&public_key);

        // Second: create the disk hypercore
        let hypercore =
            create_new_read_disk_hypercore(&self.prefix, &public_key, &discovery_key).await;

        // Third: store the value to the map
        let mut hypercores: HashMap<[u8; 32], Arc<Mutex<HypercoreWrapper<RandomAccessDisk>>>> =
            HashMap::new();
        hypercores.insert(discovery_key.clone(), Arc::new(Mutex::new(hypercore)));
        self.docs.insert(discovery_key, (None, hypercores));
    }
}

fn to_doc_url(public_key: String) -> String {
    format!("hypermerge:/{}", public_key)
}

fn to_public_key(doc_url: &str) -> String {
    doc_url[12..].to_string()
}
