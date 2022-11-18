use automerge::Automerge;
use random_access_disk::RandomAccessDisk;
use random_access_memory::RandomAccessMemory;
use random_access_storage::RandomAccess;
use std::{collections::HashMap, fmt::Debug, path::PathBuf, sync::Arc};

use crate::hypercore::{
    create_new_disk_hypercore, create_new_memory_hypercore, generate_keys, HypercoreWrapper,
};

/// A container for hypermerge documents.
#[derive(Debug)]
pub(crate) struct HypermergeStore<T>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send,
{
    docs: HashMap<String, (Option<Automerge>, HashMap<String, Arc<HypercoreWrapper<T>>>)>,
    state: T,
    prefix: PathBuf,
}
impl<T> HypermergeStore<T>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send,
{
    // pub fn get(&self, discovery_key: &[u8; 32]) -> Option<&Arc<HypercoreWrapper<T>>> {
    //     let hdkey = hex::encode(discovery_key);
    //     self.hypercores.get(&hdkey)
    // }
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

    pub async fn add_doc_memory(&mut self, doc: Automerge) -> String {
        // First generate a key pair, its discovery key and the public key string
        let (key_pair, discovery_key, public_key) = generate_keys();

        // Second: create the memory hypercore
        let hypercore = create_new_memory_hypercore(key_pair).await;

        // Third: store the value to the map
        let mut hypercores: HashMap<String, Arc<HypercoreWrapper<RandomAccessMemory>>> =
            HashMap::new();
        hypercores.insert(discovery_key.clone(), Arc::new(hypercore));
        self.docs.insert(discovery_key, (Some(doc), hypercores));

        // Return the doc url
        to_doc_url(public_key)
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

    pub async fn add_doc_disk(&mut self, doc: Automerge) -> String {
        // First generate a key pair, its discovery key and the public key string
        let (key_pair, discovery_key, public_key) = generate_keys();

        // Second: create the disk hypercore
        let hypercore = create_new_disk_hypercore(&self.prefix, key_pair, &discovery_key).await;

        // Third: store the value to the map
        let mut hypercores: HashMap<String, Arc<HypercoreWrapper<RandomAccessDisk>>> =
            HashMap::new();
        hypercores.insert(discovery_key.clone(), Arc::new(hypercore));
        self.docs.insert(discovery_key, (Some(doc), hypercores));

        // Return the doc url
        to_doc_url(public_key)
    }
}

fn to_doc_url(public_key: String) -> String {
    format!("hypermerge:/{}", public_key)
}
