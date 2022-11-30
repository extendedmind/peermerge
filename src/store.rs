use async_std::sync::{Arc, Mutex};
use automerge::{ObjId, ObjType, Prop};
use hypercore_protocol::hypercore::compact_encoding::{CompactEncoding, State};
use hypercore_protocol::hypercore::Keypair;
#[cfg(not(target_arch = "wasm32"))]
use random_access_disk::RandomAccessDisk;
use random_access_memory::RandomAccessMemory;
use random_access_storage::RandomAccess;
use std::{collections::HashMap, fmt::Debug, path::PathBuf};

use crate::automerge::{init_doc_from_entries, AutomergeDoc};
#[cfg(not(target_arch = "wasm32"))]
use crate::hypercore::{
    create_new_read_disk_hypercore, create_new_write_disk_hypercore, get_path_from_discovery_key,
};
use crate::{
    automerge::put_object_autocommit,
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
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send + 'static,
{
    pub(crate) fn get_mut(&mut self, discovery_key: &[u8; 32]) -> Option<&mut HypercoreStore<T>> {
        self.docs.get_mut(discovery_key)
    }

    pub(crate) async fn watch_root_props(
        &mut self,
        discovery_key: &[u8; 32],
        root_props: Vec<Prop>,
    ) {
        if let Some(hypercore_store) = self.get_mut(discovery_key) {
            {
                let doc_state = hypercore_store.doc_state();
                {
                    let mut doc_state = doc_state.lock().await;
                    doc_state.watch_root_props(root_props);
                }
            }
        }
    }

    pub(crate) async fn put_object<O: AsRef<ObjId>, P: Into<Prop>>(
        &mut self,
        discovery_key: &[u8; 32],
        obj: O,
        prop: P,
        object: ObjType,
    ) -> anyhow::Result<()> {
        if let Some(hypercore_store) = self.get_mut(&discovery_key.clone()) {
            {
                let doc_state = hypercore_store.doc_state();
                {
                    let mut doc_state = doc_state.lock().await;
                    let write_discovery_key = doc_state.write_discovery_key();
                    let entry = if let Some(doc) = doc_state.doc_mut(&write_discovery_key) {
                        put_object_autocommit(doc, obj, prop, object).unwrap()
                    } else {
                        unimplemented!(
                            "TODO: No proper error code for trying to change before a document is synced"
                        );
                    };
                    let length = {
                        let hypercore_map = hypercore_store.hypercores();
                        let hypercores = hypercore_map.lock().await;
                        let mut write_hypercore =
                            hypercores.get(&write_discovery_key).unwrap().lock().await;
                        write_hypercore.append(&serialize_entry(&entry)).await?
                    };
                    doc_state.set_cursor(&write_discovery_key, length).await;
                }
            }
        }
        Ok(())
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

    pub async fn add_doc_memory(
        &mut self,
        key_pair: Keypair,
        encoded_public_key: String,
        discovery_key: [u8; 32],
        doc: AutomergeDoc,
        data: Vec<u8>,
    ) -> ([u8; 32], String) {
        // Generate a key pair, its discovery key and the public key string
        let public_key = *key_pair.public.as_bytes();

        // Create the memory hypercore
        let (length, hypercore) = create_new_write_memory_hypercore(
            key_pair,
            serialize_entry(&Entry::new_init_doc(data.clone())),
        )
        .await;
        let content = DocContent::new(
            data,
            vec![DocCursor::new(discovery_key.clone(), length)],
            doc,
        );

        // Write the state
        self.repo_state.add_public_key_to_state(&public_key).await;

        // Insert a hypercore store to the doc store
        let hypercore_store = HypercoreStore::new_memory(
            (public_key, discovery_key.clone(), hypercore),
            vec![],
            Some(content),
        )
        .await;
        self.docs.insert(discovery_key.clone(), hypercore_store);

        // Return the doc discovery key and url
        (discovery_key, to_doc_url(encoded_public_key))
    }

    pub async fn register_doc_memory(&mut self, doc_url: &str) -> [u8; 32] {
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
        self.docs.insert(doc_discovery_key.clone(), hypercore_store);
        doc_discovery_key
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

    pub async fn add_doc_disk(
        &mut self,
        key_pair: Keypair,
        encoded_public_key: String,
        discovery_key: [u8; 32],
        doc: AutomergeDoc,
        data: Vec<u8>,
    ) -> ([u8; 32], String) {
        let public_key = *key_pair.public.as_bytes();

        // Create the disk hypercore with init data
        let (length, hypercore) = create_new_write_disk_hypercore(
            &self.prefix,
            key_pair,
            &discovery_key,
            serialize_entry(&Entry::new_init_doc(data.clone())),
        )
        .await;
        let content = DocContent::new(
            data,
            vec![DocCursor::new(discovery_key.clone(), length)],
            doc,
        );

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
        self.docs.insert(discovery_key.clone(), hypercore_store);

        // Return the doc discovery key and url
        (discovery_key, to_doc_url(encoded_public_key))
    }

    pub async fn register_doc_disk(&mut self, doc_url: &str) -> [u8; 32] {
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
        self.docs.insert(doc_discovery_key.clone(), hypercore_store);
        doc_discovery_key
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
    pub async fn public_keys(&self) -> (Option<[u8; 32]>, Vec<[u8; 32]>) {
        let state = self.doc_state.lock().await;
        let state = state.state();
        let peer_public_keys: Vec<[u8; 32]> = state
            .peers
            .iter()
            .map(|peer| peer.public_key.clone())
            .collect();
        (state.public_key, peer_public_keys)
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

pub(crate) async fn create_and_insert_read_memory_hypercores(
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

pub(crate) async fn create_content<T>(
    origin_discovery_key: &[u8; 32],
    write_discovery_key: &[u8; 32],
    hypercores: Arc<Mutex<HashMap<[u8; 32], Arc<Mutex<HypercoreWrapper<T>>>>>>,
) -> anyhow::Result<DocContent>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send + 'static,
{
    let hypercores = hypercores.lock().await;
    // The document starts from the origin, so get that first
    let mut origin_hypercore = hypercores.get(origin_discovery_key).unwrap().lock().await;
    let mut entries = origin_hypercore.entries(0).await?;
    let mut cursors = vec![DocCursor::new(
        origin_discovery_key.clone(),
        entries.len() as u64,
    )];
    for (discovery_key, hypercore) in &*hypercores {
        if discovery_key != origin_discovery_key {
            let mut hypercore = hypercore.lock().await;
            let new_entries = hypercore.entries(0).await?;
            cursors.push(DocCursor::new(
                origin_discovery_key.clone(),
                new_entries.len() as u64,
            ));
            entries.extend(new_entries);
        }
    }

    // Create DocContent from the hypercore
    let (doc, data) = init_doc_from_entries(write_discovery_key, entries);
    Ok(DocContent::new(data, cursors, doc))
}

pub(crate) async fn update_content<T>(
    discovery_key: &[u8; 32],
    content: &mut DocContent,
    hypercores: Arc<Mutex<HashMap<[u8; 32], Arc<Mutex<HypercoreWrapper<T>>>>>>,
) where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send + 'static,
{
    let mut hypercores = hypercores.lock().await;
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
