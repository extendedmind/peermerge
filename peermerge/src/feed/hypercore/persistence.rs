use hypercore_protocol::hypercore::{
    CacheOptionsBuilder, HypercoreBuilder, Keypair, PartialKeypair, PublicKey, Storage,
};
#[cfg(not(target_arch = "wasm32"))]
use random_access_disk::RandomAccessDisk;
use random_access_memory::RandomAccessMemory;
#[cfg(not(target_arch = "wasm32"))]
use std::path::PathBuf;

use super::HypercoreWrapper;

#[cfg(not(target_arch = "wasm32"))]
pub(crate) async fn create_new_write_disk_hypercore(
    prefix: &PathBuf,
    key_pair: Keypair,
    discovery_key: &[u8; 32],
    init_data: Vec<Vec<u8>>,
    encrypted: bool,
    encryption_key: &Option<Vec<u8>>,
) -> (u64, HypercoreWrapper<RandomAccessDisk>, Option<Vec<u8>>) {
    let hypercore_dir = get_path_from_discovery_key(prefix, discovery_key);
    let storage = Storage::new_disk(&hypercore_dir, true).await.unwrap();
    let hypercore = HypercoreBuilder::new(storage)
        .key_pair(PartialKeypair {
            public: key_pair.public,
            secret: Some(key_pair.secret),
        })
        .node_cache_options(CacheOptionsBuilder::new())
        .build()
        .await
        .unwrap();
    let (mut wrapper, encryption_key) =
        HypercoreWrapper::from_disk_hypercore(hypercore, false, encrypted, encryption_key, true);
    let mut len: u64 = 0;
    for data in init_data {
        len = wrapper.append(&data).await.unwrap();
    }
    (len, wrapper, encryption_key)
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) async fn create_new_read_disk_hypercore(
    prefix: &PathBuf,
    public_key: &[u8; 32],
    discovery_key: &[u8; 32],
    proxy: bool,
    encrypted: bool,
    encryption_key: &Option<Vec<u8>>,
) -> (u64, HypercoreWrapper<RandomAccessDisk>) {
    let hypercore_dir = get_path_from_discovery_key(prefix, discovery_key);
    let storage = Storage::new_disk(&hypercore_dir, true).await.unwrap();
    let hypercore = HypercoreBuilder::new(storage)
        .key_pair(PartialKeypair {
            public: PublicKey::from_bytes(public_key).unwrap(),
            secret: None,
        })
        .node_cache_options(CacheOptionsBuilder::new())
        .build()
        .await
        .unwrap();
    (
        hypercore.info().length,
        HypercoreWrapper::from_disk_hypercore(hypercore, proxy, encrypted, encryption_key, false).0,
    )
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) async fn open_disk_hypercore(
    prefix: &PathBuf,
    discovery_key: &[u8; 32],
    proxy: bool,
    encrypted: bool,
    encryption_key: &Option<Vec<u8>>,
) -> (u64, HypercoreWrapper<RandomAccessDisk>) {
    let hypercore_dir = get_path_from_discovery_key(prefix, discovery_key);
    let storage = Storage::new_disk(&hypercore_dir, false).await.unwrap();
    let hypercore = HypercoreBuilder::new(storage)
        .open(true)
        .node_cache_options(CacheOptionsBuilder::new())
        .build()
        .await
        .unwrap();
    (
        hypercore.info().length,
        HypercoreWrapper::from_disk_hypercore(hypercore, proxy, encrypted, encryption_key, false).0,
    )
}

pub(crate) async fn create_new_write_memory_hypercore(
    key_pair: Keypair,
    init_data: Option<Vec<Vec<u8>>>,
    encrypted: bool,
    encryption_key: &Option<Vec<u8>>,
) -> (u64, HypercoreWrapper<RandomAccessMemory>, Option<Vec<u8>>) {
    create_new_memory_hypercore(
        PartialKeypair {
            public: key_pair.public,
            secret: Some(key_pair.secret),
        },
        init_data,
        false,
        encrypted,
        encryption_key,
    )
    .await
}

pub(crate) async fn create_new_read_memory_hypercore(
    public_key: &[u8; 32],
    proxy: bool,
    encrypted: bool,
    encryption_key: &Option<Vec<u8>>,
) -> (u64, HypercoreWrapper<RandomAccessMemory>) {
    let result = create_new_memory_hypercore(
        PartialKeypair {
            public: PublicKey::from_bytes(public_key).unwrap(),
            secret: None,
        },
        None,
        proxy,
        encrypted,
        encryption_key,
    )
    .await;
    (result.0, result.1)
}

async fn create_new_memory_hypercore(
    key_pair: PartialKeypair,
    init_data: Option<Vec<Vec<u8>>>,
    proxy: bool,
    encrypted: bool,
    encryption_key: &Option<Vec<u8>>,
) -> (u64, HypercoreWrapper<RandomAccessMemory>, Option<Vec<u8>>) {
    let storage = Storage::new_memory().await.unwrap();
    let hypercore = HypercoreBuilder::new(storage)
        .key_pair(key_pair)
        .build()
        .await
        .unwrap();

    let (mut wrapper, encryption_key) = HypercoreWrapper::from_memory_hypercore(
        hypercore,
        proxy,
        encrypted,
        encryption_key,
        init_data.is_some(),
    );
    let len = if let Some(init_data) = init_data {
        let mut len: u64 = 0;
        for data in init_data {
            len = wrapper.append(&data).await.unwrap();
        }
        len
    } else {
        0
    };
    (len, wrapper, encryption_key)
}

#[cfg(not(target_arch = "wasm32"))]
fn get_path_from_discovery_key(prefix: &PathBuf, discovery_key: &[u8; 32]) -> PathBuf {
    let encoded = data_encoding::BASE32_NOPAD.encode(discovery_key);
    prefix.join(PathBuf::from(encoded))
}
