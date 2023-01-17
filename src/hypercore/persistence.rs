use hypercore_protocol::{
    discovery_key,
    hypercore::{generate_keypair, Hypercore, Keypair, PartialKeypair, PublicKey, Storage},
};
#[cfg(not(target_arch = "wasm32"))]
use random_access_disk::RandomAccessDisk;
use random_access_memory::RandomAccessMemory;
use std::convert::TryInto;
#[cfg(not(target_arch = "wasm32"))]
use std::path::PathBuf;

use super::HypercoreWrapper;

pub(crate) fn generate_keys() -> (Keypair, String, [u8; 32]) {
    let key_pair = generate_keypair();
    let discovery_key = discovery_key(&key_pair.public.to_bytes());
    let encoded_public_key = hex::encode(key_pair.public);
    (key_pair, encoded_public_key, discovery_key)
}

pub(crate) fn keys_from_public_key(public_key: &str) -> ([u8; 32], [u8; 32]) {
    let public_key: [u8; 32] = hex::decode(public_key).unwrap().try_into().unwrap();
    (public_key, discovery_key_from_public_key(&public_key))
}

pub(crate) fn discovery_key_from_public_key(public_key: &[u8; 32]) -> [u8; 32] {
    discovery_key(public_key)
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) async fn open_write_disk_hypercore(
    prefix: &PathBuf,
    key_pair: Keypair,
    discovery_key: &[u8; 32],
    init_data: Vec<u8>,
) -> (u64, HypercoreWrapper<RandomAccessDisk>) {
    open_disk_hypercore(
        prefix,
        PartialKeypair {
            public: key_pair.public,
            secret: Some(key_pair.secret),
        },
        discovery_key,
        Some(init_data),
    )
    .await
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) async fn open_read_disk_hypercore(
    prefix: &PathBuf,
    public_key: &[u8; 32],
    discovery_key: &[u8; 32],
) -> (u64, HypercoreWrapper<RandomAccessDisk>) {
    open_disk_hypercore(
        prefix,
        PartialKeypair {
            public: PublicKey::from_bytes(public_key).unwrap(),
            secret: None,
        },
        discovery_key,
        None,
    )
    .await
}

#[cfg(not(target_arch = "wasm32"))]
async fn open_disk_hypercore(
    prefix: &PathBuf,
    key_pair: PartialKeypair,
    discovery_key: &[u8; 32],
    init_data: Option<Vec<u8>>,
) -> (u64, HypercoreWrapper<RandomAccessDisk>) {
    let hypercore_dir = get_path_from_discovery_key(prefix, discovery_key);
    let storage = Storage::new_disk(&hypercore_dir, false).await.unwrap();
    let mut hypercore = Hypercore::new_with_key_pair(storage, key_pair)
        .await
        .unwrap();
    let len = if let Some(init_data) = init_data {
        hypercore.append(&init_data).await.unwrap().length
    } else {
        0
    };
    (len, HypercoreWrapper::from_disk_hypercore(hypercore))
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn get_path_from_discovery_key(prefix: &PathBuf, discovery_key: &[u8; 32]) -> PathBuf {
    prefix.join(PathBuf::from(hex::encode(discovery_key)))
}

pub(crate) async fn create_new_write_memory_hypercore(
    key_pair: Keypair,
    init_data: Vec<u8>,
) -> (u64, HypercoreWrapper<RandomAccessMemory>) {
    create_new_memory_hypercore(
        PartialKeypair {
            public: key_pair.public,
            secret: Some(key_pair.secret),
        },
        Some(init_data),
    )
    .await
}

pub(crate) async fn create_new_read_memory_hypercore(
    public_key: &[u8; 32],
) -> (u64, HypercoreWrapper<RandomAccessMemory>) {
    create_new_memory_hypercore(
        PartialKeypair {
            public: PublicKey::from_bytes(public_key).unwrap(),
            secret: None,
        },
        None,
    )
    .await
}

async fn create_new_memory_hypercore(
    key_pair: PartialKeypair,
    init_data: Option<Vec<u8>>,
) -> (u64, HypercoreWrapper<RandomAccessMemory>) {
    let storage = Storage::new_memory().await.unwrap();
    let mut hypercore = Hypercore::new_with_key_pair(storage, key_pair)
        .await
        .unwrap();
    let len = if let Some(init_data) = init_data {
        hypercore.append(&init_data).await.unwrap().length
    } else {
        0
    };
    (len, HypercoreWrapper::from_memory_hypercore(hypercore))
}
