use hypercore_protocol::{
    discovery_key,
    hypercore::{generate_keypair, Hypercore, Keypair, PartialKeypair, PublicKey, Storage},
};
#[cfg(not(target_arch = "wasm32"))]
use random_access_disk::RandomAccessDisk;
use random_access_memory::RandomAccessMemory;
#[cfg(not(target_arch = "wasm32"))]
use std::path::PathBuf;

use super::HypercoreWrapper;

pub(crate) fn generate_keys() -> (Keypair, [u8; 32]) {
    let key_pair = generate_keypair();
    let discovery_key = discovery_key(&key_pair.public.to_bytes());
    (key_pair, discovery_key)
}

pub(crate) fn discovery_key_from_public_key(public_key: &[u8; 32]) -> [u8; 32] {
    discovery_key(public_key)
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) async fn create_new_write_disk_hypercore(
    prefix: &PathBuf,
    key_pair: Keypair,
    discovery_key: &[u8; 32],
    init_data: Vec<u8>,
    encrypted: bool,
) -> (u64, HypercoreWrapper<RandomAccessDisk>, Option<Vec<u8>>) {
    create_new_disk_hypercore(
        prefix,
        PartialKeypair {
            public: key_pair.public,
            secret: Some(key_pair.secret),
        },
        discovery_key,
        init_data,
        encrypted,
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
    )
    .await
}

#[cfg(not(target_arch = "wasm32"))]
async fn open_disk_hypercore(
    prefix: &PathBuf,
    key_pair: PartialKeypair,
    discovery_key: &[u8; 32],
) -> (u64, HypercoreWrapper<RandomAccessDisk>) {
    let hypercore_dir = get_path_from_discovery_key(prefix, discovery_key);
    let storage = Storage::new_disk(&hypercore_dir, false).await.unwrap();
    let hypercore = Hypercore::new_with_key_pair(storage, key_pair)
        .await
        .unwrap();
    (
        hypercore.info().length,
        HypercoreWrapper::from_disk_hypercore(hypercore, false, None, false).0,
    )
}

#[cfg(not(target_arch = "wasm32"))]
async fn create_new_disk_hypercore(
    prefix: &PathBuf,
    key_pair: PartialKeypair,
    discovery_key: &[u8; 32],
    init_data: Vec<u8>,
    encrypted: bool,
) -> (u64, HypercoreWrapper<RandomAccessDisk>, Option<Vec<u8>>) {
    let hypercore_dir = get_path_from_discovery_key(prefix, discovery_key);
    let storage = Storage::new_disk(&hypercore_dir, false).await.unwrap();
    let hypercore = Hypercore::new_with_key_pair(storage, key_pair)
        .await
        .unwrap();
    if hypercore.info().length != 0 {
        panic!("Trying to create a hypercore that already exists on disk.");
    }
    let (mut wrapper, encryption_key) =
        HypercoreWrapper::from_disk_hypercore(hypercore, encrypted, None, true);
    let len = wrapper.append(&init_data).await.unwrap();
    (len, wrapper, encryption_key)
}

pub(crate) async fn create_new_write_memory_hypercore(
    key_pair: Keypair,
    init_data: Vec<u8>,
    encrypted: bool,
) -> (u64, HypercoreWrapper<RandomAccessMemory>, Option<Vec<u8>>) {
    create_new_memory_hypercore(
        PartialKeypair {
            public: key_pair.public,
            secret: Some(key_pair.secret),
        },
        Some(init_data),
        encrypted,
    )
    .await
}

pub(crate) async fn create_new_read_memory_hypercore(
    public_key: &[u8; 32],
) -> (u64, HypercoreWrapper<RandomAccessMemory>, Option<Vec<u8>>) {
    create_new_memory_hypercore(
        PartialKeypair {
            public: PublicKey::from_bytes(public_key).unwrap(),
            secret: None,
        },
        None,
        false,
    )
    .await
}

async fn create_new_memory_hypercore(
    key_pair: PartialKeypair,
    init_data: Option<Vec<u8>>,
    encrypted: bool,
) -> (u64, HypercoreWrapper<RandomAccessMemory>, Option<Vec<u8>>) {
    let storage = Storage::new_memory().await.unwrap();
    let hypercore = Hypercore::new_with_key_pair(storage, key_pair)
        .await
        .unwrap();

    let (mut wrapper, encryption_key) =
        HypercoreWrapper::from_memory_hypercore(hypercore, encrypted, None, true);
    let len = if let Some(init_data) = init_data {
        wrapper.append(&init_data).await.unwrap()
    } else {
        0
    };
    (len, wrapper, encryption_key)
}

#[cfg(not(target_arch = "wasm32"))]
fn get_path_from_discovery_key(prefix: &PathBuf, discovery_key: &[u8; 32]) -> PathBuf {
    prefix.join(PathBuf::from(hex::encode(discovery_key)))
}
