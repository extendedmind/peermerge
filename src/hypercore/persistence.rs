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

pub(crate) fn generate_keys() -> (Keypair, String, String) {
    let key_pair = generate_keypair();
    let discovery_key = hex::encode(discovery_key(&key_pair.public.to_bytes()));
    let public_key = hex::encode(key_pair.public);
    (key_pair, discovery_key, public_key)
}

pub(crate) fn discovery_key_from_public_key(public_key: &str) -> String {
    let public_key = hex::decode(public_key).unwrap();
    hex::encode(discovery_key(&public_key))
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) async fn create_new_write_disk_hypercore(
    prefix: &PathBuf,
    key_pair: Keypair,
    discovery_key: &str,
    init_data: Vec<u8>,
) -> HypercoreWrapper<RandomAccessDisk> {
    create_new_disk_hypercore(
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
pub(crate) async fn create_new_read_disk_hypercore(
    prefix: &PathBuf,
    public_key: &str,
    discovery_key: &str,
) -> HypercoreWrapper<RandomAccessDisk> {
    create_new_disk_hypercore(
        prefix,
        PartialKeypair {
            public: PublicKey::from_bytes(&hex::decode(public_key).unwrap()).unwrap(),
            secret: None,
        },
        discovery_key,
        None,
    )
    .await
}

#[cfg(not(target_arch = "wasm32"))]
async fn create_new_disk_hypercore(
    prefix: &PathBuf,
    key_pair: PartialKeypair,
    discovery_key: &str,
    init_data: Option<Vec<u8>>,
) -> HypercoreWrapper<RandomAccessDisk> {
    let hypercore_dir = prefix.join(PathBuf::from(discovery_key));
    let storage = Storage::new_disk(&hypercore_dir, true).await.unwrap();
    let mut hypercore = Hypercore::new_with_key_pair(storage, key_pair)
        .await
        .unwrap();
    if let Some(init_data) = init_data {
        hypercore.append(&init_data).await.unwrap();
    }
    HypercoreWrapper::from_disk_hypercore(hypercore)
}

pub(crate) async fn create_new_write_memory_hypercore(
    key_pair: Keypair,
    init_data: Vec<u8>,
) -> HypercoreWrapper<RandomAccessMemory> {
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
    public_key: &str,
) -> HypercoreWrapper<RandomAccessMemory> {
    create_new_memory_hypercore(
        PartialKeypair {
            public: PublicKey::from_bytes(&hex::decode(public_key).unwrap()).unwrap(),
            secret: None,
        },
        None,
    )
    .await
}

async fn create_new_memory_hypercore(
    key_pair: PartialKeypair,
    init_data: Option<Vec<u8>>,
) -> HypercoreWrapper<RandomAccessMemory> {
    let storage = Storage::new_memory().await.unwrap();
    let mut hypercore = Hypercore::new_with_key_pair(storage, key_pair)
        .await
        .unwrap();
    if let Some(init_data) = init_data {
        hypercore.append(&init_data).await.unwrap();
    }
    HypercoreWrapper::from_memory_hypercore(hypercore)
}
