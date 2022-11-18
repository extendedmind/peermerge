use hypercore_protocol::{
    discovery_key,
    hypercore::{generate_keypair, Hypercore, Keypair, PartialKeypair, PublicKey, Storage},
};
use random_access_disk::RandomAccessDisk;
use random_access_memory::RandomAccessMemory;
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
) -> HypercoreWrapper<RandomAccessDisk> {
    create_new_disk_hypercore(
        prefix,
        PartialKeypair {
            public: key_pair.public,
            secret: Some(key_pair.secret),
        },
        discovery_key,
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
    )
    .await
}

#[cfg(not(target_arch = "wasm32"))]
async fn create_new_disk_hypercore(
    prefix: &PathBuf,
    key_pair: PartialKeypair,
    discovery_key: &str,
) -> HypercoreWrapper<RandomAccessDisk> {
    let hypercore_dir = prefix.join(PathBuf::from(discovery_key));
    let storage = Storage::new_disk(&hypercore_dir, true).await.unwrap();
    HypercoreWrapper::from_disk_hypercore(
        Hypercore::new_with_key_pair(storage, key_pair)
            .await
            .unwrap(),
    )
}

pub(crate) async fn create_new_write_memory_hypercore(
    key_pair: Keypair,
) -> HypercoreWrapper<RandomAccessMemory> {
    create_new_memory_hypercore(PartialKeypair {
        public: key_pair.public,
        secret: Some(key_pair.secret),
    })
    .await
}

pub(crate) async fn create_new_read_memory_hypercore(
    public_key: &str,
) -> HypercoreWrapper<RandomAccessMemory> {
    create_new_memory_hypercore(PartialKeypair {
        public: PublicKey::from_bytes(&hex::decode(public_key).unwrap()).unwrap(),
        secret: None,
    })
    .await
}

async fn create_new_memory_hypercore(
    key_pair: PartialKeypair,
) -> HypercoreWrapper<RandomAccessMemory> {
    let storage = Storage::new_memory().await.unwrap();
    HypercoreWrapper::from_memory_hypercore(
        Hypercore::new_with_key_pair(storage, key_pair)
            .await
            .unwrap(),
    )
}
