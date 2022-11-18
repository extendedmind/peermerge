use hypercore_protocol::{
    discovery_key,
    hypercore::{generate_keypair, Hypercore, Keypair, PartialKeypair, Storage},
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

pub(crate) fn discovery_key_from_public_key(public_key: String) -> String {
    let public_key = hex::decode(public_key).unwrap();
    hex::encode(discovery_key(&public_key))
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) async fn create_new_disk_hypercore(
    prefix: &PathBuf,
    key_pair: Keypair,
    discovery_key: &String,
) -> HypercoreWrapper<RandomAccessDisk> {
    let hypercore_dir = prefix.join(PathBuf::from(discovery_key));
    let storage = Storage::new_disk(&hypercore_dir, true).await.unwrap();
    HypercoreWrapper::from_disk_hypercore(
        Hypercore::new_with_key_pair(
            storage,
            PartialKeypair {
                public: key_pair.public,
                secret: Some(key_pair.secret),
            },
        )
        .await
        .unwrap(),
    )
}

pub(crate) async fn create_new_memory_hypercore(
    key_pair: Keypair,
) -> HypercoreWrapper<RandomAccessMemory> {
    let storage = Storage::new_memory().await.unwrap();
    HypercoreWrapper::from_memory_hypercore(
        Hypercore::new_with_key_pair(
            storage,
            PartialKeypair {
                public: key_pair.public,
                secret: Some(key_pair.secret),
            },
        )
        .await
        .unwrap(),
    )
}
