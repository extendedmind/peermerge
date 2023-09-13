use hypercore_protocol::hypercore::{
    CacheOptionsBuilder, HypercoreBuilder, PartialKeypair, SigningKey, Storage, VerifyingKey,
};
#[cfg(not(target_arch = "wasm32"))]
use random_access_disk::RandomAccessDisk;
use random_access_memory::RandomAccessMemory;
#[cfg(not(target_arch = "wasm32"))]
use std::path::PathBuf;

use super::HypercoreWrapper;
use crate::{AccessType, FeedDiscoveryKey, FeedPublicKey};

#[cfg(not(target_arch = "wasm32"))]
pub(crate) async fn create_new_write_disk_hypercore(
    prefix: &PathBuf,
    signing_key: SigningKey,
    discovery_key: &FeedDiscoveryKey,
    encrypted: bool,
    encryption_key: &Option<Vec<u8>>,
) -> (HypercoreWrapper<RandomAccessDisk>, Option<Vec<u8>>) {
    let hypercore_dir = get_path_from_discovery_key(prefix, discovery_key);
    let storage = Storage::new_disk(&hypercore_dir, true).await.unwrap();
    let hypercore = HypercoreBuilder::new(storage)
        .key_pair(PartialKeypair {
            public: signing_key.verifying_key(),
            secret: Some(signing_key),
        })
        .node_cache_options(CacheOptionsBuilder::new())
        .build()
        .await
        .unwrap();
    let (wrapper, encryption_key) = HypercoreWrapper::from_disk_hypercore(
        hypercore,
        AccessType::ReadWrite,
        encrypted,
        encryption_key,
        true,
    );
    (wrapper, encryption_key)
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) async fn create_new_read_disk_hypercore(
    prefix: &PathBuf,
    public_key: &FeedPublicKey,
    discovery_key: &FeedDiscoveryKey,
    access_type: AccessType,
    encrypted: bool,
    encryption_key: &Option<Vec<u8>>,
) -> HypercoreWrapper<RandomAccessDisk> {
    let hypercore_dir = get_path_from_discovery_key(prefix, discovery_key);
    let storage = Storage::new_disk(&hypercore_dir, true).await.unwrap();
    let hypercore = HypercoreBuilder::new(storage)
        .key_pair(PartialKeypair {
            public: VerifyingKey::from_bytes(public_key).unwrap(),
            secret: None,
        })
        .node_cache_options(CacheOptionsBuilder::new())
        .build()
        .await
        .unwrap();
    HypercoreWrapper::from_disk_hypercore(hypercore, access_type, encrypted, encryption_key, false)
        .0
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) async fn open_disk_hypercore(
    prefix: &PathBuf,
    discovery_key: &FeedDiscoveryKey,
    access_type: AccessType,
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
        HypercoreWrapper::from_disk_hypercore(
            hypercore,
            access_type,
            encrypted,
            encryption_key,
            false,
        )
        .0,
    )
}

pub(crate) async fn create_new_write_memory_hypercore(
    signing_key: SigningKey,
    encrypted: bool,
    encryption_key: &Option<Vec<u8>>,
    reattach: bool,
) -> (HypercoreWrapper<RandomAccessMemory>, Option<Vec<u8>>) {
    create_new_memory_hypercore(
        PartialKeypair {
            public: signing_key.verifying_key(),
            secret: Some(signing_key),
        },
        AccessType::ReadWrite,
        encrypted,
        encryption_key,
        reattach,
    )
    .await
}

pub(crate) async fn create_new_read_memory_hypercore(
    public_key: &FeedPublicKey,
    access_type: AccessType,
    encrypted: bool,
    encryption_key: &Option<Vec<u8>>,
) -> HypercoreWrapper<RandomAccessMemory> {
    let result = create_new_memory_hypercore(
        PartialKeypair {
            public: VerifyingKey::from_bytes(public_key).unwrap(),
            secret: None,
        },
        access_type,
        encrypted,
        encryption_key,
        false,
    )
    .await;
    result.0
}

async fn create_new_memory_hypercore(
    key_pair: PartialKeypair,
    access_type: AccessType,
    encrypted: bool,
    encryption_key: &Option<Vec<u8>>,
    reattach: bool,
) -> (HypercoreWrapper<RandomAccessMemory>, Option<Vec<u8>>) {
    let storage = Storage::new_memory().await.unwrap();
    let hypercore = HypercoreBuilder::new(storage)
        .key_pair(key_pair)
        .build()
        .await
        .unwrap();

    let (wrapper, encryption_key) = HypercoreWrapper::from_memory_hypercore(
        hypercore,
        access_type,
        encrypted,
        encryption_key,
        !reattach,
    );
    (wrapper, encryption_key)
}

#[cfg(not(target_arch = "wasm32"))]
fn get_path_from_discovery_key(prefix: &PathBuf, discovery_key: &FeedDiscoveryKey) -> PathBuf {
    let encoded = data_encoding::BASE32_NOPAD.encode(discovery_key);
    prefix.join(PathBuf::from(encoded))
}
