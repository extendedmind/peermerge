use dashmap::DashMap;
use futures::lock::Mutex;
use std::sync::Arc;

use crate::common::utils::YieldNow;
use crate::feed::{Feed, FeedPersistence};

pub(crate) async fn get_feed<T>(
    feeds: &Arc<DashMap<[u8; 32], Arc<Mutex<Feed<T>>>>>,
    discovery_key: &[u8; 32],
) -> Option<Arc<Mutex<Feed<T>>>>
where
    T: FeedPersistence,
{
    loop {
        match feeds.try_get(discovery_key) {
            dashmap::try_result::TryResult::Absent => {
                return None;
            }
            dashmap::try_result::TryResult::Locked => {
                YieldNow(false).await;
            }
            dashmap::try_result::TryResult::Present(value) => {
                return Some(value.clone());
            }
        }
    }
}

pub(crate) async fn get_feed_discovery_keys<T>(
    feeds: &Arc<DashMap<[u8; 32], Arc<Mutex<Feed<T>>>>>,
) -> Vec<[u8; 32]>
where
    T: FeedPersistence,
{
    // I believe this needs to be resolved xacrimon/dashmap/issues/151 for this to guarantee
    // to not deadlock.
    feeds.iter().map(|multi| multi.key().clone()).collect()
}
