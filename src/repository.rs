use random_access_storage::RandomAccess;
use std::fmt::Debug;
use std::sync::Arc;

#[cfg(feature = "async-std")]
use async_std::sync::Mutex;
#[cfg(feature = "tokio")]
use tokio::sync::Mutex;

/// Repository is a store for multiple Peermerges
#[derive(Debug, Clone)]
pub struct Repository<T>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send,
{
    state: Arc<Mutex<T>>,
}
