use async_std::sync::{Arc, Mutex};
use random_access_storage::RandomAccess;
use std::fmt::Debug;

/// Repository is a store for multiple Hypermerges
#[derive(Debug, Clone)]
pub struct Repository<T>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send,
{
    state: Arc<Mutex<T>>,
}
