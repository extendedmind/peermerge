use dashmap::DashMap;
use futures::channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
#[cfg(not(target_arch = "wasm32"))]
use random_access_disk::RandomAccessDisk;
use random_access_memory::RandomAccessMemory;
use random_access_storage::RandomAccess;
use std::fmt::Debug;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{debug, instrument};

#[cfg(feature = "async-std")]
use async_std::sync::Mutex;
#[cfg(all(not(target_arch = "wasm32"), feature = "async-std"))]
use async_std::task;
#[cfg(feature = "tokio")]
use tokio::sync::Mutex;
#[cfg(all(not(target_arch = "wasm32"), feature = "tokio"))]
use tokio::task;
#[cfg(target_arch = "wasm32")]
use wasm_bindgen_futures::spawn_local;

use crate::common::storage::RepositoryStateWrapper;
use crate::common::PeerEvent;
use crate::feed::{Feed, Protocol, IO};
use crate::{Peermerge, StateEvent};

/// PeermergeRepository is a store for multiple Peermerges
#[derive(Debug, Clone)]
pub struct PeermergeRepository<T>
where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send,
{
    /// Name of the repository
    name: String,
    /// Current storable state
    repository_state: Arc<Mutex<RepositoryStateWrapper<T>>>,
    /// Created peermerges
    peermerges: Arc<DashMap<[u8; 32], Peermerge<T>>>,
    /// Map of independent docs' discovery keys with their children.
    peermerge_dependencies: Arc<DashMap<[u8; 32], Vec<[u8; 32]>>>,
    /// All feeds
    feeds: Arc<DashMap<[u8; 32], Arc<Mutex<Feed<T>>>>>,
    /// Sender for events
    state_event_sender: Arc<Mutex<Option<UnboundedSender<StateEvent>>>>,
}

impl<T> PeermergeRepository<T> where
    T: RandomAccess<Error = Box<dyn std::error::Error + Send + Sync>> + Debug + Send + 'static
{
}

//////////////////////////////////////////////////////
//
// RandomAccessMemory

// impl PeermergeRepository<RandomAccessMemory> {
//     pub async fn create_new_memory() -> Self {
//         let state = RepositoryStateWrapper::new_memory().await;
//         Self {
//             name: "".to_string(),
//             repository_state: Arc::new(Mutex::new(state)),
//             peermerges: Arc::new(DashMap::new()),
//             peermerge_dependencies: Arc::new(DashMap::new()),
//             feeds: Arc::new(DashMap::new()),
//             state_event_sender: Arc::new(Mutex::new(None)),
//         }
//     }

//     pub async fn connect_protocol_memory<T>(
//         &mut self,
//         protocol: &mut Protocol<T>,
//         state_event_sender: &mut UnboundedSender<StateEvent>,
//     ) -> anyhow::Result<()>
//     where
//         T: IO,
//     {
//         {
//             *self.state_event_sender.lock().await = Some(state_event_sender.clone());
//         }
//         let (mut peer_event_sender, peer_event_receiver): (
//             UnboundedSender<PeerEvent>,
//             UnboundedReceiver<PeerEvent>,
//         ) = unbounded();

//         let state_event_sender_for_task = state_event_sender.clone();
//         let doc_state = self.doc_state.clone();
//         let discovery_key_for_task = self.discovery_key.clone();
//         let feeds_for_task = self.feeds.clone();
//         let name_for_task = self.name.clone();
//         let task_span = tracing::debug_span!("call_on_peer_event_memory").or_current();
//         #[cfg(not(target_arch = "wasm32"))]
//         task::spawn(async move {
//             let _entered = task_span.enter();
//             on_peer_event_memory(
//                 &discovery_key_for_task,
//                 peer_event_receiver,
//                 state_event_sender_for_task,
//                 doc_state,
//                 feeds_for_task,
//                 &name_for_task,
//                 proxy,
//                 &encryption_key_for_task,
//             )
//             .await;
//         });
//         #[cfg(target_arch = "wasm32")]
//         spawn_local(async move {
//             let _entered = task_span.enter();
//             on_peer_event_memory(
//                 &discovery_key_for_task,
//                 peer_event_receiver,
//                 state_event_sender_for_task,
//                 doc_state,
//                 feeds_for_task,
//                 &name_for_task,
//                 proxy,
//                 &encryption_key_for_task,
//             )
//             .await;
//         });

//         on_protocol(
//             protocol,
//             self.doc_state.clone(),
//             self.feeds.clone(),
//             &mut peer_event_sender,
//         )
//         .await?;
//         Ok(())
//     }
// }

//////////////////////////////////////////////////////
//
// RandomAccessDisk

#[cfg(not(target_arch = "wasm32"))]
impl PeermergeRepository<RandomAccessDisk> {
    pub async fn open_disk(data_root_dir: PathBuf) -> Self {
        let state = RepositoryStateWrapper::open_disk(&data_root_dir).await;
        Self {
            name: "".to_string(),
            repository_state: Arc::new(Mutex::new(state)),
            peermerges: Arc::new(DashMap::new()),
            peermerge_dependencies: Arc::new(DashMap::new()),
            feeds: Arc::new(DashMap::new()),
            state_event_sender: Arc::new(Mutex::new(None)),
        }
    }
}

// #[instrument(level = "debug", skip_all)]
// async fn on_peer_event_memory(
//     doc_discovery_key: &[u8; 32],
//     mut peer_event_receiver: UnboundedReceiver<PeerEvent>,
//     mut state_event_sender: UnboundedSender<StateEvent>,
//     mut feeds: Arc<DashMap<[u8; 32], Arc<Mutex<Feed<RandomAccessMemory>>>>>,
//     name: &str,
//     proxy: bool,
//     encryption_key: &Option<Vec<u8>>,
// ) {
//     while let Some(event) = peer_event_receiver.next().await {
//         debug!("Received event {:?}", event);
//         match event {
//             PeerEvent::NewPeersBroadcasted(public_keys) => {
//                 let changed = {
//                     let mut doc_state = doc_state.lock().await;
//                     doc_state
//                         .add_peer_public_keys_to_state(public_keys.clone())
//                         .await
//                 };
//                 if changed {
//                     {
//                         // Create and insert all new feeds
//                         create_and_insert_read_memory_feeds(
//                             public_keys.clone(),
//                             feeds.clone(),
//                             proxy,
//                             encryption_key,
//                         )
//                         .await;
//                     }
//                     {
//                         notify_new_peers_created(doc_discovery_key, &mut feeds, public_keys).await;
//                     }
//                 }
//             }
//             _ => {
//                 process_peer_event(
//                     event,
//                     doc_discovery_key,
//                     &mut state_event_sender,
//                     &mut doc_state,
//                     &mut feeds,
//                     name,
//                     proxy,
//                 )
//                 .await
//             }
//         }
//     }
//     debug!("Exiting");
// }

// async fn create_and_insert_read_memory_feeds(
//     public_keys: Vec<[u8; 32]>,
//     feeds: Arc<DashMap<[u8; 32], Arc<Mutex<Feed<RandomAccessMemory>>>>>,
//     proxy: bool,
//     encryption_key: &Option<Vec<u8>>,
// ) {
//     for public_key in public_keys {
//         let discovery_key = discovery_key_from_public_key(&public_key);
//         // Make sure to insert only once even if two protocols notice the same new
//         // feed at the same time using the entry API.

//         // There is a deadlock possibility with entry(), so we need to loop and yield
//         let mut entry_found = false;
//         while !entry_found {
//             if let Some(entry) = feeds.try_entry(discovery_key.clone()) {
//                 match entry {
//                     dashmap::mapref::entry::Entry::Occupied(_) => {
//                         debug!("Concurrent creating of feeds noticed, continuing.");
//                     }
//                     dashmap::mapref::entry::Entry::Vacant(vacant) => {
//                         let (_, feed) = create_new_read_memory_feed(
//                             &public_key,
//                             proxy,
//                             encryption_key.is_some(),
//                             encryption_key,
//                         )
//                         .await;
//                         vacant.insert(Arc::new(Mutex::new(feed)));
//                     }
//                 }
//                 entry_found = true;
//             } else {
//                 debug!("Concurrent access to feeds noticed, yielding and retrying.");
//                 yield_now().await;
//             }
//         }
//     }
// }

//////////////////////////////////////////////////////
//
// Utilities
