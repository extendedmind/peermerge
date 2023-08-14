use crate::feed::FeedDiscoveryKey;

use super::state::DocumentFeedInfo;

/// An BroadcastMessage transmits all of the active public keys the peer knows to the other peer
#[derive(Debug)]
pub(crate) struct BroadcastMessage {
    pub(crate) write_feed: Option<DocumentFeedInfo>,
    pub(crate) other_feeds: Vec<DocumentFeedInfo>,
}
impl BroadcastMessage {
    pub(crate) fn new(
        write_feed: Option<DocumentFeedInfo>,
        other_feeds: Vec<DocumentFeedInfo>,
    ) -> Self {
        Self {
            write_feed,
            other_feeds,
        }
    }
}

/// An NewPeersCreatedMessage is an internal message that contains all of the
/// ids and public keys of created and changed peer hypercores.
#[derive(Debug)]
pub(crate) struct FeedsChangedMessage {
    pub(crate) doc_discovery_key: FeedDiscoveryKey,
    pub(crate) replaced_feeds: Vec<DocumentFeedInfo>,
    pub(crate) feeds_to_create: Vec<DocumentFeedInfo>,
}
impl FeedsChangedMessage {
    pub(crate) fn new(
        doc_discovery_key: FeedDiscoveryKey,
        replaced_feeds: Vec<DocumentFeedInfo>,
        feeds_to_create: Vec<DocumentFeedInfo>,
    ) -> Self {
        Self {
            doc_discovery_key,
            replaced_feeds,
            feeds_to_create,
        }
    }
}

/// An FeedSyncedMessage is an internal message that contains new length
/// of a hypercore for inter-protocol signaling.
#[derive(Debug)]
pub(crate) struct FeedSyncedMessage {
    pub(crate) contiguous_length: u64,
}
impl FeedSyncedMessage {
    pub(crate) fn new(contiguous_length: u64) -> Self {
        Self { contiguous_length }
    }
}
