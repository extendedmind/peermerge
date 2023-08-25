use crate::{feed::FeedDiscoveryKey, PeerId};

use super::state::DocumentFeedInfo;

/// An BroadcastMessage transmits info of all of the feeds the peer knows
/// to the other peer.
#[derive(Debug)]
pub(crate) struct BroadcastMessage {
    pub(crate) write_feed: Option<DocumentFeedInfo>,
    pub(crate) active_feeds: Vec<DocumentFeedInfo>,
    pub(crate) inactive_feeds: Option<Vec<DocumentFeedInfo>>,
}
impl BroadcastMessage {
    pub(crate) fn new(
        write_feed: Option<DocumentFeedInfo>,
        active_feeds: Vec<DocumentFeedInfo>,
        inactive_feeds: Option<Vec<DocumentFeedInfo>>,
    ) -> Self {
        Self {
            write_feed,
            active_feeds,
            inactive_feeds,
        }
    }
}

/// An FeedsChangedMessage is an internal message that contains all of the
/// ids and public keys of created and replaced peer hypercores.
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

/// A FeedVerificationMessage is an internal message that contains
/// the verification status of a feed.
#[derive(Debug)]
pub(crate) struct FeedVerificationMessage {
    pub(crate) doc_discovery_key: FeedDiscoveryKey,
    pub(crate) feed_discovery_key: FeedDiscoveryKey,
    pub(crate) verified: bool,
    pub(crate) peer_id: Option<PeerId>,
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
