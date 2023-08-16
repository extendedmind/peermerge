pub(crate) const PEERMERGE_VERSION: u8 = 1;

/// Use 1 MiB as the default limit for a single data chunk in a feed
pub const DEFAULT_MAX_ENTRY_DATA_SIZE_BYTES: usize = 1048576;

/// Use 2^8 as the default maximum length of a write feed
pub const DEFAULT_MAX_WRITE_FEED_LENGTH: u64 = 256;
