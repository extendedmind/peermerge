pub(crate) const PEERMERGE_VERSION: u8 = 1;

/// Use 1 MiB as the absolute max limit for a single data chunk in a feed.
pub const MAX_ENTRY_DATA_SIZE_BYTES: usize = 1048576;

/// Use the max size of 1 MiB also as the default value for a single data chunk
/// in a feed.
pub const DEFAULT_MAX_ENTRY_DATA_SIZE_BYTES: usize = MAX_ENTRY_DATA_SIZE_BYTES;

/// Use 2^8 as the default maximum length of a write feed. After
/// this length is reach, a new write feed created which replaces the old.
pub const DEFAULT_MAX_WRITE_FEED_LENGTH: u64 = 256;
