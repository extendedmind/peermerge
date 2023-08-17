pub(crate) const PEERMERGE_VERSION: u8 = 1;

/// Use 16 the default max batch size for a verification. This multiplied with
/// MAX_ENTRY_DATA_SIZE_BYTES gets 16 MiB which is roughly the maximum
/// amount of data a malicious-turned peer can upload to an unsuspecting peer
/// before we notice it and delete the data.
pub const DEFAULT_MAX_FEEDS_VERIFIED_BATCH_SIZE: usize = 16;

/// Use 1 MiB as the absolute max limit for a single data chunk in a feed.
pub const MAX_ENTRY_DATA_SIZE_BYTES: usize = 1048576;

/// Use the max size of 1 MiB also as the default value for a single data chunk
/// in a feed.
pub const DEFAULT_MAX_ENTRY_DATA_SIZE_BYTES: usize = MAX_ENTRY_DATA_SIZE_BYTES;

/// Use 2^8 as the default maximum length of a write feed. After
/// this length is reach, a new write feed created which replaces the old.
pub const DEFAULT_MAX_WRITE_FEED_LENGTH: u64 = 256;
