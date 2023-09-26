use std::{collections::HashMap, path::PathBuf};

use futures::channel::mpsc::UnboundedSender;

use crate::{
    common::constants::{DEFAULT_MAX_ENTRY_DATA_SIZE_BYTES, DEFAULT_MAX_WRITE_FEED_LENGTH},
    DocumentId, NameDescription, StateEvent,
};

#[derive(Builder)]
pub struct PeermergeMemoryOptions {
    pub default_peer_header: NameDescription,
    #[builder(setter(into, strip_option), default)]
    pub state_event_sender: Option<UnboundedSender<StateEvent>>,
    #[builder(setter(into, strip_option), default)]
    pub reattach_secrets: Option<HashMap<DocumentId, String>>,
    #[builder(default = "DEFAULT_MAX_ENTRY_DATA_SIZE_BYTES")]
    pub max_entry_data_size_bytes: usize,
    #[builder(default = "DEFAULT_MAX_WRITE_FEED_LENGTH")]
    pub max_write_feed_length: u64,
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Builder)]
pub struct PeermergeDiskOptions {
    pub data_root_dir: PathBuf,
    pub default_peer_header: NameDescription,
    #[builder(setter(into, strip_option), default)]
    pub state_event_sender: Option<UnboundedSender<StateEvent>>,
    #[builder(default = "DEFAULT_MAX_ENTRY_DATA_SIZE_BYTES")]
    pub max_entry_data_size_bytes: usize,
    #[builder(default = "DEFAULT_MAX_WRITE_FEED_LENGTH")]
    pub max_write_feed_length: u64,
}

#[derive(Builder)]
pub struct CreateNewDocumentMemoryOptions {
    pub document_type: String,
    #[builder(setter(into, strip_option), default)]
    pub document_header: Option<NameDescription>,
    #[builder(setter(into, strip_option), default)]
    pub parent_id: Option<DocumentId>,
    #[builder(setter(into, strip_option), default)]
    pub parent_header: Option<NameDescription>,
    #[builder(default = "true")]
    pub encrypted: bool,
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Builder)]
pub struct CreateNewDocumentDiskOptions {
    pub document_type: String,
    #[builder(setter(into, strip_option), default)]
    pub document_header: Option<NameDescription>,
    #[builder(setter(into, strip_option), default)]
    pub parent_id: Option<DocumentId>,
    #[builder(setter(into, strip_option), default)]
    pub parent_header: Option<NameDescription>,
    #[builder(default = "true")]
    pub encrypted: bool,
}

#[derive(Builder)]
pub struct AttachDocumentMemoryOptions {
    pub document_url: String,
    #[builder(setter(into, strip_option), default)]
    pub document_secret: Option<String>,
    #[builder(setter(into, strip_option), default)]
    pub parent_id: Option<DocumentId>,
    #[builder(setter(into, strip_option), default)]
    pub parent_header: Option<NameDescription>,
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Builder)]
pub struct AttachDocumentDiskOptions {
    pub document_url: String,
    #[builder(setter(into, strip_option), default)]
    pub document_secret: Option<String>,
    #[builder(setter(into, strip_option), default)]
    pub parent_id: Option<DocumentId>,
    #[builder(setter(into, strip_option), default)]
    pub parent_header: Option<NameDescription>,
}
