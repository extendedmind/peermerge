use std::path::PathBuf;

use futures::channel::mpsc::UnboundedSender;

use crate::{NameDescription, StateEvent};

#[derive(Builder)]
pub struct MemoryPeermergeOptions {
    pub default_peer_header: NameDescription,
    #[builder(setter(into, strip_option), default)]
    pub state_event_sender: Option<UnboundedSender<StateEvent>>,
}

#[derive(Builder)]
pub struct DiskPeermergeOptions {
    pub default_peer_header: NameDescription,
    #[builder(setter(into, strip_option), default)]
    pub state_event_sender: Option<UnboundedSender<StateEvent>>,
    pub data_root_dir: PathBuf,
}
