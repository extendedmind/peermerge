use automerge::Patch;

#[derive(Clone, Debug)]
pub enum StateEvent {
    PeersSynced(usize),
    DocumentChanged(Vec<Patch>),
}

#[derive(Clone, Debug)]
pub enum SynchronizeEvent {
    NewPeersAdvertised(usize),
    PeersSynced(usize),
    DocumentChanged(Vec<Patch>),
}

#[derive(Clone, Debug)]
pub enum PeerEvent {
    NewPeersAdvertised(Vec<[u8; 32]>),
    PeerSyncStarted([u8; 32]),
    PeerSynced([u8; 32]),
    PeerDisconnected(u64),
    RemotePeerSynced([u8; 32]),
}
