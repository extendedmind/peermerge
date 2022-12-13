use automerge::Patch;

#[derive(Clone, Debug)]
pub enum StateEvent {
    PeersSynced(usize),
    RemotePeerSynced(),
    DocumentChanged(Vec<Patch>),
}

#[derive(Clone, Debug)]
pub enum SynchronizeEvent {
    NewPeersBroadcasted(usize),
    PeersSynced(usize),
    RemotePeerSynced(),
    DocumentChanged(Vec<Patch>),
}

#[derive(Clone, Debug)]
pub enum PeerEvent {
    NewPeersBroadcasted(Vec<[u8; 32]>),
    PeerSyncStarted([u8; 32]),
    PeerSynced(([u8; 32], u64)),
    PeerDisconnected(u64),
    RemotePeerSynced([u8; 32]),
}
