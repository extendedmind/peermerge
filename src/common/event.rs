use automerge::Automerge;

#[derive(Clone, Debug)]
pub enum StateEvent {
    DocumentLoaded(Automerge),
}

#[derive(Clone, Debug)]
pub enum SynchronizeEvent {
    NewPeersAdvertised(usize),
    DocumentCreated(),
}

#[derive(Clone, Debug)]
pub enum PeerEvent {
    NewPeersAdvertised(Vec<[u8; 32]>),
    PeersSynced(Vec<[u8; 32]>),
    PeerDisconnected(u64),
}
