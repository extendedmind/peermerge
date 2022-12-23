use automerge::Patch;

#[derive(Clone, Debug)]
pub enum StateEvent {
    PeerSynced((String, [u8; 32], u64)),
    RemotePeerSynced(([u8; 32], u64)),
    DocumentChanged(Vec<Patch>),
}

#[derive(Clone, Debug)]
pub enum PeerEvent {
    NewPeersBroadcasted(Vec<[u8; 32]>),
    PeerSynced(([u8; 32], u64)),
    PeerDisconnected(u64),
    RemotePeerSynced(([u8; 32], u64)),
}
