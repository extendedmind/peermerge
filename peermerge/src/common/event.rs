use automerge::Patch;

#[derive(Clone, Debug)]
pub enum StateEvent {
    PeerSynced(([u8; 32], Option<String>, [u8; 32], u64)),
    RemotePeerSynced(([u8; 32], [u8; 32], u64)),
    DocumentChanged(([u8; 32], Vec<Patch>)),
}

#[derive(Clone, Debug)]
pub enum PeerEvent {
    NewPeersBroadcasted(([u8; 32], Vec<[u8; 32]>)),
    PeerSynced(([u8; 32], [u8; 32], u64)),
    PeerDisconnected(([u8; 32], u64)),
    RemotePeerSynced(([u8; 32], [u8; 32], u64)),
}
