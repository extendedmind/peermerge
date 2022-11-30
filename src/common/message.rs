/// An BroadcastMessage transmits all of the active public keys the peer knows to the other peer
#[derive(Debug)]
pub(crate) struct BroadcastMessage {
    pub(crate) public_key: Option<[u8; 32]>,
    pub(crate) peer_public_keys: Vec<[u8; 32]>,
}
impl BroadcastMessage {
    pub fn new(public_key: Option<[u8; 32]>, peer_public_keys: Vec<[u8; 32]>) -> Self {
        Self {
            public_key,
            peer_public_keys,
        }
    }
}
