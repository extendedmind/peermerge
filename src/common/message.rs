/// An BroadcastMessage transmits all of the active public keys the peer knows to the other peer
#[derive(Debug)]
pub(crate) struct BroadcastMessage {
    pub(crate) public_keys: Vec<[u8; 32]>,
}
impl BroadcastMessage {
    pub fn new(public_keys: Vec<[u8; 32]>) -> Self {
        Self { public_keys }
    }
}
