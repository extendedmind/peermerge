/// An AdvertiseMessage transmits all of the active public keys the peer knows to the other peer
#[derive(Debug)]
pub(crate) struct AdvertiseMessage {
    pub(crate) public_keys: Vec<[u8; 32]>,
}
impl AdvertiseMessage {
    pub fn new(public_keys: Vec<[u8; 32]>) -> Self {
        Self { public_keys }
    }
}
