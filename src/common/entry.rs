use std::convert::TryFrom;

/// A document is stored in entries to a log. Wraps automerge data types.
#[derive(Copy, Clone, Debug)]
#[repr(u8)]
pub(crate) enum EntryType {
    InitDoc = 0,
    InitPeer = 1,
    Change = 2,
}

impl TryFrom<u8> for EntryType {
    type Error = ();
    fn try_from(input: u8) -> Result<Self, <Self as TryFrom<u8>>::Error> {
        match input {
            0u8 => Ok(Self::InitDoc),
            1u8 => Ok(Self::InitPeer),
            2u8 => Ok(Self::Change),
            _ => Err(()),
        }
    }
}

#[derive(Debug)]
pub(crate) struct Entry {
    pub(crate) version: u8,
    pub(crate) entry_type: EntryType,
    pub(crate) data: Vec<u8>,
}
impl Entry {
    pub fn new_init_doc(data: Vec<u8>) -> Self {
        Self {
            version: 1,
            entry_type: EntryType::InitDoc,
            data,
        }
    }
    pub fn new_init_peer(discovery_key: [u8; 32]) -> Self {
        Self {
            version: 1,
            entry_type: EntryType::InitPeer,
            data: discovery_key.to_vec(),
        }
    }
}
