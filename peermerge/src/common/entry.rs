use std::convert::TryFrom;

use automerge::Change;

use crate::NameDescription;

use super::constants::PEERMERGE_VERSION;

/// Type of entry stored to a hypercore.
#[derive(Copy, Clone, Debug, PartialEq)]
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

/// A document is stored in pieces to hypercores.
#[derive(Debug, Clone)]
pub(crate) struct Entry {
    pub(crate) version: u8,
    pub(crate) entry_type: EntryType,
    pub(crate) name: Option<String>,
    pub(crate) description: Option<String>,
    pub(crate) data: Vec<u8>,
    pub(crate) change: Option<Change>,
}
impl Entry {
    pub(crate) fn new_init_doc(document_header: NameDescription, data: Vec<u8>) -> Self {
        Self::new(
            PEERMERGE_VERSION,
            EntryType::InitDoc,
            Some(document_header.name),
            document_header.description,
            data,
        )
    }

    pub(crate) fn new_init_peer(
        peer_header: NameDescription,
        root_discovery_key: [u8; 32],
    ) -> Self {
        Self::new(
            PEERMERGE_VERSION,
            EntryType::InitPeer,
            Some(peer_header.name),
            peer_header.description,
            root_discovery_key.to_vec(),
        )
    }

    pub(crate) fn new_change(mut change: Change) -> Self {
        let data = change.bytes().to_vec();
        Self {
            version: PEERMERGE_VERSION,
            entry_type: EntryType::Change,
            name: None,
            description: None,
            data,
            change: Some(change),
        }
    }

    pub(crate) fn new(
        version: u8,
        entry_type: EntryType,
        name: Option<String>,
        description: Option<String>,
        data: Vec<u8>,
    ) -> Self {
        let change: Option<Change> = if entry_type == EntryType::Change {
            Some(Change::from_bytes(data.clone()).unwrap())
        } else {
            None
        };
        Self {
            version,
            entry_type,
            name,
            description,
            data,
            change,
        }
    }
}
