//! Binary encoding of needed peermerge persistent data
use compact_encoding::{CompactEncoding, EncodingError, State};
use std::convert::TryInto;

pub(crate) use crate::common::entry::Entry;
pub(crate) use crate::common::message::BroadcastMessage;
pub(crate) use crate::common::state::{DocumentState, PeermergeState};

use super::message::{NewPeersCreatedMessage, PeerSyncedMessage};
use super::state::{DocumentContent, DocumentCursor, DocumentPeerState};
use crate::NameDescription;

impl CompactEncoding<PeermergeState> for State {
    fn preencode(&mut self, value: &PeermergeState) -> Result<usize, EncodingError> {
        self.preencode(&value.version)?;
        self.preencode(&value.peer_header)?;
        self.preencode(&value.document_ids)
    }

    fn encode(
        &mut self,
        value: &PeermergeState,
        buffer: &mut [u8],
    ) -> Result<usize, EncodingError> {
        self.encode(&value.version, buffer)?;
        self.encode(&value.peer_header, buffer)?;
        self.encode(&value.document_ids, buffer)
    }

    fn decode(&mut self, buffer: &[u8]) -> Result<PeermergeState, EncodingError> {
        let version: u8 = self.decode(buffer)?;
        let peer_header: NameDescription = self.decode(buffer)?;
        let document_ids: Vec<[u8; 32]> = self.decode(buffer)?;
        Ok(PeermergeState::new_with_version(
            version,
            peer_header,
            document_ids,
        ))
    }
}

impl CompactEncoding<DocumentState> for State {
    fn preencode(&mut self, value: &DocumentState) -> Result<usize, EncodingError> {
        self.preencode(&value.version)?;
        self.preencode(&value.plain_doc_url)?;
        self.add_end(1)?; // flags
        let len = value.peers.len();
        if len > 0 {
            self.preencode(&len)?;
            for peer in &value.peers {
                self.preencode(peer)?;
            }
        }
        if value.write_public_key.is_some() {
            self.add_end(32)?;
        }
        if let Some(content) = &value.content {
            self.preencode(content)?;
        }
        Ok(self.end())
    }

    fn encode(&mut self, value: &DocumentState, buffer: &mut [u8]) -> Result<usize, EncodingError> {
        self.encode(&value.version, buffer)?;
        self.encode(&value.plain_doc_url, buffer)?;
        let flags_index = self.start();
        let mut flags: u8 = 0;
        self.add_start(1)?;
        if value.proxy {
            flags |= 1;
        }
        if value.encrypted.unwrap_or(false) {
            flags |= 2;
        }
        let len = value.peers.len();
        if len > 0 {
            flags |= 4;
            self.encode(&len, buffer)?;
            for peer in &value.peers {
                self.encode(peer, buffer)?;
            }
        }
        if let Some(write_public_key) = &value.write_public_key {
            flags |= 8;
            self.encode_fixed_32(write_public_key, buffer)?;
        }
        if let Some(content) = &value.content {
            flags |= 16;
            self.encode(content, buffer)?;
        }

        buffer[flags_index] = flags;
        Ok(self.start())
    }

    fn decode(&mut self, buffer: &[u8]) -> Result<DocumentState, EncodingError> {
        let version: u8 = self.decode(buffer)?;
        let plain_doc_url: String = self.decode(buffer)?;
        let flags: u8 = self.decode(buffer)?;
        let proxy = flags & 1 != 0;
        let encrypted: Option<bool> = if !proxy { Some(flags & 2 != 0) } else { None };
        let peers: Vec<DocumentPeerState> = if flags & 4 != 0 {
            let len: usize = self.decode(buffer)?;
            let mut peers: Vec<DocumentPeerState> = Vec::with_capacity(len);
            for _ in 0..len {
                let peer: DocumentPeerState = self.decode(buffer)?;
                peers.push(peer);
            }
            peers
        } else {
            vec![]
        };

        let write_public_key: Option<[u8; 32]> = if flags & 8 != 0 {
            Some(self.decode_fixed_32(buffer)?.to_vec().try_into().unwrap())
        } else {
            None
        };

        let content: Option<DocumentContent> = if flags & 16 != 0 {
            Some(self.decode(buffer)?)
        } else {
            None
        };
        Ok(DocumentState::new_from_plain_doc_url(
            version,
            plain_doc_url,
            proxy,
            encrypted,
            peers,
            write_public_key,
            content,
        ))
    }
}

impl CompactEncoding<DocumentPeerState> for State {
    fn preencode(&mut self, value: &DocumentPeerState) -> Result<usize, EncodingError> {
        self.add_end(32 + 1)?; // flags
        if let Some(peer_header) = &value.peer_header {
            self.preencode(peer_header)?;
        }
        Ok(self.end())
    }

    fn encode(
        &mut self,
        value: &DocumentPeerState,
        buffer: &mut [u8],
    ) -> Result<usize, EncodingError> {
        self.encode_fixed_32(&value.public_key, buffer)?;
        let flags_index = self.start();
        let mut flags: u8 = 0;
        self.add_start(1)?;
        if let Some(peer_header) = &value.peer_header {
            flags |= 1;
            self.encode(peer_header, buffer)?;
        }
        buffer[flags_index] = flags;
        Ok(self.start())
    }

    fn decode(&mut self, buffer: &[u8]) -> Result<DocumentPeerState, EncodingError> {
        let public_key: [u8; 32] = self.decode_fixed_32(buffer)?.to_vec().try_into().unwrap();
        let flags: u8 = self.decode(buffer)?;
        let peer_header: Option<NameDescription> = if flags & 1 != 0 {
            Some(self.decode(buffer)?)
        } else {
            None
        };
        Ok(DocumentPeerState::new(public_key, peer_header))
    }
}

impl CompactEncoding<DocumentContent> for State {
    fn preencode(&mut self, value: &DocumentContent) -> Result<usize, EncodingError> {
        self.preencode(&value.data)?;
        let len = value.cursors.len();
        self.preencode(&len)?;
        for cursor in &value.cursors {
            self.preencode(cursor)?;
        }
        Ok(self.end())
    }

    fn encode(
        &mut self,
        value: &DocumentContent,
        buffer: &mut [u8],
    ) -> Result<usize, EncodingError> {
        self.encode(&value.data, buffer)?;
        let len = value.cursors.len();
        self.encode(&len, buffer)?;
        for cursor in &value.cursors {
            self.encode(cursor, buffer)?;
        }
        Ok(self.start())
    }

    fn decode(&mut self, buffer: &[u8]) -> Result<DocumentContent, EncodingError> {
        let data: Vec<u8> = self.decode(buffer)?;
        let len: usize = self.decode(buffer)?;
        let mut cursors: Vec<DocumentCursor> = Vec::with_capacity(len);
        for _ in 0..len {
            let cursor: DocumentCursor = self.decode(buffer)?;
            cursors.push(cursor);
        }
        Ok(DocumentContent {
            data,
            cursors,
            automerge_doc: None,
        })
    }
}

impl CompactEncoding<DocumentCursor> for State {
    fn preencode(&mut self, value: &DocumentCursor) -> Result<usize, EncodingError> {
        self.add_end(32)?;
        self.preencode(&value.length)
    }

    fn encode(
        &mut self,
        value: &DocumentCursor,
        buffer: &mut [u8],
    ) -> Result<usize, EncodingError> {
        self.encode_fixed_32(&value.discovery_key, buffer)?;
        self.encode(&value.length, buffer)
    }

    fn decode(&mut self, buffer: &[u8]) -> Result<DocumentCursor, EncodingError> {
        let discovery_key: [u8; 32] = self.decode_fixed_32(buffer)?.to_vec().try_into().unwrap();
        let length: u64 = self.decode(buffer)?;
        Ok(DocumentCursor {
            discovery_key,
            length,
        })
    }
}

impl CompactEncoding<BroadcastMessage> for State {
    fn preencode(&mut self, value: &BroadcastMessage) -> Result<usize, EncodingError> {
        self.add_end(1)?; // flags
        if value.write_public_key.is_some() {
            self.add_end(32)?;
        }
        let len = value.peer_public_keys.len();
        if len > 0 {
            self.preencode(&value.peer_public_keys)?;
        }
        Ok(self.end())
    }

    fn encode(
        &mut self,
        value: &BroadcastMessage,
        buffer: &mut [u8],
    ) -> Result<usize, EncodingError> {
        let flags_index = self.start();
        let mut flags: u8 = 0;
        self.add_start(1)?;
        if let Some(public_key) = &value.write_public_key {
            flags |= 1;
            self.encode_fixed_32(public_key, buffer)?;
        }
        let len = value.peer_public_keys.len();
        if len > 0 {
            flags |= 2;
            self.encode(&value.peer_public_keys, buffer)?;
        }
        buffer[flags_index] = flags;
        Ok(self.start())
    }

    fn decode(&mut self, buffer: &[u8]) -> Result<BroadcastMessage, EncodingError> {
        let flags: u8 = self.decode(buffer)?;
        let write_public_key: Option<[u8; 32]> = if flags & 1 != 0 {
            Some(self.decode_fixed_32(buffer)?.to_vec().try_into().unwrap())
        } else {
            None
        };
        let peer_public_keys: Vec<[u8; 32]> = if flags & 2 != 0 {
            self.decode(buffer)?
        } else {
            vec![]
        };
        Ok(BroadcastMessage::new(write_public_key, peer_public_keys))
    }
}

impl CompactEncoding<NewPeersCreatedMessage> for State {
    fn preencode(&mut self, value: &NewPeersCreatedMessage) -> Result<usize, EncodingError> {
        self.add_end(32)?;
        self.preencode(&value.public_keys)
    }

    fn encode(
        &mut self,
        value: &NewPeersCreatedMessage,
        buffer: &mut [u8],
    ) -> Result<usize, EncodingError> {
        self.encode_fixed_32(&value.doc_discovery_key, buffer)?;
        self.encode(&value.public_keys, buffer)
    }

    fn decode(&mut self, buffer: &[u8]) -> Result<NewPeersCreatedMessage, EncodingError> {
        let doc_discovery_key: [u8; 32] =
            self.decode_fixed_32(buffer)?.to_vec().try_into().unwrap();
        let public_keys: Vec<[u8; 32]> = self.decode(buffer)?;
        Ok(NewPeersCreatedMessage::new(doc_discovery_key, public_keys))
    }
}

impl CompactEncoding<PeerSyncedMessage> for State {
    fn preencode(&mut self, value: &PeerSyncedMessage) -> Result<usize, EncodingError> {
        self.preencode(&value.contiguous_length)
    }

    fn encode(
        &mut self,
        value: &PeerSyncedMessage,
        buffer: &mut [u8],
    ) -> Result<usize, EncodingError> {
        self.encode(&value.contiguous_length, buffer)
    }

    fn decode(&mut self, buffer: &[u8]) -> Result<PeerSyncedMessage, EncodingError> {
        let contiguous_length: u64 = self.decode(buffer)?;
        Ok(PeerSyncedMessage { contiguous_length })
    }
}

impl CompactEncoding<Entry> for State {
    fn preencode(&mut self, value: &Entry) -> Result<usize, EncodingError> {
        self.preencode(&value.version)?;
        self.preencode(&(value.entry_type as u8))?;
        self.add_end(1)?; // flags
        if !value.data.is_empty() {
            self.preencode(&value.data)?;
        }
        if let Some(name) = &value.name {
            self.preencode(name)?;
        }
        if let Some(peer_name) = &value.description {
            self.preencode(peer_name)?;
        }
        Ok(self.end())
    }

    fn encode(&mut self, value: &Entry, buffer: &mut [u8]) -> Result<usize, EncodingError> {
        self.encode(&value.version, buffer)?;
        self.encode(&(value.entry_type as u8), buffer)?;

        let flags_index = self.start();
        let mut flags: u8 = 0;
        self.add_start(1)?;
        if !value.data.is_empty() {
            flags |= 1;
            self.encode(&value.data, buffer)?;
        }
        if let Some(name) = &value.name {
            flags |= 2;
            self.encode(name, buffer)?;
        }
        if let Some(peer_name) = &value.description {
            flags |= 4;
            self.encode(peer_name, buffer)?;
        }
        buffer[flags_index] = flags;
        Ok(self.start())
    }

    fn decode(&mut self, buffer: &[u8]) -> Result<Entry, EncodingError> {
        let version: u8 = self.decode(buffer)?;
        let entry_type: u8 = self.decode(buffer)?;
        let flags: u8 = self.decode(buffer)?;

        let data: Vec<u8> = if flags & 1 != 0 {
            self.decode(buffer)?
        } else {
            vec![]
        };

        let name: Option<String> = if flags & 2 != 0 {
            Some(self.decode(buffer)?)
        } else {
            None
        };

        let description: Option<String> = if flags & 4 != 0 {
            Some(self.decode(buffer)?)
        } else {
            None
        };
        Ok(Entry::new(
            version,
            entry_type.try_into().unwrap(),
            name,
            description,
            data,
        ))
    }
}

impl CompactEncoding<NameDescription> for State {
    fn preencode(&mut self, value: &NameDescription) -> Result<usize, EncodingError> {
        self.preencode(&value.name)?;
        if let Some(description) = &value.description {
            self.preencode(description)?;
        } else {
            self.preencode(&"".to_string())?;
        }
        Ok(self.end())
    }

    fn encode(
        &mut self,
        value: &NameDescription,
        buffer: &mut [u8],
    ) -> Result<usize, EncodingError> {
        self.encode(&value.name, buffer)?;
        if let Some(description) = &value.description {
            self.encode(description, buffer)?;
        } else {
            self.encode(&"".to_string(), buffer)?;
        }
        Ok(self.start())
    }

    fn decode(&mut self, buffer: &[u8]) -> Result<NameDescription, EncodingError> {
        let name: String = self.decode(buffer)?;
        let description: String = self.decode(buffer)?;
        let description: Option<String> = if description.is_empty() {
            None
        } else {
            Some(description)
        };
        Ok(NameDescription { name, description })
    }
}

pub(crate) fn serialize_entry(entry: &Entry) -> Result<Vec<u8>, EncodingError> {
    let mut enc_state = State::new();
    enc_state.preencode(entry)?;
    let mut buffer = enc_state.create_buffer();
    enc_state.encode(entry, &mut buffer)?;
    Ok(buffer.to_vec())
}
