//! Binary encoding of needed peermerge persistent data
use hypercore_protocol::hypercore::compact_encoding::{CompactEncoding, State};
use std::convert::TryInto;

pub(crate) use crate::common::entry::Entry;
pub(crate) use crate::common::message::BroadcastMessage;
pub(crate) use crate::common::state::{DocumentState, RepositoryState};

use super::message::{NewPeersCreatedMessage, PeerSyncedMessage};
use super::state::{DocumentContent, DocumentCursor, DocumentPeerState};

impl CompactEncoding<RepositoryState> for State {
    fn preencode(&mut self, value: &RepositoryState) {
        self.preencode(&value.version);
        self.preencode(&value.name);
        preencode_fixed_32_byte_vec(self, &value.document_ids);
    }

    fn encode(&mut self, value: &RepositoryState, buffer: &mut [u8]) {
        self.encode(&value.version, buffer);
        self.encode(&value.name, buffer);
        encode_fixed_32_byte_vec(self, &value.document_ids, buffer);
    }

    fn decode(&mut self, buffer: &[u8]) -> RepositoryState {
        let version: u8 = self.decode(buffer);
        let name: String = self.decode(buffer);
        let document_ids = decode_fixed_32_byte_vec(self, buffer);
        RepositoryState::new_with_version(version, name, document_ids)
    }
}

impl CompactEncoding<DocumentState> for State {
    fn preencode(&mut self, value: &DocumentState) {
        self.preencode(&value.version);
        self.preencode(&value.doc_url);
        self.preencode(&value.name);
        self.end += 1; // flags
        let len = value.peers.len();
        if len > 0 {
            self.preencode(&len);
            for peer in &value.peers {
                self.preencode(peer);
            }
        }
        if value.write_public_key.is_some() {
            self.end += 32;
        }
        if let Some(content) = &value.content {
            self.preencode(content);
        }
    }

    fn encode(&mut self, value: &DocumentState, buffer: &mut [u8]) {
        self.encode(&value.version, buffer);
        self.encode(&value.doc_url, buffer);
        self.encode(&value.name, buffer);
        let flags_index = self.start;
        let mut flags: u8 = 0;
        self.start += 1;
        if value.proxy {
            flags = flags | 1;
        }
        let len = value.peers.len();
        if len > 0 {
            flags = flags | 2;
            self.encode(&len, buffer);
            for peer in &value.peers {
                self.encode(peer, buffer);
            }
        }
        if let Some(write_public_key) = &value.write_public_key {
            flags = flags | 4;
            self.encode_fixed_32(write_public_key, buffer);
        }
        if let Some(content) = &value.content {
            flags = flags | 8;
            self.encode(content, buffer);
        }

        buffer[flags_index] = flags;
    }

    fn decode(&mut self, buffer: &[u8]) -> DocumentState {
        let version: u8 = self.decode(buffer);
        let doc_url: String = self.decode(buffer);
        let name: String = self.decode(buffer);
        let flags: u8 = self.decode(buffer);
        let proxy = flags & 1 != 0;
        let peers: Vec<DocumentPeerState> = if flags & 2 != 0 {
            let len: usize = self.decode(buffer);
            let mut peers: Vec<DocumentPeerState> = Vec::with_capacity(len);
            for _ in 0..len {
                let peer: DocumentPeerState = self.decode(buffer);
                peers.push(peer);
            }
            peers
        } else {
            vec![]
        };

        let write_public_key: Option<[u8; 32]> = if flags & 4 != 0 {
            Some(self.decode_fixed_32(buffer).to_vec().try_into().unwrap())
        } else {
            None
        };

        let content: Option<DocumentContent> = if flags & 8 != 0 {
            Some(self.decode(buffer))
        } else {
            None
        };
        DocumentState::new_with_version(
            version,
            doc_url,
            name,
            proxy,
            peers,
            write_public_key,
            content,
        )
    }
}

impl CompactEncoding<DocumentPeerState> for State {
    fn preencode(&mut self, value: &DocumentPeerState) {
        self.end += 32;
        if let Some(name) = &value.name {
            self.preencode(name);
        }
    }

    fn encode(&mut self, value: &DocumentPeerState, buffer: &mut [u8]) {
        self.encode_fixed_32(&value.public_key, buffer);
        if let Some(name) = &value.name {
            self.encode(name, buffer);
        }
    }

    fn decode(&mut self, buffer: &[u8]) -> DocumentPeerState {
        let public_key: [u8; 32] = self.decode_fixed_32(buffer).to_vec().try_into().unwrap();
        let name: Option<String> = if self.start < self.end {
            Some(self.decode(buffer))
        } else {
            None
        };
        DocumentPeerState::new(public_key, name)
    }
}

impl CompactEncoding<DocumentContent> for State {
    fn preencode(&mut self, value: &DocumentContent) {
        self.preencode(&value.data);
        let len = value.cursors.len();
        self.preencode(&len);
        for cursor in &value.cursors {
            self.preencode(cursor);
        }
    }

    fn encode(&mut self, value: &DocumentContent, buffer: &mut [u8]) {
        self.encode(&value.data, buffer);
        let len = value.cursors.len();
        self.encode(&len, buffer);
        for cursor in &value.cursors {
            self.encode(cursor, buffer);
        }
    }

    fn decode(&mut self, buffer: &[u8]) -> DocumentContent {
        let data: Vec<u8> = self.decode(buffer);
        let len: usize = self.decode(buffer);
        let mut cursors: Vec<DocumentCursor> = Vec::with_capacity(len);
        for _ in 0..len {
            let cursor: DocumentCursor = self.decode(buffer);
            cursors.push(cursor);
        }
        DocumentContent {
            data,
            cursors,
            automerge_doc: None,
        }
    }
}

impl CompactEncoding<DocumentCursor> for State {
    fn preencode(&mut self, value: &DocumentCursor) {
        self.end += 32;
        self.preencode(&value.length);
    }

    fn encode(&mut self, value: &DocumentCursor, buffer: &mut [u8]) {
        self.encode_fixed_32(&value.discovery_key, buffer);
        self.encode(&value.length, buffer);
    }

    fn decode(&mut self, buffer: &[u8]) -> DocumentCursor {
        let discovery_key: [u8; 32] = self.decode_fixed_32(buffer).to_vec().try_into().unwrap();
        let length: u64 = self.decode(buffer);
        DocumentCursor {
            discovery_key,
            length,
        }
    }
}

impl CompactEncoding<BroadcastMessage> for State {
    fn preencode(&mut self, value: &BroadcastMessage) {
        self.end += 1; // flags
        if value.write_public_key.is_some() {
            self.end += 32;
        }
        let len = value.peer_public_keys.len();
        if len > 0 {
            preencode_fixed_32_byte_vec(self, &value.peer_public_keys);
        }
    }

    fn encode(&mut self, value: &BroadcastMessage, buffer: &mut [u8]) {
        let flags_index = self.start;
        let mut flags: u8 = 0;
        self.start += 1;
        if let Some(public_key) = &value.write_public_key {
            flags = flags | 1;
            self.encode_fixed_32(public_key, buffer);
        }
        let len = value.peer_public_keys.len();
        if len > 0 {
            flags = flags | 2;
            encode_fixed_32_byte_vec(self, &value.peer_public_keys, buffer);
        }
        buffer[flags_index] = flags;
    }

    fn decode(&mut self, buffer: &[u8]) -> BroadcastMessage {
        let flags: u8 = self.decode(buffer);
        let write_public_key: Option<[u8; 32]> = if flags & 1 != 0 {
            Some(self.decode_fixed_32(buffer).to_vec().try_into().unwrap())
        } else {
            None
        };
        let peer_public_keys: Vec<[u8; 32]> = if flags & 2 != 0 {
            decode_fixed_32_byte_vec(self, buffer)
        } else {
            vec![]
        };
        BroadcastMessage::new(write_public_key, peer_public_keys)
    }
}

impl CompactEncoding<NewPeersCreatedMessage> for State {
    fn preencode(&mut self, value: &NewPeersCreatedMessage) {
        self.end += 32;
        preencode_fixed_32_byte_vec(self, &value.public_keys);
    }

    fn encode(&mut self, value: &NewPeersCreatedMessage, buffer: &mut [u8]) {
        self.encode_fixed_32(&value.doc_discovery_key, buffer);
        encode_fixed_32_byte_vec(self, &value.public_keys, buffer);
    }

    fn decode(&mut self, buffer: &[u8]) -> NewPeersCreatedMessage {
        let doc_discovery_key: [u8; 32] = self.decode_fixed_32(buffer).to_vec().try_into().unwrap();
        let public_keys: Vec<[u8; 32]> = decode_fixed_32_byte_vec(self, buffer);
        NewPeersCreatedMessage::new(doc_discovery_key, public_keys)
    }
}

impl CompactEncoding<PeerSyncedMessage> for State {
    fn preencode(&mut self, value: &PeerSyncedMessage) {
        self.preencode(&value.contiguous_length);
    }

    fn encode(&mut self, value: &PeerSyncedMessage, buffer: &mut [u8]) {
        self.encode(&value.contiguous_length, buffer);
    }

    fn decode(&mut self, buffer: &[u8]) -> PeerSyncedMessage {
        let contiguous_length: u64 = self.decode(buffer);
        PeerSyncedMessage { contiguous_length }
    }
}

impl CompactEncoding<Entry> for State {
    fn preencode(&mut self, value: &Entry) {
        self.preencode(&value.version);
        self.preencode(&(value.entry_type as u8));
        self.preencode(&value.data);
        if let Some(peer_name) = &value.peer_name {
            self.preencode(peer_name);
        }
    }

    fn encode(&mut self, value: &Entry, buffer: &mut [u8]) {
        self.encode(&value.version, buffer);
        self.encode(&(value.entry_type as u8), buffer);
        self.encode(&value.data, buffer);
        if let Some(peer_name) = &value.peer_name {
            self.encode(peer_name, buffer);
        }
    }

    fn decode(&mut self, buffer: &[u8]) -> Entry {
        let version: u8 = self.decode(buffer);
        let entry_type: u8 = self.decode(buffer);
        let data: Vec<u8> = self.decode(buffer);
        let peer_name: Option<String> = if self.start < self.end {
            Some(self.decode(buffer))
        } else {
            None
        };
        Entry::new(version, entry_type.try_into().unwrap(), peer_name, data)
    }
}

pub(crate) fn serialize_entry(entry: &Entry) -> Vec<u8> {
    let mut enc_state = State::new();
    enc_state.preencode(entry);
    let mut buffer = enc_state.create_buffer();
    enc_state.encode(entry, &mut buffer);
    buffer.to_vec()
}

fn preencode_fixed_32_byte_vec(state: &mut State, entries: &Vec<[u8; 32]>) {
    let len = entries.len();
    state.preencode(&len);
    state.end += len * 32;
}

fn encode_fixed_32_byte_vec(state: &mut State, entries: &Vec<[u8; 32]>, buffer: &mut [u8]) {
    state.encode(&entries.len(), buffer);
    for entry in entries {
        buffer[state.start..state.start + 32].copy_from_slice(entry);
        state.start += 32;
    }
}

fn decode_fixed_32_byte_vec(state: &mut State, buffer: &[u8]) -> Vec<[u8; 32]> {
    let len: usize = state.decode(buffer);
    let mut entries: Vec<[u8; 32]> = Vec::with_capacity(len);
    for _ in 0..len {
        entries.push(buffer[state.start..state.start + 32].try_into().unwrap());
        state.start += 32;
    }
    entries
}
