//! Binary encoding of needed hypermerge persistent data
use hypercore_protocol::hypercore::compact_encoding::{CompactEncoding, State};
use std::convert::TryInto;

pub(crate) use crate::common::entry::Entry;
pub(crate) use crate::common::message::BroadcastMessage;
pub(crate) use crate::common::state::{DocState, RepoState};

use super::state::{DocContent, DocCursor, DocPeerState};

impl CompactEncoding<RepoState> for State {
    fn preencode(&mut self, value: &RepoState) {
        self.preencode(&value.version);
        preencode_fixed_32_byte_vec(self, &value.doc_public_keys);
    }

    fn encode(&mut self, value: &RepoState, buffer: &mut [u8]) {
        self.encode(&value.version, buffer);
        encode_fixed_32_byte_vec(self, &value.doc_public_keys, buffer);
    }

    fn decode(&mut self, buffer: &[u8]) -> RepoState {
        let version: u8 = self.decode(buffer);
        let doc_public_keys = decode_fixed_32_byte_vec(self, buffer);
        RepoState {
            version,
            doc_public_keys,
        }
    }
}

impl CompactEncoding<DocState> for State {
    fn preencode(&mut self, value: &DocState) {
        self.preencode(&value.version);
        self.end += 1; // flags
        let len = value.peers.len();
        if len > 0 {
            self.preencode(&len);
            for peer in &value.peers {
                self.preencode(peer);
            }
        }
        if value.public_key.is_some() {
            self.end += 32;
        }
        if let Some(content) = &value.content {
            self.preencode(content);
        }
    }

    fn encode(&mut self, value: &DocState, buffer: &mut [u8]) {
        self.encode(&value.version, buffer);
        let flags_index = self.start;
        let mut flags: u8 = 0;
        self.start += 1;
        let len = value.peers.len();
        if len > 0 {
            flags = flags | 1;
            self.encode(&len, buffer);
            for peer in &value.peers {
                self.encode(peer, buffer);
            }
        }
        if let Some(public_key) = &value.public_key {
            flags = flags | 2;
            self.encode_fixed_32(public_key, buffer);
        }
        if let Some(content) = &value.content {
            flags = flags | 4;
            self.encode(content, buffer);
        }

        buffer[flags_index] = flags;
    }

    fn decode(&mut self, buffer: &[u8]) -> DocState {
        let version: u8 = self.decode(buffer);
        let flags: u8 = self.decode(buffer);
        let peers: Vec<DocPeerState> = if flags & 1 != 0 {
            let len: usize = self.decode(buffer);
            let mut peers: Vec<DocPeerState> = Vec::with_capacity(len);
            for _ in 0..len {
                let peer: DocPeerState = self.decode(buffer);
                peers.push(peer);
            }
            peers
        } else {
            vec![]
        };

        let public_key: Option<[u8; 32]> = if flags & 2 != 0 {
            Some(self.decode_fixed_32(buffer).to_vec().try_into().unwrap())
        } else {
            None
        };

        let content: Option<DocContent> = if flags & 4 != 0 {
            Some(self.decode(buffer))
        } else {
            None
        };
        DocState {
            version,
            peers,
            public_key,
            content,
            watched_root_props: vec![],
        }
    }
}

impl CompactEncoding<DocPeerState> for State {
    fn preencode(&mut self, _value: &DocPeerState) {
        self.end += 32 + 1;
    }

    fn encode(&mut self, value: &DocPeerState, buffer: &mut [u8]) {
        self.encode_fixed_32(&value.public_key, buffer);
        let synced: u8 = if value.synced { 1 } else { 0 };
        self.encode(&synced, buffer);
    }

    fn decode(&mut self, buffer: &[u8]) -> DocPeerState {
        let public_key: [u8; 32] = self.decode_fixed_32(buffer).to_vec().try_into().unwrap();
        let synced: u8 = self.decode(buffer);
        DocPeerState {
            public_key,
            synced: synced != 0,
        }
    }
}

impl CompactEncoding<DocContent> for State {
    fn preencode(&mut self, value: &DocContent) {
        self.preencode(&value.data);
        let len = value.cursors.len();
        self.preencode(&len);
        for cursor in &value.cursors {
            self.preencode(cursor);
        }
    }

    fn encode(&mut self, value: &DocContent, buffer: &mut [u8]) {
        self.encode(&value.data, buffer);
        let len = value.cursors.len();
        self.encode(&len, buffer);
        for cursor in &value.cursors {
            self.encode(cursor, buffer);
        }
    }

    fn decode(&mut self, buffer: &[u8]) -> DocContent {
        let data: Vec<u8> = self.decode(buffer);
        let len: usize = self.decode(buffer);
        let mut cursors: Vec<DocCursor> = Vec::with_capacity(len);
        for _ in 0..len {
            let cursor: DocCursor = self.decode(buffer);
            cursors.push(cursor);
        }
        DocContent {
            data,
            cursors,
            doc: None,
        }
    }
}

impl CompactEncoding<DocCursor> for State {
    fn preencode(&mut self, value: &DocCursor) {
        self.end += 32;
        self.preencode(&value.length);
    }

    fn encode(&mut self, value: &DocCursor, buffer: &mut [u8]) {
        self.encode_fixed_32(&value.discovery_key, buffer);
        self.encode(&value.length, buffer);
    }

    fn decode(&mut self, buffer: &[u8]) -> DocCursor {
        let discovery_key: [u8; 32] = self.decode_fixed_32(buffer).to_vec().try_into().unwrap();
        let length: u64 = self.decode(buffer);
        DocCursor {
            discovery_key,
            length,
        }
    }
}

impl CompactEncoding<BroadcastMessage> for State {
    fn preencode(&mut self, value: &BroadcastMessage) {
        preencode_fixed_32_byte_vec(self, &value.public_keys);
    }

    fn encode(&mut self, value: &BroadcastMessage, buffer: &mut [u8]) {
        encode_fixed_32_byte_vec(self, &value.public_keys, buffer);
    }

    fn decode(&mut self, buffer: &[u8]) -> BroadcastMessage {
        let public_keys = decode_fixed_32_byte_vec(self, buffer);
        BroadcastMessage { public_keys }
    }
}

impl CompactEncoding<Entry> for State {
    fn preencode(&mut self, value: &Entry) {
        self.preencode(&value.version);
        self.preencode(&(value.entry_type as u8));
        self.preencode(&value.data);
    }

    fn encode(&mut self, value: &Entry, buffer: &mut [u8]) {
        self.encode(&value.version, buffer);
        self.encode(&(value.entry_type as u8), buffer);
        self.encode(&value.data, buffer);
    }

    fn decode(&mut self, buffer: &[u8]) -> Entry {
        let version: u8 = self.decode(buffer);
        let entry_type: u8 = self.decode(buffer);
        let data: Vec<u8> = self.decode(buffer);
        Entry {
            version,
            entry_type: entry_type.try_into().unwrap(),
            data,
        }
    }
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
