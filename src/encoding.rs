//! Binary encoding of needed hypermerge persistent data
use hypercore_protocol::hypercore::compact_encoding::{CompactEncoding, State};
use std::convert::TryInto;

/// A RepoState stores serialized information about the Repo.
#[derive(Debug)]
pub(super) struct RepoState {
    pub(crate) version: u8,
    pub(crate) doc_discovery_keys: Vec<[u8; 32]>,
}
impl Default for RepoState {
    fn default() -> Self {
        Self {
            version: 1,
            doc_discovery_keys: Vec::new(),
        }
    }
}

impl CompactEncoding<RepoState> for State {
    fn preencode(&mut self, value: &RepoState) {
        self.preencode(&value.version);
        preencode_fixed_32_byte_vec(self, &value.doc_discovery_keys);
    }

    fn encode(&mut self, value: &RepoState, buffer: &mut [u8]) {
        self.encode(&value.version, buffer);
        encode_fixed_32_byte_vec(self, &value.doc_discovery_keys, buffer);
    }

    fn decode(&mut self, buffer: &[u8]) -> RepoState {
        let version: u8 = self.decode(buffer);
        let doc_discovery_keys = decode_fixed_32_byte_vec(self, buffer);
        RepoState {
            version,
            doc_discovery_keys,
        }
    }
}

/// A DocState stores serialized information about a single document.
#[derive(Debug)]
pub(super) struct DocState {
    pub(crate) version: u8,
    pub(crate) peer_discovery_keys: Vec<[u8; 32]>,
}
impl Default for DocState {
    fn default() -> Self {
        Self {
            version: 1,
            peer_discovery_keys: Vec::new(),
        }
    }
}

impl CompactEncoding<DocState> for State {
    fn preencode(&mut self, value: &DocState) {
        self.preencode(&value.version);
        preencode_fixed_32_byte_vec(self, &value.peer_discovery_keys);
    }

    fn encode(&mut self, value: &DocState, buffer: &mut [u8]) {
        self.encode(&value.version, buffer);
        encode_fixed_32_byte_vec(self, &value.peer_discovery_keys, buffer);
    }

    fn decode(&mut self, buffer: &[u8]) -> DocState {
        let version: u8 = self.decode(buffer);
        let peer_discovery_keys = decode_fixed_32_byte_vec(self, buffer);
        DocState {
            version,
            peer_discovery_keys,
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
