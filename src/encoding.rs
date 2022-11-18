//! Binary encoding of needed hypermerge persistent data
use hypercore_protocol::hypercore::compact_encoding::{CompactEncoding, State};
use std::convert::TryInto;

/// A RepoState stores serialized information about the Repo.
#[derive(Debug)]
pub(super) struct RepoState {
    pub(crate) version: u8,
    pub(crate) doc_ids: Vec<[u8; 32]>,
}
impl Default for RepoState {
    fn default() -> Self {
        Self {
            version: 1,
            doc_ids: Vec::new(),
        }
    }
}

impl CompactEncoding<RepoState> for State {
    fn preencode(&mut self, value: &RepoState) {
        self.preencode(&value.version);
        preencode_fixed_32_byte_vec(self, &value.doc_ids);
    }

    fn encode(&mut self, value: &RepoState, buffer: &mut [u8]) {
        self.encode(&value.version, buffer);
        encode_fixed_32_byte_vec(self, &value.doc_ids, buffer);
    }

    fn decode(&mut self, buffer: &[u8]) -> RepoState {
        let version: u8 = self.decode(buffer);
        let doc_ids = decode_fixed_32_byte_vec(self, buffer);
        RepoState { version, doc_ids }
    }
}

/// A DocState stores serialized information about a single document.
#[derive(Debug)]
pub(super) struct DocState {
    pub(crate) version: u8,
    pub(crate) peer_ids: Vec<[u8; 32]>,
}
impl Default for DocState {
    fn default() -> Self {
        Self {
            version: 1,
            peer_ids: Vec::new(),
        }
    }
}

impl CompactEncoding<DocState> for State {
    fn preencode(&mut self, value: &DocState) {
        self.preencode(&value.version);
        preencode_fixed_32_byte_vec(self, &value.peer_ids);
    }

    fn encode(&mut self, value: &DocState, buffer: &mut [u8]) {
        self.encode(&value.version, buffer);
        encode_fixed_32_byte_vec(self, &value.peer_ids, buffer);
    }

    fn decode(&mut self, buffer: &[u8]) -> DocState {
        let version: u8 = self.decode(buffer);
        let peer_ids = decode_fixed_32_byte_vec(self, buffer);
        DocState { version, peer_ids }
    }
}

fn preencode_fixed_32_byte_vec(state: &mut State, value: &Vec<[u8; 32]>) {
    let len = value.len();
    state.preencode(&len);
    state.end += len * 32;
}

fn encode_fixed_32_byte_vec(state: &mut State, value: &Vec<[u8; 32]>, buffer: &mut [u8]) {
    state.encode(&value.len(), buffer);
    for entry in value {
        buffer[state.start..state.start + 32].copy_from_slice(entry);
        state.start += 32;
    }
}

fn decode_fixed_32_byte_vec(state: &mut State, buffer: &[u8]) -> Vec<[u8; 32]> {
    let len: usize = state.decode(buffer);
    let mut doc_ids: Vec<[u8; 32]> = Vec::with_capacity(len);
    for _ in 0..len {
        doc_ids.push(buffer[state.start..state.start + 32].try_into().unwrap());
        state.start += 32;
    }
    doc_ids
}
