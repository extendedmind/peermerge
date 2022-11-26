//! Binary encoding of needed hypermerge persistent data
use hypercore_protocol::hypercore::compact_encoding::{CompactEncoding, State};
use std::convert::TryInto;

pub(crate) use crate::common::message::AdvertiseMessage;
pub(crate) use crate::common::state::{DocState, RepoState};

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
        self.end += 32;
        preencode_fixed_32_byte_vec(self, &value.peer_public_keys);
    }

    fn encode(&mut self, value: &DocState, buffer: &mut [u8]) {
        self.encode(&value.version, buffer);
        self.encode_fixed_32(&value.public_key, buffer);
        encode_fixed_32_byte_vec(self, &value.peer_public_keys, buffer);
    }

    fn decode(&mut self, buffer: &[u8]) -> DocState {
        let version: u8 = self.decode(buffer);
        let public_key: [u8; 32] = self.decode_fixed_32(buffer).to_vec().try_into().unwrap();
        let peer_public_keys = decode_fixed_32_byte_vec(self, buffer);
        DocState {
            version,
            public_key,
            peer_public_keys,
        }
    }
}

impl CompactEncoding<AdvertiseMessage> for State {
    fn preencode(&mut self, value: &AdvertiseMessage) {
        preencode_fixed_32_byte_vec(self, &value.public_keys);
    }

    fn encode(&mut self, value: &AdvertiseMessage, buffer: &mut [u8]) {
        encode_fixed_32_byte_vec(self, &value.public_keys, buffer);
    }

    fn decode(&mut self, buffer: &[u8]) -> AdvertiseMessage {
        let public_keys = decode_fixed_32_byte_vec(self, buffer);
        AdvertiseMessage { public_keys }
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
