//! Binary encoding of needed peermerge persistent data
use compact_encoding::{CompactEncoding, EncodingError, State};
use std::convert::TryInto;

pub(crate) use crate::common::entry::Entry;
pub(crate) use crate::common::message::BroadcastMessage;
pub(crate) use crate::common::state::{DocumentState, PeermergeState};
use crate::feed::FeedPublicKey;

use super::cipher::DocUrlAppendix;
use super::entry::EntryContent;
use super::message::{FeedSyncedMessage, FeedsChangedMessage};
use super::state::{DocumentContent, DocumentCursor, DocumentFeedInfo, DocumentFeedsState};
use crate::{NameDescription, PeerId};

impl CompactEncoding<PeermergeState> for State {
    fn preencode(&mut self, value: &PeermergeState) -> Result<usize, EncodingError> {
        self.preencode(&value.version)?;
        self.preencode_fixed_16()?; // peer_id
        self.preencode(&value.default_peer_header)?;
        self.preencode(&value.document_ids)
    }

    fn encode(
        &mut self,
        value: &PeermergeState,
        buffer: &mut [u8],
    ) -> Result<usize, EncodingError> {
        self.encode(&value.version, buffer)?;
        self.encode_fixed_16(&value.peer_id, buffer)?;
        self.encode(&value.default_peer_header, buffer)?;
        self.encode(&value.document_ids, buffer)
    }

    fn decode(&mut self, buffer: &[u8]) -> Result<PeermergeState, EncodingError> {
        let version: u8 = self.decode(buffer)?;
        let peer_id: PeerId = self.decode_fixed_16(buffer)?.to_vec().try_into().unwrap();
        let peer_header: NameDescription = self.decode(buffer)?;
        let document_ids: Vec<[u8; 32]> = self.decode(buffer)?;
        Ok(PeermergeState::new_with_version(
            version,
            peer_id,
            peer_header,
            document_ids,
        ))
    }
}

impl CompactEncoding<DocumentState> for State {
    fn preencode(&mut self, value: &DocumentState) -> Result<usize, EncodingError> {
        self.preencode(&value.version)?;
        self.preencode_fixed_32()?; // doc_public_key
        self.add_end(1)?; // flags
        let len = value.feeds_state.other_feeds.len();
        if len > 0 {
            self.preencode(&len)?;
            for feed in &value.feeds_state.other_feeds {
                self.preencode(feed)?;
            }
        }
        if let Some(write_feed) = &value.feeds_state.write_feed {
            self.preencode(write_feed)?;
        }
        if let Some(content) = &value.content {
            self.preencode(content)?;
        }
        Ok(self.end())
    }

    fn encode(&mut self, value: &DocumentState, buffer: &mut [u8]) -> Result<usize, EncodingError> {
        self.encode(&value.version, buffer)?;
        self.encode_fixed_32(&value.doc_public_key, buffer)?;
        let flags_index = self.start();
        let mut flags: u8 = 0;
        self.add_start(1)?;
        if value.proxy {
            flags |= 1;
        }
        if value.encrypted.unwrap_or(false) {
            flags |= 2;
        }
        let len = value.feeds_state.other_feeds.len();
        if len > 0 {
            flags |= 4;
            self.encode(&len, buffer)?;
            for feed in &value.feeds_state.other_feeds {
                self.encode(feed, buffer)?;
            }
        }
        if let Some(write_feed) = &value.feeds_state.write_feed {
            flags |= 8;
            self.encode(write_feed, buffer)?;
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
        let doc_public_key: FeedPublicKey =
            self.decode_fixed_32(buffer)?.to_vec().try_into().unwrap();
        let flags: u8 = self.decode(buffer)?;
        let proxy = flags & 1 != 0;
        let encrypted: Option<bool> = if !proxy { Some(flags & 2 != 0) } else { None };
        let other_feeds: Vec<DocumentFeedInfo> = if flags & 4 != 0 {
            let len: usize = self.decode(buffer)?;
            let mut feeds: Vec<DocumentFeedInfo> = Vec::with_capacity(len);
            for _ in 0..len {
                let feed: DocumentFeedInfo = self.decode(buffer)?;
                feeds.push(feed);
            }
            feeds
        } else {
            vec![]
        };

        let write_feed: Option<DocumentFeedInfo> = if flags & 8 != 0 {
            Some(self.decode(buffer)?)
        } else {
            None
        };

        let content: Option<DocumentContent> = if flags & 16 != 0 {
            Some(self.decode(buffer)?)
        } else {
            None
        };
        Ok(DocumentState::new_with_version(
            version,
            proxy,
            doc_public_key,
            encrypted,
            DocumentFeedsState::new_from_data(write_feed, other_feeds),
            content,
        ))
    }
}

impl CompactEncoding<DocumentFeedInfo> for State {
    fn preencode(&mut self, _value: &DocumentFeedInfo) -> Result<usize, EncodingError> {
        self.preencode_fixed_16()?; // peer_id
        self.preencode_fixed_32()?; // public_key
        self.add_end(1) // flags
    }

    fn encode(
        &mut self,
        value: &DocumentFeedInfo,
        buffer: &mut [u8],
    ) -> Result<usize, EncodingError> {
        self.encode_fixed_16(&value.peer_id, buffer)?;
        self.encode_fixed_32(&value.public_key, buffer)?;
        let flags_index = self.start();
        let mut flags: u8 = 0;
        self.add_start(1)?;
        if let Some(replaced_by_public_key) = &value.replaced_by_public_key {
            flags |= 1;
            self.encode_fixed_32(replaced_by_public_key, buffer)?;
        }
        buffer[flags_index] = flags;
        Ok(self.start())
    }

    fn decode(&mut self, buffer: &[u8]) -> Result<DocumentFeedInfo, EncodingError> {
        let id: PeerId = self.decode_fixed_16(buffer)?.to_vec().try_into().unwrap();
        let public_key: FeedPublicKey = self.decode_fixed_32(buffer)?.to_vec().try_into().unwrap();

        let flags: u8 = self.decode(buffer)?;
        let replaced_by_public_key: Option<FeedPublicKey> = if flags & 1 != 0 {
            Some(self.decode_fixed_32(buffer)?.to_vec().try_into().unwrap())
        } else {
            None
        };
        Ok(DocumentFeedInfo::new(
            id,
            public_key,
            replaced_by_public_key,
        ))
    }
}

impl CompactEncoding<DocumentContent> for State {
    fn preencode(&mut self, value: &DocumentContent) -> Result<usize, EncodingError> {
        self.add_end(1)?; // flags
        self.preencode_fixed_16()?; // peer_id
        self.preencode(&value.meta_doc_data)?;
        let len = value.cursors.len();
        if len > 0 {
            self.preencode(&len)?;
            for cursor in &value.cursors {
                self.preencode(cursor)?;
            }
        }
        if let Some(user_doc_data) = &value.user_doc_data {
            self.preencode(user_doc_data)?;
        }
        Ok(self.end())
    }

    fn encode(
        &mut self,
        value: &DocumentContent,
        buffer: &mut [u8],
    ) -> Result<usize, EncodingError> {
        let flags_index = self.start();
        let mut flags: u8 = 0;
        self.add_start(1)?;
        self.encode_fixed_16(&value.peer_id, buffer)?;
        self.encode(&value.meta_doc_data, buffer)?;
        let len = value.cursors.len();
        if len > 0 {
            flags |= 1;
            self.encode(&len, buffer)?;
            for cursor in &value.cursors {
                self.encode(cursor, buffer)?;
            }
        }
        if let Some(user_doc_data) = &value.user_doc_data {
            flags |= 2;
            self.encode(user_doc_data, buffer)?;
        }
        buffer[flags_index] = flags;
        Ok(self.start())
    }

    fn decode(&mut self, buffer: &[u8]) -> Result<DocumentContent, EncodingError> {
        let flags: u8 = self.decode(buffer)?;
        let peer_id: PeerId = self.decode_fixed_16(buffer)?.to_vec().try_into().unwrap();
        let meta_doc_data: Vec<u8> = self.decode(buffer)?;
        let cursors: Vec<DocumentCursor> = if flags & 1 != 0 {
            let len: usize = self.decode(buffer)?;
            let mut cursors: Vec<DocumentCursor> = Vec::with_capacity(len);
            for _ in 0..len {
                let cursor: DocumentCursor = self.decode(buffer)?;
                cursors.push(cursor);
            }
            cursors
        } else {
            vec![]
        };
        let user_doc_data: Option<Vec<u8>> = if flags & 2 != 0 {
            Some(self.decode(buffer)?)
        } else {
            None
        };

        Ok(DocumentContent {
            peer_id,
            cursors,
            meta_doc_data,
            user_doc_data,
            meta_automerge_doc: None,
            user_automerge_doc: None,
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
        if let Some(write_feed) = &value.write_feed {
            self.preencode(write_feed)?;
        }
        let len = value.other_feeds.len();
        if len > 0 {
            self.preencode(&len)?;
            for feed in &value.other_feeds {
                self.preencode(feed)?;
            }
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
        if let Some(write_feed) = &value.write_feed {
            flags |= 1;
            self.encode(write_feed, buffer)?;
        }
        let len = value.other_feeds.len();
        if len > 0 {
            flags |= 2;
            self.encode(&len, buffer)?;
            for feed in &value.other_feeds {
                self.encode(feed, buffer)?;
            }
        }
        buffer[flags_index] = flags;
        Ok(self.start())
    }

    fn decode(&mut self, buffer: &[u8]) -> Result<BroadcastMessage, EncodingError> {
        let flags: u8 = self.decode(buffer)?;
        let write_feed: Option<DocumentFeedInfo> = if flags & 1 != 0 {
            Some(self.decode(buffer)?)
        } else {
            None
        };
        let feeds: Vec<DocumentFeedInfo> = if flags & 2 != 0 {
            let len: usize = self.decode(buffer)?;
            let mut feeds: Vec<DocumentFeedInfo> = Vec::with_capacity(len);
            for _ in 0..len {
                let feed: DocumentFeedInfo = self.decode(buffer)?;
                feeds.push(feed);
            }
            feeds
        } else {
            vec![]
        };
        Ok(BroadcastMessage::new(write_feed, feeds))
    }
}

impl CompactEncoding<FeedsChangedMessage> for State {
    fn preencode(&mut self, value: &FeedsChangedMessage) -> Result<usize, EncodingError> {
        self.preencode_fixed_32()?;
        self.add_end(1)?; // flags
        let len = value.replaced_feeds.len();
        if len > 0 {
            self.preencode(&len)?;
            for feed in &value.replaced_feeds {
                self.preencode(feed)?;
            }
        }
        let len = value.feeds_to_create.len();
        if len > 0 {
            self.preencode(&len)?;
            for feed in &value.feeds_to_create {
                self.preencode(feed)?;
            }
        }
        Ok(self.end())
    }

    fn encode(
        &mut self,
        value: &FeedsChangedMessage,
        buffer: &mut [u8],
    ) -> Result<usize, EncodingError> {
        self.encode_fixed_32(&value.doc_discovery_key, buffer)?;
        let flags_index = self.start();
        let mut flags: u8 = 0;
        self.add_start(1)?;
        let len = value.replaced_feeds.len();
        if len > 0 {
            flags |= 1;
            self.encode(&len, buffer)?;
            for feed in &value.replaced_feeds {
                self.encode(feed, buffer)?;
            }
        }
        let len = value.feeds_to_create.len();
        if len > 0 {
            flags |= 2;
            self.encode(&len, buffer)?;
            for feed in &value.feeds_to_create {
                self.encode(feed, buffer)?;
            }
        }
        buffer[flags_index] = flags;
        Ok(self.start())
    }

    fn decode(&mut self, buffer: &[u8]) -> Result<FeedsChangedMessage, EncodingError> {
        let doc_discovery_key: [u8; 32] =
            self.decode_fixed_32(buffer)?.to_vec().try_into().unwrap();
        let flags: u8 = self.decode(buffer)?;
        let replaced_feeds: Vec<DocumentFeedInfo> = if flags & 1 != 0 {
            let len: usize = self.decode(buffer)?;
            let mut feeds: Vec<DocumentFeedInfo> = Vec::with_capacity(len);
            for _ in 0..len {
                let feed: DocumentFeedInfo = self.decode(buffer)?;
                feeds.push(feed);
            }
            feeds
        } else {
            vec![]
        };
        let feeds_to_create: Vec<DocumentFeedInfo> = if flags & 2 != 0 {
            let len: usize = self.decode(buffer)?;
            let mut feeds: Vec<DocumentFeedInfo> = Vec::with_capacity(len);
            for _ in 0..len {
                let feed: DocumentFeedInfo = self.decode(buffer)?;
                feeds.push(feed);
            }
            feeds
        } else {
            vec![]
        };

        Ok(FeedsChangedMessage::new(
            doc_discovery_key,
            replaced_feeds,
            feeds_to_create,
        ))
    }
}

impl CompactEncoding<FeedSyncedMessage> for State {
    fn preencode(&mut self, value: &FeedSyncedMessage) -> Result<usize, EncodingError> {
        self.preencode(&value.contiguous_length)
    }

    fn encode(
        &mut self,
        value: &FeedSyncedMessage,
        buffer: &mut [u8],
    ) -> Result<usize, EncodingError> {
        self.encode(&value.contiguous_length, buffer)
    }

    fn decode(&mut self, buffer: &[u8]) -> Result<FeedSyncedMessage, EncodingError> {
        let contiguous_length: u64 = self.decode(buffer)?;
        Ok(FeedSyncedMessage { contiguous_length })
    }
}

#[derive(Copy, Clone, Debug, PartialEq)]
#[repr(u8)]
enum EntryType {
    InitDoc = 0,
    InitPeer = 1,
    DocPart = 2,
    Change = 3,
}

impl EntryType {
    fn from_entry_content(content: &EntryContent) -> Self {
        match content {
            EntryContent::InitDoc { .. } => Self::InitDoc,
            EntryContent::InitPeer { .. } => Self::InitPeer,
            EntryContent::DocPart { .. } => Self::DocPart,
            EntryContent::Change { .. } => Self::Change,
        }
    }
}

impl TryFrom<u8> for EntryType {
    type Error = ();
    fn try_from(input: u8) -> Result<Self, <Self as TryFrom<u8>>::Error> {
        match input {
            0u8 => Ok(Self::InitDoc),
            1u8 => Ok(Self::InitPeer),
            2u8 => Ok(Self::DocPart),
            3u8 => Ok(Self::Change),
            _ => Err(()),
        }
    }
}

impl CompactEncoding<Entry> for State {
    fn preencode(&mut self, value: &Entry) -> Result<usize, EncodingError> {
        self.preencode(&value.version)?;
        self.add_end(2)?; // entry_type and flags
        match &value.content {
            EntryContent::InitDoc {
                doc_part_count,
                meta_doc_data,
                user_doc_data,
            } => {
                self.preencode(doc_part_count)?;
                self.preencode(meta_doc_data)?;
                if let Some(user_doc_data) = &user_doc_data {
                    self.preencode(user_doc_data)?;
                }
            }
            EntryContent::InitPeer {
                doc_part_count,
                meta_doc_data,
                user_doc_data,
            } => {
                self.preencode(doc_part_count)?;
                self.preencode(meta_doc_data)?;
                if let Some(user_doc_data) = &user_doc_data {
                    self.preencode(user_doc_data)?;
                }
            }
            EntryContent::DocPart {
                index,
                meta_doc_data,
                user_doc_data,
            } => {
                self.preencode(index)?;
                if let Some(meta_doc_data) = &meta_doc_data {
                    self.preencode(meta_doc_data)?;
                }
                if let Some(user_doc_data) = &user_doc_data {
                    self.preencode(user_doc_data)?;
                }
            }
            EntryContent::Change { data, .. } => {
                self.preencode(data)?;
            }
        }
        Ok(self.end())
    }

    fn encode(&mut self, value: &Entry, buffer: &mut [u8]) -> Result<usize, EncodingError> {
        self.encode(&value.version, buffer)?;
        self.encode(
            &(EntryType::from_entry_content(&value.content) as u8),
            buffer,
        )?;
        let flags_index = self.start();
        let mut flags: u8 = 0;
        self.add_start(1)?;

        match &value.content {
            EntryContent::InitDoc {
                doc_part_count,
                meta_doc_data,
                user_doc_data,
            } => {
                self.encode(doc_part_count, buffer)?;
                self.encode(meta_doc_data, buffer)?;
                if let Some(user_doc_data) = user_doc_data {
                    flags |= 1;
                    self.encode(user_doc_data, buffer)?;
                }
            }
            EntryContent::InitPeer {
                doc_part_count,
                meta_doc_data,
                user_doc_data,
            } => {
                self.encode(doc_part_count, buffer)?;
                self.encode(meta_doc_data, buffer)?;
                if let Some(user_doc_data) = user_doc_data {
                    flags |= 1;
                    self.encode(user_doc_data, buffer)?;
                }
            }
            EntryContent::DocPart {
                index,
                meta_doc_data,
                user_doc_data,
            } => {
                self.encode(index, buffer)?;
                if let Some(meta_doc_data) = meta_doc_data {
                    flags |= 1;
                    self.encode(meta_doc_data, buffer)?;
                }
                if let Some(user_doc_data) = user_doc_data {
                    flags |= 2;
                    self.encode(user_doc_data, buffer)?;
                }
            }
            EntryContent::Change { meta, data, .. } => {
                if *meta {
                    flags |= 1;
                }
                self.encode(data, buffer)?;
            }
        }
        buffer[flags_index] = flags;
        Ok(self.start())
    }

    fn decode(&mut self, buffer: &[u8]) -> Result<Entry, EncodingError> {
        let version: u8 = self.decode(buffer)?;
        let entry_type: EntryType = self.decode_u8(buffer)?.try_into().unwrap();
        let flags: u8 = self.decode(buffer)?;
        let content = match entry_type {
            EntryType::InitDoc => {
                let doc_part_count: u32 = self.decode(buffer)?;
                let meta_doc_data: Vec<u8> = self.decode(buffer)?;
                let user_doc_data: Option<Vec<u8>> = if flags & 1 != 0 {
                    Some(self.decode(buffer)?)
                } else {
                    None
                };
                EntryContent::InitDoc {
                    doc_part_count,
                    meta_doc_data,
                    user_doc_data,
                }
            }
            EntryType::InitPeer => {
                let doc_part_count: u32 = self.decode(buffer)?;
                let meta_doc_data: Vec<u8> = self.decode(buffer)?;
                let user_doc_data: Option<Vec<u8>> = if flags & 1 != 0 {
                    Some(self.decode(buffer)?)
                } else {
                    None
                };

                EntryContent::InitPeer {
                    doc_part_count,
                    meta_doc_data,
                    user_doc_data,
                }
            }
            EntryType::DocPart => {
                let index: u32 = self.decode(buffer)?;
                let meta_doc_data: Option<Vec<u8>> = if flags & 1 != 0 {
                    Some(self.decode(buffer)?)
                } else {
                    None
                };
                let user_doc_data: Option<Vec<u8>> = if flags & 2 != 0 {
                    Some(self.decode(buffer)?)
                } else {
                    None
                };
                EntryContent::DocPart {
                    index,
                    meta_doc_data,
                    user_doc_data,
                }
            }
            EntryType::Change => {
                let meta: bool = flags & 1 != 0;
                let data: Vec<u8> = self.decode(buffer)?;
                EntryContent::new_change(meta, data)
            }
        };

        Ok(Entry::new(version, content))
    }
}

impl CompactEncoding<NameDescription> for State {
    fn preencode(&mut self, value: &NameDescription) -> Result<usize, EncodingError> {
        self.add_end(1)?; // flags
        self.preencode(&value.name)?;
        if let Some(description) = &value.description {
            self.preencode(description)?;
        }
        Ok(self.end())
    }

    fn encode(
        &mut self,
        value: &NameDescription,
        buffer: &mut [u8],
    ) -> Result<usize, EncodingError> {
        let flags_index = self.start();
        let mut flags: u8 = 0;
        self.add_start(1)?;
        self.encode(&value.name, buffer)?;
        if let Some(description) = &value.description {
            flags |= 1;
            self.encode(description, buffer)?;
        }
        buffer[flags_index] = flags;
        Ok(self.start())
    }

    fn decode(&mut self, buffer: &[u8]) -> Result<NameDescription, EncodingError> {
        let flags: u8 = self.decode(buffer)?;
        let name: String = self.decode(buffer)?;
        let description: Option<String> = if flags & 1 != 0 {
            Some(self.decode(buffer)?)
        } else {
            None
        };
        Ok(NameDescription { name, description })
    }
}

impl CompactEncoding<DocUrlAppendix> for State {
    fn preencode(&mut self, value: &DocUrlAppendix) -> Result<usize, EncodingError> {
        self.add_end(1)?; // flags
        self.preencode(&value.meta_doc_data)?;
        self.preencode(&value.document_type)?;
        if let Some(header) = &value.document_header {
            self.preencode(&header.name)?;
            if let Some(description) = &header.description {
                self.preencode(description)?;
            }
        }
        Ok(self.end())
    }

    fn encode(
        &mut self,
        value: &DocUrlAppendix,
        buffer: &mut [u8],
    ) -> Result<usize, EncodingError> {
        let flags_index = self.start();
        let mut flags: u8 = 0;
        self.add_start(1)?;
        self.encode(&value.meta_doc_data, buffer)?;
        self.encode(&value.document_type, buffer)?;
        if let Some(header) = &value.document_header {
            flags |= 1;
            self.encode(&header.name, buffer)?;
            if let Some(description) = &header.description {
                flags |= 2;
                self.encode(description, buffer)?;
            }
        }
        buffer[flags_index] = flags;
        Ok(self.start())
    }

    fn decode(&mut self, buffer: &[u8]) -> Result<DocUrlAppendix, EncodingError> {
        let flags: u8 = self.decode(buffer)?;
        let meta_doc_data: Vec<u8> = self.decode(buffer)?;
        let document_type: String = self.decode(buffer)?;
        let document_header: Option<NameDescription> = if flags & 1 != 0 {
            let name: String = self.decode(buffer)?;
            if flags & 2 != 0 {
                let description: String = self.decode(buffer)?;
                Some(NameDescription::new_with_description(&name, &description))
            } else {
                Some(NameDescription::new(&name))
            }
        } else {
            None
        };

        Ok(DocUrlAppendix {
            meta_doc_data,
            document_type,
            document_header,
        })
    }
}

pub(crate) fn serialize_entry(entry: &Entry) -> Result<Vec<u8>, EncodingError> {
    let mut enc_state = State::new();
    enc_state.preencode(entry)?;
    let mut buffer = enc_state.create_buffer();
    enc_state.encode(entry, &mut buffer)?;
    Ok(buffer.to_vec())
}
