//! Binary encoding of needed peermerge persistent data
use compact_encoding::{CompactEncoding, EncodingError, State};
use hypercore_protocol::hypercore::SigningKey;
use std::convert::TryInto;

pub(crate) use crate::common::entry::Entry;
pub(crate) use crate::common::message::BroadcastMessage;
pub(crate) use crate::common::state::{DocumentState, PeermergeState};
use crate::document::DocumentSettings;
use crate::feeds::FeedPublicKey;

use super::cipher::{DocUrlAppendix, DocumentSecret};
use super::constants::PEERMERGE_VERSION;
use super::entry::EntryContent;
use super::message::{FeedSyncedMessage, FeedVerificationMessage, FeedsChangedMessage};
use super::state::{
    ChildDocumentInfo, DocumentContent, DocumentCursor, DocumentFeedInfo, DocumentFeedsState,
};
use crate::{AccessType, NameDescription, PeerId, PeermergeError};

impl CompactEncoding<PeermergeState> for State {
    fn preencode(&mut self, value: &PeermergeState) -> Result<usize, EncodingError> {
        self.preencode(&value.version)?;
        self.preencode_fixed_16()?; // peer_id
        self.preencode(&value.default_peer_header)?;
        self.preencode(&value.document_ids)?;
        self.preencode(&value.document_settings)
    }

    fn encode(
        &mut self,
        value: &PeermergeState,
        buffer: &mut [u8],
    ) -> Result<usize, EncodingError> {
        self.encode(&value.version, buffer)?;
        self.encode_fixed_16(&value.peer_id, buffer)?;
        self.encode(&value.default_peer_header, buffer)?;
        self.encode(&value.document_ids, buffer)?;
        self.encode(&value.document_settings, buffer)
    }

    fn decode(&mut self, buffer: &[u8]) -> Result<PeermergeState, EncodingError> {
        let version: u8 = self.decode(buffer)?;
        let peer_id: PeerId = self.decode_fixed_16(buffer)?.to_vec().try_into().unwrap();
        let peer_header: NameDescription = self.decode(buffer)?;
        let document_ids: Vec<[u8; 32]> = self.decode(buffer)?;
        let document_settings: DocumentSettings = self.decode(buffer)?;
        Ok(PeermergeState::new_with_version(
            version,
            peer_id,
            peer_header,
            document_ids,
            document_settings,
        ))
    }
}

impl CompactEncoding<DocumentSettings> for State {
    fn preencode(&mut self, value: &DocumentSettings) -> Result<usize, EncodingError> {
        self.preencode(&value.max_entry_data_size_bytes)?;
        self.preencode(&value.max_write_feed_length)
    }

    fn encode(
        &mut self,
        value: &DocumentSettings,
        buffer: &mut [u8],
    ) -> Result<usize, EncodingError> {
        self.encode(&value.max_entry_data_size_bytes, buffer)?;
        self.encode(&value.max_write_feed_length, buffer)
    }

    fn decode(&mut self, buffer: &[u8]) -> Result<DocumentSettings, EncodingError> {
        let max_entry_data_size_bytes: usize = self.decode(buffer)?;
        let max_write_feed_length: u64 = self.decode(buffer)?;
        Ok(DocumentSettings {
            max_entry_data_size_bytes,
            max_write_feed_length,
        })
    }
}

impl CompactEncoding<DocumentState> for State {
    fn preencode(&mut self, value: &DocumentState) -> Result<usize, EncodingError> {
        self.preencode(&value.version)?;
        self.preencode_fixed_32()?; // doc_signature_verifying_key
        self.add_end(1)?; // flags
        self.preencode(&value.feeds_state)?; // flags
        if let Some(content) = &value.content {
            self.preencode(content)?;
        }
        if !value.child_documents.is_empty() {
            let len = value.child_documents.len();
            self.preencode(&len)?;
            for child_document in &value.child_documents {
                self.preencode(child_document)?;
            }
        }
        Ok(self.end())
    }

    fn encode(&mut self, value: &DocumentState, buffer: &mut [u8]) -> Result<usize, EncodingError> {
        self.encode(&value.version, buffer)?;
        self.encode_fixed_32(&value.doc_signature_verifying_key, buffer)?;
        let flags_index = self.start();
        let mut flags: u8 = 0;
        self.add_start(1)?;
        if value.access_type == AccessType::Proxy {
            flags |= 1;
        }
        if value.encrypted.unwrap_or(false) {
            flags |= 2;
        }
        self.encode(&value.feeds_state, buffer)?;
        if let Some(content) = &value.content {
            flags |= 4;
            self.encode(content, buffer)?;
        }
        if !value.child_documents.is_empty() {
            flags |= 8;
            let len = value.child_documents.len();
            self.encode(&len, buffer)?;
            for child_document in &value.child_documents {
                self.encode(child_document, buffer)?;
            }
        }
        if value.access_type == AccessType::ReadWrite {
            flags |= 16;
        }
        if value.child {
            flags |= 32;
        }
        buffer[flags_index] = flags;
        Ok(self.start())
    }

    fn decode(&mut self, buffer: &[u8]) -> Result<DocumentState, EncodingError> {
        let version: u8 = self.decode(buffer)?;
        let doc_signature_verifying_key: [u8; 32] =
            self.decode_fixed_32(buffer)?.to_vec().try_into().unwrap();
        let flags: u8 = self.decode(buffer)?;
        let proxy = flags & 1 != 0;
        let encrypted: Option<bool> = if !proxy { Some(flags & 2 != 0) } else { None };
        let feeds_state: DocumentFeedsState = self.decode(buffer)?;
        let content: Option<DocumentContent> = if flags & 4 != 0 {
            Some(self.decode(buffer)?)
        } else {
            None
        };
        let child_documents: Vec<ChildDocumentInfo> = if flags & 8 != 0 {
            let len: usize = self.decode(buffer)?;
            let mut docs: Vec<ChildDocumentInfo> = Vec::with_capacity(len);
            for _ in 0..len {
                let doc: ChildDocumentInfo = self.decode(buffer)?;
                docs.push(doc);
            }
            docs
        } else {
            vec![]
        };
        let writable = flags & 16 != 0;
        let access_type = if proxy {
            AccessType::Proxy
        } else if writable {
            AccessType::ReadWrite
        } else {
            AccessType::ReadOnly
        };
        let child = flags & 32 != 0;
        Ok(DocumentState::new_with_version(
            version,
            access_type,
            doc_signature_verifying_key,
            encrypted,
            feeds_state,
            content,
            child,
            child_documents,
        ))
    }
}

impl CompactEncoding<DocumentFeedInfo> for State {
    fn preencode(&mut self, value: &DocumentFeedInfo) -> Result<usize, EncodingError> {
        self.preencode_fixed_16()?; // peer_id
        self.preencode_fixed_32()?; // public_key
        self.add_end(1)?; // flags
        if value.replaced_public_key.is_some() {
            self.preencode_fixed_32()?;
        }
        self.preencode(&value.signature)?; // signature
        Ok(self.end())
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
        if let Some(replaced_public_key) = &value.replaced_public_key {
            flags |= 1;
            self.encode_fixed_32(replaced_public_key, buffer)?;
        }
        self.encode(&value.signature, buffer)?; // signature
        if value.verified {
            flags |= 2;
        }
        if value.removed {
            flags |= 4;
        }
        buffer[flags_index] = flags;
        Ok(self.start())
    }

    fn decode(&mut self, buffer: &[u8]) -> Result<DocumentFeedInfo, EncodingError> {
        let peer_id: PeerId = self.decode_fixed_16(buffer)?.to_vec().try_into().unwrap();
        let public_key: FeedPublicKey = self.decode_fixed_32(buffer)?.to_vec().try_into().unwrap();
        let flags: u8 = self.decode(buffer)?;
        let replaced_public_key: Option<FeedPublicKey> = if flags & 1 != 0 {
            Some(self.decode_fixed_32(buffer)?.to_vec().try_into().unwrap())
        } else {
            None
        };
        let signature: Vec<u8> = self.decode(buffer)?; // signature
        let verified = flags & 2 != 0;
        let removed = flags & 4 != 0;
        Ok(DocumentFeedInfo {
            peer_id,
            public_key,
            replaced_public_key,
            signature,
            verified,
            removed,
            discovery_key: None,
        })
    }
}

impl CompactEncoding<ChildDocumentInfo> for State {
    fn preencode(&mut self, value: &ChildDocumentInfo) -> Result<usize, EncodingError> {
        self.preencode_fixed_32()?; // doc_public_key
        self.preencode_fixed_32()?; // doc_signature_verifying_key
        self.preencode(&value.signature)?;
        self.add_end(1) // flags
    }

    fn encode(
        &mut self,
        value: &ChildDocumentInfo,
        buffer: &mut [u8],
    ) -> Result<usize, EncodingError> {
        self.encode_fixed_32(&value.doc_public_key, buffer)?;
        self.encode_fixed_32(&value.doc_signature_verifying_key.to_bytes(), buffer)?;
        self.encode(&value.signature, buffer)?;
        let flags_index = self.start();
        let mut flags: u8 = 0;
        self.add_start(1)?;
        if value.creation_pending {
            flags |= 1;
        }
        buffer[flags_index] = flags;
        Ok(self.start())
    }

    fn decode(&mut self, buffer: &[u8]) -> Result<ChildDocumentInfo, EncodingError> {
        let doc_public_key: FeedPublicKey =
            self.decode_fixed_32(buffer)?.to_vec().try_into().unwrap();
        let doc_signature_verifying_key: [u8; 32] =
            self.decode_fixed_32(buffer)?.to_vec().try_into().unwrap();
        let signature: Vec<u8> = self.decode(buffer)?;
        let flags: u8 = self.decode(buffer)?;
        Ok(ChildDocumentInfo::new_from_data(
            doc_public_key,
            doc_signature_verifying_key,
            signature,
            flags & 1 != 0,
        ))
    }
}

impl CompactEncoding<DocumentFeedsState> for State {
    fn preencode(&mut self, value: &DocumentFeedsState) -> Result<usize, EncodingError> {
        self.preencode_fixed_16()?; // peer_id
        self.preencode_fixed_32()?; // doc_public_key
        self.add_end(1)?; // flags
        let len = value
            .other_feeds
            .values()
            .fold(0, |acc, feeds| acc + feeds.len());
        if len > 0 {
            self.preencode(&len)?;
            for feeds in value.other_feeds.values() {
                for feed in feeds {
                    self.preencode(feed)?;
                }
            }
        }
        if let Some(write_feed) = &value.write_feed {
            self.preencode(write_feed)?;
        }
        Ok(self.end())
    }

    fn encode(
        &mut self,
        value: &DocumentFeedsState,
        buffer: &mut [u8],
    ) -> Result<usize, EncodingError> {
        self.encode_fixed_16(&value.peer_id, buffer)?;
        self.encode_fixed_32(&value.doc_public_key, buffer)?;
        let flags_index = self.start();
        let mut flags: u8 = 0;
        self.add_start(1)?;
        if value.doc_feed_verified {
            flags |= 1;
        }
        let len = value
            .other_feeds
            .values()
            .fold(0, |acc, feeds| acc + feeds.len());
        if len > 0 {
            flags |= 2;
            self.encode(&len, buffer)?;
            for feeds in value.other_feeds.values() {
                for feed in feeds {
                    self.encode(feed, buffer)?;
                }
            }
        }
        if let Some(write_feed) = &value.write_feed {
            flags |= 4;
            self.encode(write_feed, buffer)?;
        }

        buffer[flags_index] = flags;
        Ok(self.start())
    }

    fn decode(&mut self, buffer: &[u8]) -> Result<DocumentFeedsState, EncodingError> {
        let peer_id: PeerId = self.decode_fixed_16(buffer)?.to_vec().try_into().unwrap();
        let doc_public_key: FeedPublicKey =
            self.decode_fixed_32(buffer)?.to_vec().try_into().unwrap();
        let flags: u8 = self.decode(buffer)?;
        let doc_feed_verified = flags & 4 != 0;
        let other_feeds: Vec<DocumentFeedInfo> = if flags & 2 != 0 {
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
        let write_feed: Option<DocumentFeedInfo> = if flags & 4 != 0 {
            Some(self.decode(buffer)?)
        } else {
            None
        };

        Ok(DocumentFeedsState::new_from_data(
            peer_id,
            doc_public_key,
            doc_feed_verified,
            write_feed,
            other_feeds,
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
        if let Some(document_type) = &value.temporary_document_type {
            self.preencode(document_type)?;
        }
        if let Some(document_header) = &value.temporary_document_header {
            self.preencode(document_header)?;
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
        if let Some(document_type) = &value.temporary_document_type {
            flags |= 4;
            self.encode(document_type, buffer)?;
        }
        if let Some(document_header) = &value.temporary_document_header {
            flags |= 8;
            self.encode(document_header, buffer)?;
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
        let temporary_document_type: Option<String> = if flags & 4 != 0 {
            Some(self.decode(buffer)?)
        } else {
            None
        };
        let temporary_document_header: Option<NameDescription> = if flags & 8 != 0 {
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
            temporary_document_type,
            temporary_document_header,
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
        let len = value.active_feeds.len();
        if len > 0 {
            self.preencode(&len)?;
            for feed in &value.active_feeds {
                self.preencode(feed)?;
            }
        }
        if let Some(inactive_feeds) = &value.inactive_feeds {
            let len = inactive_feeds.len();
            self.preencode(&len)?;
            for feed in inactive_feeds {
                self.preencode(feed)?;
            }
        }
        let len = value.child_documents.len();
        if len > 0 {
            self.preencode(&len)?;
            for child in &value.child_documents {
                self.preencode(child)?;
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
        let len = value.active_feeds.len();
        if len > 0 {
            flags |= 2;
            self.encode(&len, buffer)?;
            for feed in &value.active_feeds {
                self.encode(feed, buffer)?;
            }
        }
        if let Some(inactive_feeds) = &value.inactive_feeds {
            flags |= 4;
            let len = inactive_feeds.len();
            self.encode(&len, buffer)?;
            for feed in inactive_feeds {
                self.encode(feed, buffer)?;
            }
        }
        let len = value.child_documents.len();
        if len > 0 {
            flags |= 8;
            self.encode(&len, buffer)?;
            for child in &value.child_documents {
                self.encode(child, buffer)?;
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
        let active_feeds: Vec<DocumentFeedInfo> = if flags & 2 != 0 {
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
        let inactive_feeds: Option<Vec<DocumentFeedInfo>> = if flags & 4 != 0 {
            let len: usize = self.decode(buffer)?;
            let mut feeds: Vec<DocumentFeedInfo> = Vec::with_capacity(len);
            for _ in 0..len {
                let feed: DocumentFeedInfo = self.decode(buffer)?;
                feeds.push(feed);
            }
            Some(feeds)
        } else {
            None
        };
        let child_documents: Vec<ChildDocumentInfo> = if flags & 8 != 0 {
            let len: usize = self.decode(buffer)?;
            let mut children: Vec<ChildDocumentInfo> = Vec::with_capacity(len);
            for _ in 0..len {
                let child: ChildDocumentInfo = self.decode(buffer)?;
                children.push(child);
            }
            children
        } else {
            vec![]
        };

        Ok(BroadcastMessage::new(
            write_feed,
            active_feeds,
            inactive_feeds,
            child_documents,
        ))
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

impl CompactEncoding<FeedVerificationMessage> for State {
    fn preencode(&mut self, value: &FeedVerificationMessage) -> Result<usize, EncodingError> {
        self.preencode_fixed_32()?;
        self.preencode_fixed_32()?;
        self.add_end(1)?;
        if value.peer_id.is_some() {
            self.preencode_fixed_16()?;
        }
        Ok(self.end())
    }

    fn encode(
        &mut self,
        value: &FeedVerificationMessage,
        buffer: &mut [u8],
    ) -> Result<usize, EncodingError> {
        self.encode_fixed_32(&value.doc_discovery_key, buffer)?;
        self.encode_fixed_32(&value.feed_discovery_key, buffer)?;
        let flags_index = self.start();
        let mut flags: u8 = 0;
        self.add_start(1)?;
        if value.verified {
            flags |= 1;
        }
        if let Some(peer_id) = &value.peer_id {
            flags |= 2;
            self.encode_fixed_16(peer_id, buffer)?;
        }
        buffer[flags_index] = flags;
        Ok(self.start())
    }

    fn decode(&mut self, buffer: &[u8]) -> Result<FeedVerificationMessage, EncodingError> {
        let doc_discovery_key: [u8; 32] =
            self.decode_fixed_32(buffer)?.to_vec().try_into().unwrap();
        let feed_discovery_key: [u8; 32] =
            self.decode_fixed_32(buffer)?.to_vec().try_into().unwrap();
        let flags: u8 = self.decode(buffer)?;
        let verified: bool = flags & 1 != 0;

        let peer_id: Option<PeerId> = if flags & 2 != 0 {
            Some(self.decode_fixed_16(buffer)?.to_vec().try_into().unwrap())
        } else {
            None
        };

        Ok(FeedVerificationMessage {
            doc_discovery_key,
            feed_discovery_key,
            verified,
            peer_id,
        })
    }
}

impl CompactEncoding<FeedSyncedMessage> for State {
    fn preencode(&mut self, value: &FeedSyncedMessage) -> Result<usize, EncodingError> {
        self.preencode(&value.contiguous_length)?;
        let len = value.pending_child_documents.len();
        for child_document in &value.pending_child_documents {
            self.preencode(child_document)?;
        }
        self.preencode(&len)?;
        Ok(self.end())
    }

    fn encode(
        &mut self,
        value: &FeedSyncedMessage,
        buffer: &mut [u8],
    ) -> Result<usize, EncodingError> {
        self.encode(&value.contiguous_length, buffer)?;
        let len = value.pending_child_documents.len();
        self.encode(&len, buffer)?;
        for child_document in &value.pending_child_documents {
            self.encode(child_document, buffer)?;
        }
        Ok(self.start())
    }

    fn decode(&mut self, buffer: &[u8]) -> Result<FeedSyncedMessage, EncodingError> {
        let contiguous_length: u64 = self.decode(buffer)?;
        let len: usize = self.decode(buffer)?;
        let mut pending_child_documents: Vec<ChildDocumentInfo> = Vec::with_capacity(len);
        for _ in 0..len {
            let doc: ChildDocumentInfo = self.decode(buffer)?;
            pending_child_documents.push(doc);
        }
        Ok(FeedSyncedMessage {
            contiguous_length,
            pending_child_documents,
        })
    }
}

#[derive(Copy, Clone, Debug, PartialEq)]
#[repr(u8)]
enum EntryType {
    InitDoc = 0,
    InitPeer = 1,
    DocPart = 2,
    Change = 3,
    ChangePart = 4,
}

impl EntryType {
    fn from_entry_content(content: &EntryContent) -> Self {
        match content {
            EntryContent::InitDoc { .. } => Self::InitDoc,
            EntryContent::InitPeer { .. } => Self::InitPeer,
            EntryContent::DocPart { .. } => Self::DocPart,
            EntryContent::Change { .. } => Self::Change,
            EntryContent::ChangePart { .. } => Self::ChangePart,
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
        if !matches!(value.content, EntryContent::ChangePart { .. }) {
            self.add_end(2)?; // entry_type and flags
        } else {
            self.add_end(1)?; // entry_type
        };
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
            EntryContent::Change {
                data, part_count, ..
            } => {
                self.preencode(part_count)?;
                self.preencode(data)?;
            }
            EntryContent::ChangePart { index, data } => {
                self.preencode(index)?;
                self.preencode(data)?;
            }
        }
        Ok(self.end())
    }

    fn encode(&mut self, value: &Entry, buffer: &mut [u8]) -> Result<usize, EncodingError> {
        self.encode(&value.version, buffer)?;
        let entry_type = EntryType::from_entry_content(&value.content);
        self.encode(&(entry_type as u8), buffer)?;
        let mut flags: u8 = 0;
        let flags_index: usize = if entry_type != EntryType::ChangePart {
            let index = self.start();
            self.add_start(1)?;
            index
        } else {
            0
        };
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
            EntryContent::Change {
                meta,
                data,
                part_count,
                ..
            } => {
                if *meta {
                    flags |= 1;
                }
                self.encode(part_count, buffer)?;
                self.encode(data, buffer)?;
            }
            EntryContent::ChangePart { index, data } => {
                self.encode(index, buffer)?;
                self.encode(data, buffer)?;
            }
        }
        if !matches!(value.content, EntryContent::ChangePart { .. }) {
            buffer[flags_index] = flags;
        }
        Ok(self.start())
    }

    fn decode(&mut self, buffer: &[u8]) -> Result<Entry, EncodingError> {
        let version: u8 = self.decode(buffer)?;
        let entry_type: EntryType = self.decode_u8(buffer)?.try_into().unwrap();
        let flags: u8 = if entry_type != EntryType::ChangePart {
            self.decode(buffer)?
        } else {
            0
        };
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
                let part_count: u32 = self.decode(buffer)?;
                let data: Vec<u8> = self.decode(buffer)?;
                EntryContent::new_change(meta, part_count, data)
            }
            EntryType::ChangePart => {
                let index: u32 = self.decode(buffer)?;
                let data: Vec<u8> = self.decode(buffer)?;
                EntryContent::ChangePart { index, data }
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

impl CompactEncoding<DocumentSecret> for State {
    fn preencode(&mut self, value: &DocumentSecret) -> Result<usize, EncodingError> {
        self.add_end(2)?; // version and flags
        if let Some(key) = &value.encryption_key {
            self.preencode(key)?;
        }
        if value.doc_signature_signing_key.is_some() {
            self.preencode_fixed_32()?;
        }
        Ok(self.end())
    }

    fn encode(
        &mut self,
        value: &DocumentSecret,
        buffer: &mut [u8],
    ) -> Result<usize, EncodingError> {
        self.encode(&PEERMERGE_VERSION, buffer)?;
        let flags_index = self.start();
        let mut flags: u8 = 0;
        self.add_start(1)?;
        if let Some(encryption_key) = &value.encryption_key {
            flags |= 1;
            self.encode(encryption_key, buffer)?;
        }
        if let Some(doc_signature_signing_key) = &value.doc_signature_signing_key {
            flags |= 2;
            self.encode_fixed_32(&doc_signature_signing_key.to_bytes(), buffer)?;
        }

        buffer[flags_index] = flags;
        Ok(self.start())
    }

    fn decode(&mut self, buffer: &[u8]) -> Result<DocumentSecret, EncodingError> {
        let version: u8 = self.decode(buffer)?;
        let flags: u8 = self.decode(buffer)?;
        let encryption_key: Option<Vec<u8>> = if flags & 1 != 0 {
            let key: Vec<u8> = self.decode(buffer)?;
            Some(key)
        } else {
            None
        };
        let doc_signature_signing_key: Option<SigningKey> = if flags & 2 != 0 {
            let key: [u8; 32] = self.decode_fixed_32(buffer)?.to_vec().try_into().unwrap();
            Some(SigningKey::from_bytes(&key))
        } else {
            None
        };

        Ok(DocumentSecret {
            version,
            encryption_key,
            doc_signature_signing_key,
        })
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

pub(crate) fn serialize_init_entries(
    init_entries: Vec<Entry>,
) -> Result<Vec<Vec<u8>>, PeermergeError> {
    init_entries
        .into_iter()
        .enumerate()
        .map(|(i, entry)| {
            if i == 0
                && !matches!(
                    entry.content,
                    EntryContent::InitDoc { .. } | EntryContent::InitPeer { .. }
                )
            {
                Err(PeermergeError::InvalidOperation {
                    context: "First init entry not InitDoc nor InitPeer".to_string(),
                })
            } else {
                // Only the first entry needs to be signed, the ones after that
                // are guaranteed to be derived from the first entry's signature
                // via Hypercore's merkle tree.
                serialize_entry(&entry)
            }
        })
        .collect()
}

pub(crate) fn serialize_entry(entry: &Entry) -> Result<Vec<u8>, PeermergeError> {
    let mut enc_state = State::new();
    enc_state.preencode(entry)?;
    let mut buffer = enc_state.create_buffer();
    enc_state.encode(entry, &mut buffer)?;
    Ok(buffer.to_vec())
}
