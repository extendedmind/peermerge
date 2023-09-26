use hypercore_protocol::hypercore::VerifyingKey;

use super::constants::PEERMERGE_VERSION;
use crate::{DocumentId, FeedDiscoveryKey, FeedPublicKey};

/// Type of feed.
#[derive(Copy, Clone, Debug, PartialEq)]
#[repr(u8)]
pub enum FeedType {
    Hypercore = 0,
    P2Panda = 1,
}

impl TryFrom<u8> for FeedType {
    type Error = ();
    fn try_from(input: u8) -> Result<Self, <Self as TryFrom<u8>>::Error> {
        match input {
            0u8 => Ok(Self::Hypercore),
            1u8 => Ok(Self::P2Panda),
            _ => Err(()),
        }
    }
}

/// Access type to a document.
#[derive(Copy, Clone, Debug, PartialEq)]
#[repr(u8)]
pub enum AccessType {
    /// Proxy-only, meaning not able to access the data inside the document
    /// but only proxy the data to other peers. NB: If the document is not
    /// encrypted, reading is technically still possible by investigating
    /// the storage directly.
    Proxy = 0,
    /// Reading the document is possible, but not making changes to it.
    ReadOnly = 1,
    /// Reading and writing to the document is possible.
    ReadWrite = 2,
}

impl TryFrom<u8> for AccessType {
    type Error = ();
    fn try_from(input: u8) -> Result<Self, <Self as TryFrom<u8>>::Error> {
        match input {
            0u8 => Ok(Self::Proxy),
            1u8 => Ok(Self::ReadOnly),
            2u8 => Ok(Self::ReadWrite),
            _ => Err(()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct StaticDocumentInfo {
    pub version: u8,
    pub feed_type: FeedType,
    pub child: bool,
    pub document_public_key: FeedPublicKey,
    pub document_discovery_key: FeedDiscoveryKey,
    pub document_id: DocumentId,
    pub document_signature_verifying_key: VerifyingKey,
}

impl StaticDocumentInfo {
    pub(crate) fn from_keys(
        document_public_key: FeedPublicKey,
        document_discovery_key: FeedDiscoveryKey,
        document_id: DocumentId,
        document_signature_verifying_key: VerifyingKey,
        child: bool,
    ) -> Self {
        Self::new(
            PEERMERGE_VERSION,
            FeedType::Hypercore,
            child,
            document_public_key,
            document_discovery_key,
            document_id,
            document_signature_verifying_key,
        )
    }

    pub(crate) fn new(
        version: u8,
        feed_type: FeedType,
        child: bool,
        document_public_key: [u8; 32],
        document_discovery_key: [u8; 32],
        document_id: DocumentId,
        document_signature_verifying_key: VerifyingKey,
    ) -> Self {
        Self {
            version,
            feed_type,
            child,
            document_public_key,
            document_discovery_key,
            document_id,
            document_signature_verifying_key,
        }
    }
}

#[derive(Debug, Clone)]
pub struct DynamicDocumentInfo {
    pub document_type: String,
    pub document_header: Option<NameDescription>,
}

/// Document information parsed from a document URL. NB: This is a
/// limited set of information compared to what is received when
/// manipulating documents in a live peermerge, i.e. DocumentInfo.
/// Most notably, the URL will not contain a full list of child
/// documents because that list can grow infinitely and thus cause
/// the URL to exceed the maximum character limit.
#[derive(Debug, Clone)]
pub struct UrlDocumentInfo {
    /// Access to the document
    pub access_type: AccessType,
    /// Is the document encrypted. For access_type Proxy, this is None.
    pub encrypted: Option<bool>,
    /// Static information about the document that's always available
    /// regardless of access_type.
    pub static_info: StaticDocumentInfo,
    /// Dynamic information about the document. This changes over time,
    /// and is None for access_type Proxy, and might contain outdated
    /// information for
    pub dynamic_info: Option<DynamicDocumentInfo>,
}

impl UrlDocumentInfo {
    pub fn id(&self) -> DocumentId {
        self.static_info.document_id
    }
}

#[derive(Debug, Clone)]
pub struct DocumentInfo {
    /// Access to the document
    pub access_type: AccessType,
    /// Is the document encrypted. For access_type Proxy, this is None.
    pub encrypted: Option<bool>,
    /// Child documents of this document. See static_info.child to know
    /// whether this is a child of some other document.
    pub child_documents: Vec<DocumentId>,
    /// Static information about the document that's always available
    /// regardless of access_type.
    pub static_info: StaticDocumentInfo,
    /// Dynamic information about the document. This changes over time,
    /// and is None for access_type Proxy, and might contain outdated
    /// information for
    pub dynamic_info: Option<DynamicDocumentInfo>,
}

impl DocumentInfo {
    pub fn id(&self) -> DocumentId {
        self.static_info.document_id
    }
}

#[derive(Debug, Clone)]
pub struct DocumentSharingInfo {
    pub proxy: bool,
    pub proxy_doc_url: String,
    pub read_write_doc_url: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NameDescription {
    pub name: String,
    pub description: Option<String>,
}

impl NameDescription {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            description: None,
        }
    }

    pub fn new_with_description(name: &str, description: &str) -> Self {
        Self {
            name: name.to_string(),
            description: Some(description.to_string()),
        }
    }
}
