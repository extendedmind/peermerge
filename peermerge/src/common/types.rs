use crate::DocumentId;

/// Type of feed.
#[derive(Copy, Clone, Debug, PartialEq)]
#[repr(u8)]
pub enum FeedType {
    Hypercore = 1,
    P2Panda = 2,
}

impl TryFrom<u8> for FeedType {
    type Error = ();
    fn try_from(input: u8) -> Result<Self, <Self as TryFrom<u8>>::Error> {
        match input {
            1u8 => Ok(Self::Hypercore),
            22u8 => Ok(Self::P2Panda),
            _ => Err(()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct DocUrlInfo {
    pub version: u8,
    pub feed_type: FeedType,
    pub proxy_only: bool,
    pub encrypted: Option<bool>,
    pub compatible: bool,
}

impl DocUrlInfo {
    pub(crate) fn new(version: u8, feed_type: FeedType, encrypted: bool) -> Self {
        Self {
            version,
            feed_type,
            proxy_only: false,
            encrypted: Some(encrypted),
            compatible: feed_type == FeedType::Hypercore && version == 1,
        }
    }

    pub(crate) fn new_proxy_only(version: u8, feed_type: FeedType) -> Self {
        Self {
            version,
            feed_type,
            proxy_only: true,
            encrypted: None,
            compatible: feed_type == FeedType::Hypercore && version == 1,
        }
    }
}

#[derive(Debug, Clone)]
pub struct DocumentInfo {
    pub document_id: DocumentId,
    pub doc_url_info: DocUrlInfo,
    pub document_header: Option<NameDescription>,
    pub parent_document_id: Option<DocumentId>,
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
