use automerge::AutomergeError;
use hypercore_protocol::{hypercore::HypercoreError, ChannelSendError};
use thiserror::Error;

/// Common error type for the peermerge interface
#[derive(Error, Debug)]
pub enum PeermergeError {
    /// Bad argument
    #[error("Bad argument. {context}")]
    BadArgument {
        /// Context for the error
        context: String,
    },
    #[error("Automerge error occured.{}",
          .context.as_ref().map_or_else(String::new, |ctx| format!(" {}.", ctx)))]
    AutomergeError {
        /// Context for the error
        context: Option<String>,
        /// Original source error
        #[source]
        source: AutomergeError,
    },
    /// Disconnected
    #[error("Peermerge disconnected. {context}")]
    Disconnected {
        /// Context for the error
        context: String,
    },
    /// Not writable
    #[error("Peermerge not writable")]
    NotWritable,
    /// Invalid operation
    #[error("Invalid operation. {context}")]
    InvalidOperation {
        /// Context for the error
        context: String,
    },
    /// Unexpected IO error occured
    #[error("Unrecoverable input/output error occured.{}",
          .context.as_ref().map_or_else(String::new, |ctx| format!(" {}.", ctx)))]
    IO {
        /// Context for the error
        context: Option<String>,
        /// Original source error
        #[source]
        source: std::io::Error,
    },
}

impl From<std::io::Error> for PeermergeError {
    fn from(err: std::io::Error) -> Self {
        Self::IO {
            context: None,
            source: err,
        }
    }
}

/// By default, all hypercore errors are unexpected operation failures, that are passed through
/// as InvalidOperation.
impl From<HypercoreError> for PeermergeError {
    fn from(err: HypercoreError) -> Self {
        match err {
            HypercoreError::BadArgument { context } => PeermergeError::InvalidOperation {
                context: format!("Hypercore bad argument: {}", context),
            },
            HypercoreError::InvalidSignature { context } => PeermergeError::InvalidOperation {
                context: format!("Hypercore invalid signature: {}", context),
            },
            HypercoreError::InvalidChecksum { context } => PeermergeError::InvalidOperation {
                context: format!("Hypercore invalid checksum: {}", context),
            },
            HypercoreError::CorruptStorage { context, store } => PeermergeError::InvalidOperation {
                context: format!(
                    "Hypercore corrupt storage, store: {}, context: {:?}",
                    store, context
                ),
            },
            HypercoreError::EmptyStorage { store } => PeermergeError::InvalidOperation {
                context: format!("Hypercore empty storage, store: {}", store),
            },
            HypercoreError::NotWritable => PeermergeError::InvalidOperation {
                context: format!("Hypercore storage not writable"),
            },
            HypercoreError::InvalidOperation { context } => PeermergeError::InvalidOperation {
                context: format!("Hypercore invalid operation: {}", context),
            },
            HypercoreError::IO { context, source } => PeermergeError::IO {
                context: Some(format!("Hypercore IO error, context: {:?}", context)),
                source,
            },
        }
    }
}

impl From<AutomergeError> for PeermergeError {
    fn from(err: AutomergeError) -> Self {
        Self::AutomergeError {
            context: None,
            source: err,
        }
    }
}

impl<T> From<futures::channel::mpsc::TrySendError<T>> for PeermergeError {
    fn from(err: futures::channel::mpsc::TrySendError<T>) -> Self {
        if err.is_disconnected() {
            Self::Disconnected {
                context: "Channel disconnected".to_string(),
            }
        } else {
            Self::InvalidOperation {
                context: format!("Unexpected channel error, {:?}", err),
            }
        }
    }
}

impl<T> From<ChannelSendError<T>> for PeermergeError {
    fn from(err: ChannelSendError<T>) -> Self {
        Self::InvalidOperation {
            context: format!("Unexpected hypercore protocol channel error, {:?}", err),
        }
    }
}
