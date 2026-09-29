//! Worker-owned message handler declarations and routing.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use rustclamp_core::{
    Contribution, ContributionId, ContributionTarget, ContributionTargetId, ModuleId, Qualifier,
    QualifierId,
};
use rustclamp_messaging::MessageEnvelope;
use std::collections::{BTreeMap, btree_map::Entry};
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// Qualifier for one worker handler target.
pub struct WorkerHandlers;

impl Qualifier for WorkerHandlers {
    const ID: QualifierId = QualifierId::new("rustclamp.worker.handlers");
}

/// Typed future returned by a message handler.
pub type HandlerFuture = Pin<Box<dyn Future<Output = Result<(), HandlerError>> + Send + 'static>>;

/// Application or infrastructure error returned from one handler attempt.
pub type HandlerError = Box<dyn Error + Send + Sync>;

type Handler = Arc<dyn Fn(MessageEnvelope) -> HandlerFuture + Send + Sync + 'static>;

/// A module's declaration that it handles one message name and schema version.
pub struct HandlerDeclaration {
    name: String,
    schema_version: u32,
    handler: Handler,
}

impl HandlerDeclaration {
    /// Creates a handler declaration; the target validates its route identity.
    pub fn new<F, Fut>(name: impl Into<String>, schema_version: u32, handler: F) -> Self
    where
        F: Fn(MessageEnvelope) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), HandlerError>> + Send + 'static,
    {
        Self {
            name: name.into(),
            schema_version,
            handler: Arc::new(move |message| Box::pin(handler(message))),
        }
    }
}

impl Contribution for HandlerDeclaration {
    const ID: ContributionId = ContributionId::new("rustclamp.worker.handler");
}

/// Target that validates and compiles message handler declarations.
pub struct HandlerTarget;

impl ContributionTarget for HandlerTarget {
    type Contribution = HandlerDeclaration;
    type Runtime = HandlerRegistry;
    type Error = HandlerBuildError;

    const ID: ContributionTargetId = ContributionTargetId::new("rustclamp.worker.handlers");

    fn build(
        &self,
        contributions: &[(ModuleId, Self::Contribution)],
    ) -> Result<Self::Runtime, Self::Error> {
        let mut handlers = BTreeMap::new();
        for (owner, declaration) in contributions {
            if declaration.name.trim().is_empty() {
                return Err(HandlerBuildError::EmptyName { owner: *owner });
            }
            if declaration.schema_version == 0 {
                return Err(HandlerBuildError::InvalidVersion {
                    owner: *owner,
                    name: declaration.name.clone(),
                });
            }
            let key = (declaration.name.clone(), declaration.schema_version);
            match handlers.entry(key) {
                Entry::Vacant(entry) => {
                    entry.insert((*owner, Arc::clone(&declaration.handler)));
                }
                Entry::Occupied(entry) => {
                    return Err(HandlerBuildError::Duplicate {
                        name: declaration.name.clone(),
                        schema_version: declaration.schema_version,
                        first_owner: entry.get().0,
                        second_owner: *owner,
                    });
                }
            }
        }
        Ok(HandlerRegistry { handlers })
    }
}

/// Validated registry produced by the worker handler target.
#[derive(Clone)]
pub struct HandlerRegistry {
    handlers: BTreeMap<(String, u32), (ModuleId, Handler)>,
}

impl HandlerRegistry {
    /// Decodes a serialized envelope and dispatches it to its matching handler.
    pub async fn dispatch_json(&self, bytes: &[u8]) -> Result<(), DispatchError> {
        let message = serde_json::from_slice(bytes).map_err(DispatchError::Decode)?;
        self.dispatch(message).await
    }

    /// Dispatches one decoded envelope to its exact name and schema version.
    pub async fn dispatch(&self, message: MessageEnvelope) -> Result<(), DispatchError> {
        let key = (message.name.clone(), message.schema_version);
        let Some((_, handler)) = self.handlers.get(&key) else {
            return Err(DispatchError::NoHandler {
                name: message.name,
                schema_version: message.schema_version,
            });
        };
        handler(message).await.map_err(DispatchError::Handler)
    }

    /// Returns the number of compiled message handlers.
    pub fn len(&self) -> usize {
        self.handlers.len()
    }

    /// Reports whether this target compiled no handlers.
    pub fn is_empty(&self) -> bool {
        self.handlers.is_empty()
    }
}

/// Invalid handler declarations detected before a worker starts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HandlerBuildError {
    /// A handler route has an empty message name.
    EmptyName {
        /// Module that contributed the invalid declaration.
        owner: ModuleId,
    },
    /// A handler route uses schema version zero.
    InvalidVersion {
        /// Module that contributed the invalid declaration.
        owner: ModuleId,
        /// Invalid semantic message name.
        name: String,
    },
    /// Two modules contributed the same message name and schema version.
    Duplicate {
        /// Semantic message name.
        name: String,
        /// Schema version shared by both declarations.
        schema_version: u32,
        /// First contributing module.
        first_owner: ModuleId,
        /// Conflicting contributing module.
        second_owner: ModuleId,
    },
}

impl fmt::Display for HandlerBuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyName { owner } => {
                write!(f, "module {:?} declared an empty message name", owner)
            }
            Self::InvalidVersion { owner, name } => {
                write!(
                    f,
                    "module {:?} declared schema version zero for {name:?}",
                    owner
                )
            }
            Self::Duplicate {
                name,
                schema_version,
                first_owner,
                second_owner,
            } => write!(
                f,
                "modules {:?} and {:?} both handle {name:?} schema version {schema_version}",
                first_owner, second_owner
            ),
        }
    }
}

impl Error for HandlerBuildError {}

/// Failure decoding or routing one message envelope.
#[derive(Debug)]
pub enum DispatchError {
    /// The serialized input is not a valid message envelope.
    Decode(serde_json::Error),
    /// No handler is registered for the exact message name and version.
    NoHandler {
        /// Semantic message name.
        name: String,
        /// Schema version present in the envelope.
        schema_version: u32,
    },
    /// The selected application handler failed.
    Handler(HandlerError),
}

impl fmt::Display for DispatchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Decode(error) => write!(f, "invalid message envelope: {error}"),
            Self::NoHandler {
                name,
                schema_version,
            } => {
                write!(f, "no handler for {name:?} schema version {schema_version}")
            }
            Self::Handler(error) => write!(f, "message handler failed: {error}"),
        }
    }
}

impl Error for DispatchError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Decode(error) => Some(error),
            Self::Handler(error) => Some(error.as_ref()),
            Self::NoHandler { .. } => None,
        }
    }
}
