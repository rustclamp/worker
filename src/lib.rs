//! Worker-owned message handler declarations and routing.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use rustclamp_core::{
    Clock, Contribution, ContributionId, ContributionTarget, ContributionTargetId, ModuleId,
    Qualifier, QualifierId,
};
use rustclamp_messaging::MessageEnvelope;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::collections::{BTreeMap, btree_map::Entry};
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

/// Qualifier for one worker handler target.
pub struct WorkerHandlers;

impl Qualifier for WorkerHandlers {
    const ID: QualifierId = QualifierId::new("rustclamp.worker.handlers");
}

/// Application or infrastructure error returned from one handler attempt.
pub type HandlerError = Box<dyn Error + Send + Sync>;

/// Handler failure classification used to decide whether another attempt is safe.
#[derive(Debug)]
pub enum HandlerFailure {
    /// The operation failed before producing an external side effect.
    Retryable(HandlerError),
    /// The input or operation is permanently invalid.
    Permanent(HandlerError),
    /// A remote side effect may have completed, but its result is unknown.
    UnknownOutcome(HandlerError),
}

impl fmt::Display for HandlerFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Retryable(error) => write!(f, "retryable handler failure: {error}"),
            Self::Permanent(error) => write!(f, "permanent handler failure: {error}"),
            Self::UnknownOutcome(error) => write!(f, "handler outcome is unknown: {error}"),
        }
    }
}

impl Error for HandlerFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Retryable(error) | Self::Permanent(error) | Self::UnknownOutcome(error) => {
                Some(error.as_ref())
            }
        }
    }
}

/// One handler invocation: the message and its 1-based delivery attempt.
#[derive(Clone, Debug)]
pub struct Delivery {
    /// The transport-neutral message being handled.
    pub message: MessageEnvelope,
    /// Which attempt this is, starting at 1. Transports own the counting.
    pub attempt: u32,
}

/// Typed future returned by a message handler: a result value or a classified failure.
pub type HandlerFuture =
    Pin<Box<dyn Future<Output = Result<Value, HandlerFailure>> + Send + 'static>>;

type Handler =
    Arc<dyn Fn(Delivery) -> Result<HandlerFuture, DispatchError> + Send + Sync + 'static>;

type Validator = Arc<dyn Fn(&Value) -> Result<(), serde_json::Error> + Send + Sync + 'static>;

/// A module's declaration that it handles one message name and schema version.
pub struct HandlerDeclaration {
    name: String,
    schema_version: u32,
    handler: Handler,
    validator: Option<Validator>,
}

impl HandlerDeclaration {
    /// Declares a handler over the raw delivery. Return `Value::Null` for no result.
    pub fn new<F, Fut>(name: impl Into<String>, schema_version: u32, handler: F) -> Self
    where
        F: Fn(Delivery) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Value, HandlerFailure>> + Send + 'static,
    {
        Self {
            name: name.into(),
            schema_version,
            handler: Arc::new(move |delivery| Ok(Box::pin(handler(delivery)) as HandlerFuture)),
            validator: None,
        }
    }

    /// Declares a handler whose payload decodes into `P` and whose result serializes from `R`.
    ///
    /// A payload that does not decode fails dispatch with
    /// [`DispatchError::InvalidPayload`] before the handler runs, and
    /// [`HandlerRegistry::validate`] can check a payload without running anything.
    pub fn typed<P, R, F, Fut>(name: impl Into<String>, schema_version: u32, handler: F) -> Self
    where
        P: DeserializeOwned + 'static,
        R: Serialize + 'static,
        F: Fn(P, Delivery) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<R, HandlerFailure>> + Send + 'static,
    {
        let handler = Arc::new(handler);
        Self {
            name: name.into(),
            schema_version,
            handler: Arc::new(move |delivery: Delivery| {
                let payload = P::deserialize(&delivery.message.payload)
                    .map_err(DispatchError::InvalidPayload)?;
                let future = handler(payload, delivery);
                Ok(Box::pin(async move {
                    let result = future.await?;
                    serde_json::to_value(result)
                        .map_err(|error| HandlerFailure::Permanent(Box::new(error)))
                }) as HandlerFuture)
            }),
            validator: Some(Arc::new(|payload| P::deserialize(payload).map(drop))),
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
                    entry.insert(Route {
                        owner: *owner,
                        handler: Arc::clone(&declaration.handler),
                        validator: declaration.validator.clone(),
                    });
                }
                Entry::Occupied(entry) => {
                    return Err(HandlerBuildError::Duplicate {
                        name: declaration.name.clone(),
                        schema_version: declaration.schema_version,
                        first_owner: entry.get().owner,
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
    handlers: BTreeMap<(String, u32), Route>,
}

#[derive(Clone)]
struct Route {
    owner: ModuleId,
    handler: Handler,
    validator: Option<Validator>,
}

impl HandlerRegistry {
    /// Decodes a serialized envelope and dispatches it as the given attempt.
    pub async fn dispatch_json(&self, bytes: &[u8], attempt: u32) -> Result<Value, DispatchError> {
        let message = serde_json::from_slice(bytes).map_err(DispatchError::Decode)?;
        self.dispatch(Delivery { message, attempt }).await
    }

    /// Dispatches one delivery to its exact name and schema version.
    pub async fn dispatch(&self, delivery: Delivery) -> Result<Value, DispatchError> {
        let route = self.route(&delivery.message.name, delivery.message.schema_version)?;
        (route.handler)(delivery)?
            .await
            .map_err(DispatchError::Handler)
    }

    /// Checks that a route exists and, for typed handlers, that `payload` decodes.
    /// Runs no handler; use it to reject a message before enqueueing it.
    pub fn validate(
        &self,
        name: &str,
        schema_version: u32,
        payload: &Value,
    ) -> Result<(), DispatchError> {
        match &self.route(name, schema_version)?.validator {
            Some(validator) => validator(payload).map_err(DispatchError::InvalidPayload),
            None => Ok(()),
        }
    }

    /// Dispatches one delivery and classifies the result under `policy`.
    ///
    /// A message past its `deadline_unix_ms` on `clock` is dead-lettered as
    /// [`DeadReason::Expired`] without running. Handler timeouts are the caller's:
    /// wrap this future in a timer and treat expiry as [`DeadReason::UnknownOutcome`].
    pub async fn deliver(
        &self,
        delivery: Delivery,
        policy: &RetryPolicy,
        clock: &dyn Clock,
    ) -> Outcome {
        if let Some(deadline) = delivery.message.deadline_unix_ms {
            let now = clock.now().duration_since(UNIX_EPOCH).map_or(0, |since| {
                u64::try_from(since.as_millis()).unwrap_or(u64::MAX)
            });
            if now >= deadline {
                return Outcome::DeadLetter {
                    reason: DeadReason::Expired,
                    error: None,
                };
            }
        }
        let attempt = delivery.attempt;
        match self.dispatch(delivery).await {
            Ok(value) => Outcome::Done(value),
            Err(error) => policy.classify(error, attempt),
        }
    }

    fn route(&self, name: &str, schema_version: u32) -> Result<&Route, DispatchError> {
        // ponytail: linear scan avoids allocating a String key; fine for tens of routes.
        self.handlers
            .iter()
            .find(|((route, version), _)| route == name && *version == schema_version)
            .map(|(_, route)| route)
            .ok_or_else(|| DispatchError::NoHandler {
                name: name.to_owned(),
                schema_version,
            })
    }

    /// Reports whether a handler is registered for this exact name and schema version.
    pub fn contains(&self, name: &str, schema_version: u32) -> bool {
        self.handlers
            .keys()
            .any(|(route, version)| route == name && *version == schema_version)
    }

    /// Returns every registered route as `(name, schema_version)`, sorted.
    pub fn routes(&self) -> impl Iterator<Item = (&str, u32)> {
        self.handlers
            .keys()
            .map(|(name, version)| (name.as_str(), *version))
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
    /// The payload does not decode into the typed handler's input.
    InvalidPayload(serde_json::Error),
    /// The selected application handler failed.
    Handler(HandlerFailure),
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
            Self::InvalidPayload(error) => write!(f, "invalid message payload: {error}"),
            Self::Handler(error) => write!(f, "message handler failed: {error}"),
        }
    }
}

impl Error for DispatchError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Decode(error) | Self::InvalidPayload(error) => Some(error),
            Self::Handler(error) => Some(error),
            Self::NoHandler { .. } => None,
        }
    }
}

/// How many attempts a retryable failure gets and how long to wait between them.
#[derive(Clone)]
pub struct RetryPolicy {
    max_attempts: u32,
    backoff: Arc<dyn Fn(u32) -> Duration + Send + Sync>,
}

impl RetryPolicy {
    /// Allows `max_attempts` attempts in total; `backoff(attempt)` is the wait after
    /// failed attempt `attempt` (1-based), e.g. a stepped table.
    pub fn new(
        max_attempts: u32,
        backoff: impl Fn(u32) -> Duration + Send + Sync + 'static,
    ) -> Self {
        Self {
            max_attempts,
            backoff: Arc::new(backoff),
        }
    }

    /// `base × attempt` backoff.
    pub fn linear(max_attempts: u32, base: Duration) -> Self {
        Self::new(max_attempts, move |attempt| base.saturating_mul(attempt))
    }

    /// Maps a failed attempt to a retry or a dead letter.
    pub fn classify(&self, error: DispatchError, attempt: u32) -> Outcome {
        let reason = match &error {
            DispatchError::Handler(HandlerFailure::Retryable(_)) if attempt < self.max_attempts => {
                return Outcome::Retry((self.backoff)(attempt));
            }
            DispatchError::Handler(HandlerFailure::Retryable(_)) => DeadReason::RetryExhausted,
            DispatchError::Handler(HandlerFailure::Permanent(_)) => DeadReason::Permanent,
            DispatchError::Handler(HandlerFailure::UnknownOutcome(_)) => DeadReason::UnknownOutcome,
            DispatchError::NoHandler { .. } => DeadReason::NoHandler,
            DispatchError::InvalidPayload(_) | DispatchError::Decode(_) => {
                DeadReason::InvalidPayload
            }
        };
        Outcome::DeadLetter {
            reason,
            error: Some(error),
        }
    }
}

impl fmt::Debug for RetryPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RetryPolicy")
            .field("max_attempts", &self.max_attempts)
            .finish_non_exhaustive()
    }
}

/// What a transport should do with one delivery after an attempt.
#[derive(Debug)]
pub enum Outcome {
    /// Acknowledge; the handler's result value.
    Done(Value),
    /// Redeliver after this delay.
    Retry(Duration),
    /// Stop delivering; move to the transport's dead-letter store.
    DeadLetter {
        /// Why no further attempt is made.
        reason: DeadReason,
        /// The failure, when one was produced (`None` for [`DeadReason::Expired`]).
        error: Option<DispatchError>,
    },
}

/// Why a delivery was dead-lettered.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeadReason {
    /// No handler for the name and schema version.
    NoHandler,
    /// The envelope or payload does not decode.
    InvalidPayload,
    /// The handler reported a permanent failure.
    Permanent,
    /// Retryable failures used up the policy's attempts.
    RetryExhausted,
    /// A side effect may have happened; retrying is unsafe.
    UnknownOutcome,
    /// The message's deadline passed before it ran.
    Expired,
}
