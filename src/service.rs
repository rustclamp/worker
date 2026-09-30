//! Transport-driven worker loop (feature `service`).
//!
//! [`WorkerService`] owns the parts every worker re-implements: claiming with
//! backpressure, bounded concurrency, in-process retries under a
//! [`RetryPolicy`], dead-lettering, a handler timeout, and a graceful drain
//! that cancels what is still running when the drain timeout expires. The
//! application supplies a [`Transport`] (where messages live and how they are
//! settled) and reacts to [`ServiceEvent`]s.

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::io;
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::time::Duration;

use rustclamp_core::{Clock, SystemClock};
use rustclamp_messaging::MessageEnvelope;
use rustclamp_runtime::CancellationToken;
use serde_json::Value;
use tokio::sync::mpsc;
use tokio::time::{Instant, MissedTickBehavior, interval, sleep, sleep_until, timeout};

use crate::{DeadReason, Delivery, HandlerRegistry, Outcome, RetryPolicy};

/// Where messages come from and how their final state is recorded.
///
/// Methods run on the service's own task, so neither the transport nor its
/// receipts need to be `Send`.
pub trait Transport {
    /// Handle identifying one claimed message to its transport (a file name, a broker ack token).
    type Receipt;

    /// Claims up to `limit` messages. Claimed messages stay invisible to other
    /// consumers until settled.
    fn claim(
        &mut self,
        limit: usize,
    ) -> impl Future<Output = io::Result<Vec<Claim<Self::Receipt>>>>;

    /// Records the final state of one claimed message.
    fn settle(
        &mut self,
        receipt: Self::Receipt,
        settlement: Settlement,
    ) -> impl Future<Output = io::Result<()>>;
}

/// One claimed message, or one the transport could not decode.
#[derive(Debug)]
pub enum Claim<R> {
    /// A decoded message ready for dispatch.
    Message {
        /// Transport handle for settling.
        receipt: R,
        /// The message.
        message: MessageEnvelope,
    },
    /// A claimed item that is not a valid message; it is dead-lettered as
    /// [`DeadReason::Malformed`] without running.
    Malformed {
        /// Transport handle for settling.
        receipt: R,
        /// Identity reported in events (e.g. a file stem).
        id: String,
        /// Why decoding failed.
        error: String,
    },
}

/// What the transport should do with a claimed message.
#[derive(Debug)]
pub enum Settlement {
    /// Completed; the handler's result.
    Done {
        /// Handler result value.
        result: Value,
        /// Attempts used, including the successful one.
        attempts: u32,
    },
    /// Never deliver again; store it for inspection.
    DeadLetter {
        /// Why.
        reason: DeadReason,
        /// Attempts made (0 when dead-lettered at claim).
        attempts: u32,
        /// The failure, when there was one.
        error: Option<String>,
    },
    /// Make it available again with its attempt uncounted (shutdown).
    Release,
}

/// Limits and timings for a [`WorkerService`].
#[derive(Clone, Debug)]
pub struct ServiceConfig {
    /// Maximum handler attempts running at once.
    pub concurrency: usize,
    /// Claimed messages allowed beyond `concurrency` (queued or backing off).
    pub capacity: usize,
    /// How often to claim.
    pub poll: Duration,
    /// How long running attempts may finish after shutdown before they are cancelled.
    pub drain_timeout: Duration,
    /// Per-attempt limit; an attempt that exceeds it is dead-lettered as
    /// [`DeadReason::UnknownOutcome`], since its side effects are unknown.
    pub handler_timeout: Option<Duration>,
    /// Attempts and backoff for retryable failures.
    pub retry: RetryPolicy,
}

/// Progress reported to the service's observer.
#[derive(Clone, Copy, Debug)]
pub enum ServiceEvent<'a> {
    /// An attempt started.
    Started {
        /// Message id.
        id: &'a str,
        /// 1-based attempt.
        attempt: u32,
    },
    /// An attempt failed retryably; the next one starts after `delay`
    /// (unless shutdown releases the message first).
    Retrying {
        /// Message id.
        id: &'a str,
        /// The attempt that failed.
        attempt: u32,
        /// Backoff before the next attempt.
        delay: Duration,
    },
    /// The message completed.
    Done {
        /// Message id.
        id: &'a str,
        /// Attempts used.
        attempts: u32,
    },
    /// The message was dead-lettered.
    DeadLettered {
        /// Message id (or the transport's id for malformed items).
        id: &'a str,
        /// Attempts made.
        attempts: u32,
        /// Why.
        reason: DeadReason,
    },
}

/// Live counters, readable while the service runs (e.g. from a heartbeat).
#[derive(Debug, Default)]
pub struct ServiceStats {
    claimed: AtomicUsize,
    executing: AtomicUsize,
    done: AtomicUsize,
    dead: AtomicUsize,
}

/// A point-in-time copy of [`ServiceStats`].
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StatsSnapshot {
    /// Claimed and not yet settled (queued, running or backing off).
    pub claimed: usize,
    /// Attempts running now.
    pub executing: usize,
    /// Completed since start.
    pub done: usize,
    /// Dead-lettered since start.
    pub dead: usize,
}

impl ServiceStats {
    /// Reads all counters.
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            claimed: self.claimed.load(Relaxed),
            executing: self.executing.load(Relaxed),
            done: self.done.load(Relaxed),
            dead: self.dead.load(Relaxed),
        }
    }
}

/// How a run ended.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ServiceReport {
    /// Messages released back to the transport at shutdown.
    pub returned: usize,
    /// The subset of `returned` that was running when the drain timeout cancelled it.
    pub cancelled: usize,
    /// Whether every running attempt finished within the drain timeout.
    pub drained: bool,
}

type Observer = Box<dyn FnMut(&ServiceEvent<'_>)>;

/// Runs handlers from a [`HandlerRegistry`] over messages from a [`Transport`].
pub struct WorkerService<T: Transport> {
    registry: Arc<HandlerRegistry>,
    transport: T,
    config: ServiceConfig,
    clock: Arc<dyn Clock + Send + Sync>,
    stats: Arc<ServiceStats>,
    observer: Observer,
}

struct Job<R> {
    receipt: R,
    message: MessageEnvelope,
    attempt: u32,
}

enum Signal {
    /// `None`: cancelled by the drain timeout.
    Finished(u64, Option<Outcome>),
    BackoffElapsed(u64),
}

struct State<R> {
    next_slot: u64,
    queue: VecDeque<u64>,
    jobs: HashMap<u64, Job<R>>,
    backing_off: Vec<u64>,
    stopping: bool,
    report: ServiceReport,
    root: CancellationToken,
    tx: mpsc::UnboundedSender<Signal>,
}

impl<T: Transport> WorkerService<T> {
    /// Creates a service using the system clock for message deadlines.
    pub fn new(registry: HandlerRegistry, transport: T, config: ServiceConfig) -> Self {
        Self {
            registry: Arc::new(registry),
            transport,
            config,
            clock: Arc::new(SystemClock),
            stats: Arc::default(),
            observer: Box::new(|_| {}),
        }
    }

    /// Calls `observer` for every [`ServiceEvent`], on the service's task.
    #[must_use]
    pub fn on_event(mut self, observer: impl FnMut(&ServiceEvent<'_>) + 'static) -> Self {
        self.observer = Box::new(observer);
        self
    }

    /// Shared live counters.
    pub fn stats(&self) -> Arc<ServiceStats> {
        Arc::clone(&self.stats)
    }

    /// Claims and runs messages until `shutdown` completes, then releases
    /// waiting messages and drains running ones.
    ///
    /// Must run inside a Tokio runtime; handler attempts are spawned onto it.
    pub async fn run(mut self, shutdown: impl Future<Output = ()>) -> io::Result<ServiceReport> {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut state = State {
            next_slot: 0,
            queue: VecDeque::new(),
            jobs: HashMap::new(),
            backing_off: Vec::new(),
            stopping: false,
            report: ServiceReport {
                returned: 0,
                cancelled: 0,
                drained: true,
            },
            root: CancellationToken::default(),
            tx,
        };
        let mut shutdown = pin!(shutdown);
        let mut poll = interval(self.config.poll);
        poll.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                () = &mut shutdown => break,
                _ = poll.tick() => self.intake(&mut state).await?,
                Some(signal) = rx.recv() => self.receive(&mut state, signal).await?,
            }
            self.fill(&mut state);
        }

        state.stopping = true;
        let waiting = state
            .queue
            .drain(..)
            .chain(state.backing_off.drain(..))
            .collect::<Vec<_>>();
        for slot in waiting {
            self.release(&mut state, slot).await?;
        }
        let deadline = Instant::now() + self.config.drain_timeout;
        while self.stats.executing.load(Relaxed) > 0 {
            tokio::select! {
                Some(signal) = rx.recv() => self.receive(&mut state, signal).await?,
                () = sleep_until(deadline), if state.report.drained => {
                    state.report.drained = false;
                    state.root.cancel();
                }
            }
        }
        Ok(state.report)
    }

    async fn intake(&mut self, state: &mut State<T::Receipt>) -> io::Result<()> {
        let limit = self.config.concurrency + self.config.capacity;
        let room = limit.saturating_sub(self.stats.claimed.load(Relaxed));
        if room == 0 {
            return Ok(());
        }
        for claim in self.transport.claim(room).await? {
            match claim {
                Claim::Malformed { receipt, id, error } => {
                    self.dead_letter(receipt, &id, DeadReason::Malformed, 0, Some(error))
                        .await?;
                }
                Claim::Message { receipt, message }
                    if !self
                        .registry
                        .contains(&message.name, message.schema_version) =>
                {
                    let error = format!(
                        "no handler for {:?} schema version {}",
                        message.name, message.schema_version
                    );
                    self.dead_letter(receipt, &message.id, DeadReason::NoHandler, 0, Some(error))
                        .await?;
                }
                Claim::Message { receipt, message } => {
                    self.stats.claimed.fetch_add(1, Relaxed);
                    let slot = state.next_slot;
                    state.next_slot += 1;
                    state.jobs.insert(
                        slot,
                        Job {
                            receipt,
                            message,
                            attempt: 1,
                        },
                    );
                    state.queue.push_back(slot);
                }
            }
        }
        Ok(())
    }

    fn fill(&mut self, state: &mut State<T::Receipt>) {
        while self.stats.executing.load(Relaxed) < self.config.concurrency {
            let Some(slot) = state.queue.pop_front() else {
                break;
            };
            let job = &state.jobs[&slot];
            (self.observer)(&ServiceEvent::Started {
                id: &job.message.id,
                attempt: job.attempt,
            });
            self.stats.executing.fetch_add(1, Relaxed);
            let delivery = Delivery {
                message: job.message.clone(),
                attempt: job.attempt,
            };
            let (registry, clock, policy) = (
                Arc::clone(&self.registry),
                Arc::clone(&self.clock),
                self.config.retry.clone(),
            );
            let (root, tx, limit) = (
                state.root.clone(),
                state.tx.clone(),
                self.config.handler_timeout,
            );
            tokio::spawn(async move {
                let attempt = async {
                    let outcome = registry.deliver(delivery, &policy, &*clock);
                    match limit {
                        Some(limit) => {
                            timeout(limit, outcome)
                                .await
                                .unwrap_or(Outcome::DeadLetter {
                                    reason: DeadReason::UnknownOutcome,
                                    error: None,
                                })
                        }
                        None => outcome.await,
                    }
                };
                let outcome = tokio::select! {
                    () = root.cancelled() => None,
                    outcome = attempt => Some(outcome),
                };
                let _ = tx.send(Signal::Finished(slot, outcome));
            });
        }
    }

    async fn receive(&mut self, state: &mut State<T::Receipt>, signal: Signal) -> io::Result<()> {
        let (slot, outcome) = match signal {
            Signal::BackoffElapsed(slot) => {
                if let Some(index) = state
                    .backing_off
                    .iter()
                    .position(|waiting| *waiting == slot)
                {
                    state.backing_off.swap_remove(index);
                    state.queue.push_back(slot);
                }
                return Ok(());
            }
            Signal::Finished(slot, outcome) => (slot, outcome),
        };
        self.stats.executing.fetch_sub(1, Relaxed);
        let Some(outcome) = outcome else {
            state.report.cancelled += 1;
            return self.release(state, slot).await;
        };
        match outcome {
            Outcome::Retry(delay) => {
                let job = state.jobs.get_mut(&slot).expect("running job is tracked");
                (self.observer)(&ServiceEvent::Retrying {
                    id: &job.message.id,
                    attempt: job.attempt,
                    delay,
                });
                job.attempt += 1;
                if state.stopping {
                    return self.release(state, slot).await;
                }
                state.backing_off.push(slot);
                let tx = state.tx.clone();
                tokio::spawn(async move {
                    sleep(delay).await;
                    let _ = tx.send(Signal::BackoffElapsed(slot));
                });
            }
            Outcome::Done(result) => {
                let job = state.jobs.remove(&slot).expect("running job is tracked");
                self.transport
                    .settle(
                        job.receipt,
                        Settlement::Done {
                            result,
                            attempts: job.attempt,
                        },
                    )
                    .await?;
                self.stats.claimed.fetch_sub(1, Relaxed);
                self.stats.done.fetch_add(1, Relaxed);
                (self.observer)(&ServiceEvent::Done {
                    id: &job.message.id,
                    attempts: job.attempt,
                });
            }
            Outcome::DeadLetter { reason, error } => {
                let job = state.jobs.remove(&slot).expect("running job is tracked");
                self.stats.claimed.fetch_sub(1, Relaxed);
                let error = error.map(|error| error.to_string());
                self.dead_letter(job.receipt, &job.message.id, reason, job.attempt, error)
                    .await?;
            }
        }
        Ok(())
    }

    async fn release(&mut self, state: &mut State<T::Receipt>, slot: u64) -> io::Result<()> {
        let job = state.jobs.remove(&slot).expect("claimed job is tracked");
        self.transport
            .settle(job.receipt, Settlement::Release)
            .await?;
        self.stats.claimed.fetch_sub(1, Relaxed);
        state.report.returned += 1;
        Ok(())
    }

    async fn dead_letter(
        &mut self,
        receipt: T::Receipt,
        id: &str,
        reason: DeadReason,
        attempts: u32,
        error: Option<String>,
    ) -> io::Result<()> {
        self.transport
            .settle(
                receipt,
                Settlement::DeadLetter {
                    reason,
                    attempts,
                    error,
                },
            )
            .await?;
        self.stats.dead.fetch_add(1, Relaxed);
        (self.observer)(&ServiceEvent::DeadLettered {
            id,
            attempts,
            reason,
        });
        Ok(())
    }
}
