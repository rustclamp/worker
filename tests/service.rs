//! Tests for the transport-driven worker loop.

#![cfg(feature = "service")]

use std::cell::{Cell, RefCell};
use std::io;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use rustclamp_core::{ContributionTarget, ModuleId};
use rustclamp_messaging::MessageEnvelope;
use rustclamp_worker::service::{
    Claim, ServiceConfig, ServiceEvent, ServiceReport, Settlement, Transport, WorkerService,
};
use rustclamp_worker::{
    Delivery, HandlerDeclaration, HandlerFailure, HandlerRegistry, HandlerTarget, RetryPolicy,
};
use serde::Deserialize;
use serde_json::{Value, json};

const OWNER: ModuleId = ModuleId::new("test.module.jobs");

enum Item {
    Message(MessageEnvelope),
    Garbage(&'static str),
}

/// Receipts are indexes into `items`; every settlement and the peak number of
/// unsettled claims are recorded.
#[derive(Default)]
struct Memory {
    items: Vec<Option<Item>>,
    next: usize,
    unsettled: usize,
    peak_unsettled: Rc<Cell<usize>>,
    settled: Rc<RefCell<Vec<(usize, String)>>>,
}

impl Transport for Memory {
    type Receipt = usize;

    async fn claim(&mut self, limit: usize) -> io::Result<Vec<Claim<usize>>> {
        let mut claims = Vec::new();
        while claims.len() < limit && self.next < self.items.len() {
            let receipt = self.next;
            self.next += 1;
            claims.push(match self.items[receipt].take().expect("claimed once") {
                Item::Message(message) => Claim::Message { receipt, message },
                Item::Garbage(id) => Claim::Malformed {
                    receipt,
                    id: id.into(),
                    error: "not json".into(),
                },
            });
        }
        self.unsettled += claims.len();
        self.peak_unsettled
            .set(self.peak_unsettled.get().max(self.unsettled));
        Ok(claims)
    }

    async fn settle(&mut self, receipt: usize, settlement: Settlement) -> io::Result<()> {
        self.unsettled -= 1;
        let label = match settlement {
            Settlement::Done { result, attempts } => format!("done {attempts} {result}"),
            Settlement::DeadLetter {
                reason, attempts, ..
            } => format!("dead {attempts} {reason:?}"),
            Settlement::Release => "release".into(),
        };
        self.settled.borrow_mut().push((receipt, label));
        Ok(())
    }
}

fn message(id: &str, name: &str, payload: Value) -> Item {
    Item::Message(MessageEnvelope {
        id: id.into(),
        name: name.into(),
        schema_version: 1,
        correlation_id: id.into(),
        causation_id: None,
        deadline_unix_ms: None,
        payload,
    })
}

#[derive(Deserialize)]
struct Sleep {
    ms: u64,
}

#[derive(Deserialize)]
struct Flaky {
    fail_times: u32,
    permanent: bool,
}

fn registry(running: Arc<AtomicUsize>, peak: Arc<AtomicUsize>) -> HandlerRegistry {
    HandlerTarget
        .build(&[
            (
                OWNER,
                HandlerDeclaration::typed("sleep", 1, move |sleep: Sleep, _| {
                    let (running, peak) = (running.clone(), peak.clone());
                    async move {
                        let now = running.fetch_add(1, Ordering::SeqCst) + 1;
                        peak.fetch_max(now, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(sleep.ms)).await;
                        running.fetch_sub(1, Ordering::SeqCst);
                        Ok(json!({ "slept": sleep.ms }))
                    }
                }),
            ),
            (
                OWNER,
                HandlerDeclaration::typed(
                    "flaky",
                    1,
                    |flaky: Flaky, delivery: Delivery| async move {
                        if delivery.attempt > flaky.fail_times {
                            return Ok(delivery.attempt);
                        }
                        Err(if flaky.permanent {
                            HandlerFailure::Permanent("no".into())
                        } else {
                            HandlerFailure::Retryable("again".into())
                        })
                    },
                ),
            ),
        ])
        .unwrap()
}

fn config() -> ServiceConfig {
    ServiceConfig {
        concurrency: 2,
        capacity: 1,
        poll: Duration::from_millis(1),
        drain_timeout: Duration::from_secs(5),
        handler_timeout: None,
        retry: RetryPolicy::linear(3, Duration::from_millis(1)),
    }
}

struct Run {
    report: ServiceReport,
    settled: Vec<(usize, String)>,
    events: Vec<String>,
    peak_running: usize,
    peak_unsettled: usize,
}

/// Runs the service over `items`, shutting down after `stop_after`.
fn run(items: Vec<Item>, config: ServiceConfig, stop_after: Duration) -> Run {
    let tokio = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    let (running, peak_running) = (Arc::default(), Arc::<AtomicUsize>::default());
    let settled = Rc::new(RefCell::new(Vec::new()));
    let events = Rc::new(RefCell::new(Vec::new()));
    let peak_unsettled = Rc::new(Cell::new(0));
    let transport = Memory {
        items: items.into_iter().map(Some).collect(),
        settled: settled.clone(),
        peak_unsettled: peak_unsettled.clone(),
        ..Memory::default()
    };
    let log = events.clone();
    let registry = registry(running, peak_running.clone());
    let service = WorkerService::new(registry, transport, config).on_event(move |event| {
        log.borrow_mut().push(match event {
            ServiceEvent::Started { id, attempt } => format!("start {id} {attempt}"),
            ServiceEvent::Retrying { id, attempt, .. } => format!("retry {id} {attempt}"),
            ServiceEvent::Done { id, attempts } => format!("done {id} {attempts}"),
            ServiceEvent::DeadLettered {
                id,
                attempts,
                reason,
            } => {
                format!("dead {id} {attempts} {reason:?}")
            }
        });
    });
    let report = tokio.block_on(async move { service.run(tokio::time::sleep(stop_after)).await });
    Run {
        report: report.unwrap(),
        settled: settled.take(),
        events: events.take(),
        peak_running: peak_running.load(Ordering::SeqCst),
        peak_unsettled: peak_unsettled.get(),
    }
}

#[test]
fn outcomes_are_settled_and_reported_in_order() {
    let run = run(
        vec![
            message("a", "flaky", json!({"fail_times": 2, "permanent": false})),
            message("b", "flaky", json!({"fail_times": 1, "permanent": true})),
            message("c", "missing", json!({})),
            Item::Garbage("d"),
            message("e", "flaky", json!({"fail_times": 9, "permanent": false})),
            message("f", "sleep", json!({"ms": "soon"})),
        ],
        ServiceConfig {
            concurrency: 8,
            capacity: 8,
            ..config()
        },
        Duration::from_millis(300),
    );
    let settled = |receipt| {
        run.settled
            .iter()
            .find(|(r, _)| *r == receipt)
            .unwrap()
            .1
            .clone()
    };
    assert_eq!(settled(0), "done 3 3");
    assert_eq!(settled(1), "dead 1 Permanent");
    assert_eq!(settled(2), "dead 0 NoHandler");
    assert_eq!(settled(3), "dead 0 Malformed");
    assert_eq!(settled(4), "dead 3 RetryExhausted");
    assert_eq!(settled(5), "dead 1 InvalidPayload");

    let events = |id: &str| {
        run.events
            .iter()
            .filter(|event| event.split(' ').nth(1) == Some(id))
            .cloned()
            .collect::<Vec<_>>()
    };
    assert_eq!(
        events("a"),
        [
            "start a 1",
            "retry a 1",
            "start a 2",
            "retry a 2",
            "start a 3",
            "done a 3"
        ]
    );
    assert_eq!(events("c"), ["dead c 0 NoHandler"]);
    assert_eq!(events("d"), ["dead d 0 Malformed"]);
    assert_eq!(events("f"), ["start f 1", "dead f 1 InvalidPayload"]);
    assert_eq!(
        run.report,
        ServiceReport {
            returned: 0,
            cancelled: 0,
            drained: true
        }
    );
}

#[test]
fn concurrency_and_backpressure_hold() {
    let items = (0..12)
        .map(|n| message(&format!("s{n}"), "sleep", json!({"ms": 20})))
        .collect();
    let run = run(items, config(), Duration::from_millis(400));
    assert_eq!(run.peak_running, 2);
    assert_eq!(run.peak_unsettled, 3, "concurrency 2 + capacity 1");
    assert_eq!(run.settled.len(), 12);
    assert!(
        run.settled
            .iter()
            .all(|(_, label)| label.starts_with("done 1"))
    );
}

#[test]
fn drain_timeout_cancels_running_and_releases_everything_else() {
    let items = (0..6)
        .map(|n| message(&format!("s{n}"), "sleep", json!({"ms": 10_000})))
        .collect();
    let run = run(
        items,
        ServiceConfig {
            drain_timeout: Duration::from_millis(50),
            ..config()
        },
        Duration::from_millis(50),
    );
    assert_eq!(
        run.report,
        ServiceReport {
            returned: 3,
            cancelled: 2,
            drained: false
        }
    );
    assert_eq!(run.settled.len(), 3);
    assert!(run.settled.iter().all(|(_, label)| label == "release"));
}

#[test]
fn a_clean_shutdown_drains_running_attempts() {
    let items = (0..2)
        .map(|n| message(&format!("s{n}"), "sleep", json!({"ms": 100})))
        .collect();
    let run = run(items, config(), Duration::from_millis(30));
    assert_eq!(
        run.report,
        ServiceReport {
            returned: 0,
            cancelled: 0,
            drained: true
        }
    );
    assert!(
        run.settled
            .iter()
            .all(|(_, label)| label.starts_with("done"))
    );
}

#[test]
fn an_attempt_over_the_handler_timeout_has_an_unknown_outcome() {
    let run = run(
        vec![message("slow", "sleep", json!({"ms": 10_000}))],
        ServiceConfig {
            handler_timeout: Some(Duration::from_millis(20)),
            ..config()
        },
        Duration::from_millis(200),
    );
    assert_eq!(run.settled, [(0, "dead 1 UnknownOutcome".to_owned())]);
}
