//! Tests for the SQLite queue and outbox.

#![cfg(feature = "sqlite")]

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, UNIX_EPOCH};

use rusqlite::Connection;
use rustclamp_core::{ContributionTarget, ManualClock, ModuleId, SystemClock};
use rustclamp_messaging::MessageEnvelope;
use rustclamp_worker::service::{
    Blocking, BlockingTransport, Claim, ServiceConfig, Settlement, WorkerService,
};
use rustclamp_worker::sqlite::{SqliteQueue, enqueue};
use rustclamp_worker::{DeadReason, HandlerDeclaration, HandlerTarget, RetryPolicy};
use serde_json::json;

/// A database file removed on drop, so a second connection can inspect it.
struct TempDb(PathBuf);

impl TempDb {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let n = NEXT.fetch_add(1, Ordering::SeqCst);
        Self(std::env::temp_dir().join(format!("worker-sqlite-{}-{n}.db", std::process::id())))
    }

    fn open(&self) -> Connection {
        Connection::open(&self.0).unwrap()
    }

    fn states(&self) -> Vec<(String, u32)> {
        let conn = self.open();
        let mut statement = conn
            .prepare("SELECT state, attempts FROM worker_jobs ORDER BY id")
            .unwrap();
        statement
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .map(Result::unwrap)
            .collect()
    }
}

impl Drop for TempDb {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

fn message(id: &str) -> MessageEnvelope {
    let mut message = MessageEnvelope::new("echo", 1, json!({ "text": "hi" }));
    message.id = id.into();
    message
}

fn at(seconds: u64) -> std::time::SystemTime {
    UNIX_EPOCH + Duration::from_secs(seconds)
}

fn ids(claims: Vec<Claim<i64>>) -> Vec<String> {
    claims
        .into_iter()
        .map(|claim| match claim {
            Claim::Message { message, .. } => message.id,
            Claim::Malformed { id, .. } => format!("malformed {id}"),
        })
        .collect()
}

#[test]
fn claims_only_due_jobs_on_the_injected_clock() {
    let db = TempDb::new();
    let clock = Arc::new(ManualClock::new(at(100)));
    let mut queue = SqliteQueue::new(db.open(), clock.clone()).unwrap();
    let conn = db.open();
    enqueue(&conn, &message("later"), at(200), None).unwrap();
    enqueue(&conn, &message("now"), at(100), None).unwrap();

    assert_eq!(ids(queue.claim(10).unwrap()), ["now"]);
    assert!(
        queue.claim(10).unwrap().is_empty(),
        "claimed jobs stay hidden"
    );
    clock.advance(Duration::from_secs(100));
    assert_eq!(ids(queue.claim(10).unwrap()), ["later"]);
}

#[test]
fn a_dedupe_key_admits_one_job() {
    let db = TempDb::new();
    let queue = SqliteQueue::new(db.open(), Arc::new(SystemClock)).unwrap();
    let conn = db.open();
    assert!(enqueue(&conn, &message("a"), at(0), Some("order-1")).unwrap());
    assert!(!enqueue(&conn, &message("b"), at(0), Some("order-1")).unwrap());
    assert!(enqueue(&conn, &message("c"), at(0), None).unwrap());
    assert!(enqueue(&conn, &message("d"), at(0), None).unwrap());
    assert_eq!(db.states().len(), 3);
    drop(queue);
}

#[test]
fn settlements_record_state_and_attempts_and_release_requeues() {
    let db = TempDb::new();
    let mut queue = SqliteQueue::new(db.open(), Arc::new(SystemClock)).unwrap();
    let conn = db.open();
    for id in ["a", "b", "c"] {
        enqueue(&conn, &message(id), at(0), None).unwrap();
    }
    let receipts: Vec<i64> = queue
        .claim(3)
        .unwrap()
        .into_iter()
        .map(|claim| match claim {
            Claim::Message { receipt, .. } => receipt,
            Claim::Malformed { .. } => unreachable!(),
        })
        .collect();
    queue
        .settle(
            receipts[0],
            Settlement::Done {
                result: json!(1),
                attempts: 2,
                message: message("a"),
            },
        )
        .unwrap();
    queue
        .settle(
            receipts[1],
            Settlement::DeadLetter {
                reason: DeadReason::Permanent,
                attempts: 1,
                error: Some("no".into()),
                message: Some(message("b")),
            },
        )
        .unwrap();
    queue.settle(receipts[2], Settlement::Release).unwrap();

    assert_eq!(
        db.states(),
        [
            ("done".into(), 2),
            ("dead".into(), 1),
            ("pending".into(), 0)
        ]
    );
    assert_eq!(ids(queue.claim(10).unwrap()), ["c"]);
}

#[test]
fn recover_returns_claimed_jobs_a_crashed_run_left() {
    let db = TempDb::new();
    let mut first = SqliteQueue::new(db.open(), Arc::new(SystemClock)).unwrap();
    enqueue(&db.open(), &message("a"), at(0), None).unwrap();
    assert_eq!(first.claim(1).unwrap().len(), 1);
    drop(first); // crash: the row stays claimed

    let mut second = SqliteQueue::new(db.open(), Arc::new(SystemClock)).unwrap();
    assert!(second.claim(1).unwrap().is_empty());
    second.recover().unwrap();
    assert_eq!(ids(second.claim(1).unwrap()), ["a"]);
}

#[test]
fn a_rolled_back_transaction_leaves_no_job() {
    let db = TempDb::new();
    let _queue = SqliteQueue::new(db.open(), Arc::new(SystemClock)).unwrap();
    let mut conn = db.open();
    let transaction = conn.transaction().unwrap();
    enqueue(&transaction, &message("a"), at(0), None).unwrap();
    transaction.rollback().unwrap();
    let transaction = conn.transaction().unwrap();
    enqueue(&transaction, &message("b"), at(0), None).unwrap();
    transaction.commit().unwrap();
    assert_eq!(db.states(), [("pending".into(), 0)]);
}

#[test]
fn an_undecodable_row_is_malformed() {
    let db = TempDb::new();
    let mut queue = SqliteQueue::new(db.open(), Arc::new(SystemClock)).unwrap();
    db.open()
        .execute(
            "INSERT INTO worker_jobs (message_id, message, run_at) VALUES ('x', 'nope', 0)",
            [],
        )
        .unwrap();
    assert_eq!(ids(queue.claim(1).unwrap()), ["malformed x"]);
}

#[test]
fn the_service_runs_jobs_from_the_queue() {
    let db = TempDb::new();
    let queue = SqliteQueue::new(db.open(), Arc::new(SystemClock)).unwrap();
    let conn = db.open();
    for id in ["a", "b"] {
        enqueue(&conn, &message(id), at(0), None).unwrap();
    }
    let registry =
        HandlerTarget
            .build(&[(
                ModuleId::new("test.module.echo"),
                HandlerDeclaration::new("echo", 1, |delivery| async move {
                    Ok(delivery.message.payload)
                }),
            )])
            .unwrap();
    let config = ServiceConfig {
        concurrency: 2,
        capacity: 2,
        poll: Duration::from_millis(5),
        drain_timeout: Duration::from_secs(1),
        handler_timeout: None,
        retry: RetryPolicy::linear(1, Duration::ZERO),
    };
    let service = WorkerService::new(registry, Blocking::new(queue), config);
    let tokio = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    tokio
        .block_on(async move {
            service
                .run(tokio::time::sleep(Duration::from_millis(200)))
                .await
        })
        .unwrap();
    assert_eq!(db.states(), [("done".into(), 1), ("done".into(), 1)]);
}
