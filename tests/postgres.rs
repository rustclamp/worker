//! Tests for the Postgres queue and outbox against a real server.
//!
//! They run when `RUSTCLAMP_TEST_DATABASE_URL` is set (as in rustclamp's CI and
//! rustclamp-postgres's `compose.yaml`) and pass without doing anything when it
//! is not. Each test works in a schema of its own, dropped at the end.

#![cfg(feature = "postgres")]

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, UNIX_EPOCH};

use rustclamp_core::{ContributionTarget, ManualClock, ModuleId, SystemClock};
use rustclamp_messaging::MessageEnvelope;
use rustclamp_postgres::sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use rustclamp_postgres::sqlx::{self, PgPool};
use rustclamp_postgres::{Migrations, Primary};
use rustclamp_worker::postgres::{PgQueue, enqueue, migration};
use rustclamp_worker::service::{Claim, ServiceConfig, Settlement, Transport, WorkerService};
use rustclamp_worker::{
    DeadReason, HandlerDeclaration, HandlerFailure, HandlerTarget, RetryPolicy,
};
use serde_json::json;

const VISIBILITY: Duration = Duration::from_secs(30);

/// A pool whose connections all use a fresh schema holding `worker_jobs`.
struct TestDb {
    pool: PgPool,
    admin: PgPool,
    schema: String,
}

impl TestDb {
    async fn new() -> Option<Self> {
        let url = std::env::var("RUSTCLAMP_TEST_DATABASE_URL").ok()?;
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let schema = format!(
            "worker_test_{}_{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        );
        let admin = PgPool::connect(&url).await.unwrap();
        sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
            "DROP SCHEMA IF EXISTS {schema} CASCADE; CREATE SCHEMA {schema}"
        )))
        .execute(&admin)
        .await
        .unwrap();
        let options: PgConnectOptions = url.parse().unwrap();
        let pool = PgPoolOptions::new()
            .max_connections(16)
            .connect_with(options.options([("search_path", schema.as_str())]))
            .await
            .unwrap();
        Migrations::<Primary>::new(vec![migration(1)])
            .unwrap()
            .run(&pool)
            .await
            .unwrap();
        Some(Self {
            pool,
            admin,
            schema,
        })
    }

    fn queue(&self, clock: Arc<dyn rustclamp_core::Clock>) -> PgQueue {
        PgQueue::new(self.pool.clone(), clock, VISIBILITY)
    }

    async fn states(&self) -> Vec<(String, i64)> {
        sqlx::query_as("SELECT state, attempts FROM worker_jobs ORDER BY id")
            .fetch_all(&self.pool)
            .await
            .unwrap()
    }

    async fn enqueue(&self, id: &str, seconds: u64) {
        let mut conn = self.pool.acquire().await.unwrap();
        enqueue(&mut conn, &message(id), at(seconds), None)
            .await
            .unwrap();
    }

    async fn drop(self) {
        self.pool.close().await;
        sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
            "DROP SCHEMA {} CASCADE",
            self.schema
        )))
        .execute(&self.admin)
        .await
        .unwrap();
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

type Receipt = (i64, i64);

fn ids(claims: &[Claim<Receipt>]) -> Vec<String> {
    claims
        .iter()
        .map(|claim| match claim {
            Claim::Message { message, .. } => message.id.clone(),
            Claim::Malformed { id, .. } => format!("malformed {id}"),
        })
        .collect()
}

fn receipts(claims: Vec<Claim<Receipt>>) -> Vec<Receipt> {
    claims
        .into_iter()
        .map(|claim| match claim {
            Claim::Message { receipt, .. } | Claim::Malformed { receipt, .. } => receipt,
        })
        .collect()
}

#[tokio::test]
async fn claims_only_due_jobs_on_the_injected_clock() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let clock = Arc::new(ManualClock::new(at(100)));
    // A lease longer than the clock moves here, so only due-ness is in play.
    let mut queue = PgQueue::new(db.pool.clone(), clock.clone(), Duration::from_secs(3600));
    db.enqueue("later", 200).await;
    db.enqueue("now", 100).await;

    assert_eq!(ids(&queue.claim(10).await.unwrap()), ["now"]);
    assert!(
        queue.claim(10).await.unwrap().is_empty(),
        "claimed jobs stay hidden"
    );
    clock.advance(Duration::from_secs(100));
    assert_eq!(ids(&queue.claim(10).await.unwrap()), ["later"]);
    db.drop().await;
}

#[tokio::test]
async fn a_dedupe_key_admits_one_job() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let mut conn = db.pool.acquire().await.unwrap();
    assert!(
        enqueue(&mut conn, &message("a"), at(0), Some("order-1"))
            .await
            .unwrap()
    );
    assert!(
        !enqueue(&mut conn, &message("b"), at(0), Some("order-1"))
            .await
            .unwrap()
    );
    assert!(
        enqueue(&mut conn, &message("c"), at(0), None)
            .await
            .unwrap()
    );
    assert!(
        enqueue(&mut conn, &message("d"), at(0), None)
            .await
            .unwrap()
    );
    drop(conn);
    assert_eq!(db.states().await.len(), 3);
    db.drop().await;
}

#[tokio::test]
async fn settlements_record_state_and_attempts_and_release_requeues() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let mut queue = db.queue(Arc::new(SystemClock));
    for id in ["a", "b", "c"] {
        db.enqueue(id, 0).await;
    }
    let receipts = receipts(queue.claim(3).await.unwrap());
    queue
        .settle(
            receipts[0],
            Settlement::Done {
                result: json!(1),
                attempts: 2,
                message: message("a"),
            },
        )
        .await
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
        .await
        .unwrap();
    queue
        .settle(receipts[2], Settlement::Release)
        .await
        .unwrap();

    assert_eq!(
        db.states().await,
        [
            ("done".into(), 2),
            ("dead".into(), 1),
            ("pending".into(), 0)
        ]
    );
    let dead: (Option<String>, Option<String>) =
        sqlx::query_as("SELECT reason, error FROM worker_jobs WHERE state = 'dead'")
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(dead, (Some("Permanent".into()), Some("no".into())));
    assert_eq!(ids(&queue.claim(10).await.unwrap()), ["c"]);
    db.drop().await;
}

#[tokio::test]
async fn a_rolled_back_transaction_leaves_no_job() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    sqlx::query("CREATE TABLE orders (id BIGINT PRIMARY KEY)")
        .execute(&db.pool)
        .await
        .unwrap();
    for (id, commit) in [(1_i64, false), (2, true)] {
        let mut tx = db.pool.begin().await.unwrap();
        sqlx::query("INSERT INTO orders (id) VALUES ($1)")
            .bind(id)
            .execute(&mut *tx)
            .await
            .unwrap();
        enqueue(&mut tx, &message(&id.to_string()), at(0), None)
            .await
            .unwrap();
        if commit {
            tx.commit().await.unwrap();
        } else {
            tx.rollback().await.unwrap();
        }
    }
    let orders: Vec<(i64,)> = sqlx::query_as("SELECT id FROM orders")
        .fetch_all(&db.pool)
        .await
        .unwrap();
    assert_eq!(orders, [(2,)]);
    let mut queue = db.queue(Arc::new(SystemClock));
    assert_eq!(ids(&queue.claim(10).await.unwrap()), ["2"]);
    db.drop().await;
}

#[tokio::test]
async fn an_undecodable_row_is_malformed() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    sqlx::query("INSERT INTO worker_jobs (message_id, message, run_at) VALUES ('x', 'nope', 0)")
        .execute(&db.pool)
        .await
        .unwrap();
    let mut queue = db.queue(Arc::new(SystemClock));
    assert_eq!(ids(&queue.claim(1).await.unwrap()), ["malformed x"]);
    db.drop().await;
}

// One thread is enough: the eight claims are in flight at once on eight
// connections, so the server runs them concurrently.
#[tokio::test]
async fn concurrent_consumers_never_double_claim() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    const JOBS: usize = 500;
    let mut conn = db.pool.acquire().await.unwrap();
    for n in 0..JOBS {
        enqueue(&mut conn, &message(&n.to_string()), at(0), None)
            .await
            .unwrap();
    }
    // Open a connection per consumer first, so none of them gets a head start.
    let mut warm = vec![conn];
    for _ in 1..8 {
        warm.push(db.pool.acquire().await.unwrap());
    }
    drop(warm);
    // Rounds in lock step: all eight claims are in flight together, and a
    // consumer that comes back empty keeps going until every job is out.
    let total = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(tokio::sync::Barrier::new(8));
    let consumers: Vec<_> = (0..8)
        .map(|_| {
            let mut queue = db.queue(Arc::new(SystemClock));
            let (total, barrier) = (Arc::clone(&total), Arc::clone(&barrier));
            tokio::spawn(async move {
                let mut claimed = Vec::new();
                loop {
                    barrier.wait().await;
                    let claims = queue.claim(7).await.unwrap();
                    total.fetch_add(claims.len(), Ordering::SeqCst);
                    claimed.extend(ids(&claims));
                    barrier.wait().await;
                    if total.load(Ordering::SeqCst) >= JOBS {
                        return claimed;
                    }
                }
            })
        })
        .collect();
    let mut all = Vec::new();
    let mut busy = 0;
    for consumer in consumers {
        let claimed = consumer.await.unwrap();
        busy += usize::from(!claimed.is_empty());
        all.extend(claimed);
    }
    let unique: HashSet<_> = all.iter().collect();
    assert_eq!(all.len(), JOBS, "every job claimed exactly once");
    assert_eq!(unique.len(), JOBS);
    assert!(busy > 1, "claims were spread over consumers");
    db.drop().await;
}

#[tokio::test]
async fn an_expired_lease_is_claimed_again_and_the_old_owner_cannot_settle() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let clock = Arc::new(ManualClock::new(at(1_000)));
    let mut crashed = db.queue(clock.clone());
    let mut survivor = db.queue(clock.clone());
    db.enqueue("a", 0).await;
    let stale = receipts(crashed.claim(1).await.unwrap())[0];

    clock.advance(VISIBILITY - Duration::from_millis(1));
    assert!(
        survivor.claim(1).await.unwrap().is_empty(),
        "the lease still holds"
    );
    clock.advance(Duration::from_millis(1));
    let claims = survivor.claim(1).await.unwrap();
    assert_eq!(ids(&claims), ["a"]);
    let fresh = receipts(claims)[0];
    assert_eq!(fresh.0, stale.0);

    // The first consumer wakes up late: its settlement must not count.
    crashed
        .settle(
            stale,
            Settlement::DeadLetter {
                reason: DeadReason::Permanent,
                attempts: 1,
                error: None,
                message: Some(message("a")),
            },
        )
        .await
        .unwrap();
    crashed.settle(stale, Settlement::Release).await.unwrap();
    assert_eq!(db.states().await, [("claimed".into(), 0)]);
    survivor
        .settle(
            fresh,
            Settlement::Done {
                result: json!(null),
                attempts: 1,
                message: message("a"),
            },
        )
        .await
        .unwrap();
    assert_eq!(db.states().await, [("done".into(), 1)]);
    db.drop().await;
}

#[tokio::test]
async fn the_service_retries_then_dead_letters() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    for id in ["flaky", "broken"] {
        db.enqueue(id, 0).await;
    }
    let registry = HandlerTarget
        .build(&[(
            ModuleId::new("test.module.echo"),
            HandlerDeclaration::new("echo", 1, |delivery| async move {
                if delivery.message.id == "flaky" && delivery.attempt == 2 {
                    return Ok(json!("ok"));
                }
                Err(HandlerFailure::retryable("busy"))
            }),
        )])
        .unwrap();
    let config = ServiceConfig {
        concurrency: 2,
        capacity: 2,
        poll: Duration::from_millis(5),
        drain_timeout: Duration::from_secs(1),
        handler_timeout: None,
        retry: RetryPolicy::linear(3, Duration::from_millis(1)),
    };
    let service = WorkerService::new(registry, db.queue(Arc::new(SystemClock)), config);
    service
        .run(tokio::time::sleep(Duration::from_millis(300)))
        .await
        .unwrap();
    assert_eq!(db.states().await, [("done".into(), 2), ("dead".into(), 3)]);
    let dead: (String, String) =
        sqlx::query_as("SELECT reason, error FROM worker_jobs WHERE state = 'dead'")
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert!(dead.1.ends_with("busy"), "{}", dead.1);
    db.drop().await;
}
