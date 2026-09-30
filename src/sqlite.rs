//! Durable SQLite job queue and transactional outbox (feature `sqlite`, ADR 0026).
//!
//! [`SqliteQueue`] is a [`BlockingTransport`] over one `rusqlite` connection:
//! wrap it in [`Blocking`](crate::service::Blocking) to hand it to a
//! [`WorkerService`](crate::service::WorkerService). Jobs are rows in
//! `worker_jobs`; [`enqueue`] adds one and works inside the caller's own
//! transaction, so a business write and the job it triggers commit together
//! (the outbox).

use std::io;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, params};
use rustclamp_core::Clock;
use rustclamp_messaging::MessageEnvelope;

use crate::service::{BlockingTransport, Claim, Settlement};

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS worker_jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id TEXT NOT NULL,
    message TEXT NOT NULL,
    dedupe_key TEXT UNIQUE,
    run_at INTEGER NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    result TEXT,
    reason TEXT,
    error TEXT
);
CREATE INDEX IF NOT EXISTS worker_jobs_due ON worker_jobs (state, run_at);";

/// Creates the `worker_jobs` table when it is missing.
pub fn migrate(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch(SCHEMA)
}

/// Adds a pending job that becomes claimable at `run_at`.
///
/// Pass the application's open transaction (`&Transaction` derefs to
/// `&Connection`) to make the job commit or roll back with the rest of it.
/// A job whose `dedupe_key` already exists is not added; returns whether a row
/// was inserted.
pub fn enqueue(
    conn: &Connection,
    message: &MessageEnvelope,
    run_at: SystemTime,
    dedupe_key: Option<&str>,
) -> rusqlite::Result<bool> {
    let json = serde_json::to_string(message)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    let inserted = conn.execute(
        "INSERT INTO worker_jobs (message_id, message, dedupe_key, run_at)
         VALUES (?1, ?2, ?3, ?4) ON CONFLICT (dedupe_key) DO NOTHING",
        params![message.id, json, dedupe_key, unix_ms(run_at)],
    )?;
    Ok(inserted == 1)
}

/// A [`BlockingTransport`] over the `worker_jobs` table.
///
/// One consumer per database: [`recover`](BlockingTransport::recover) returns
/// every claimed row to pending, which is only right when no other worker
/// holds claims.
pub struct SqliteQueue {
    conn: Connection,
    clock: Arc<dyn Clock>,
}

impl SqliteQueue {
    /// Uses `conn` (creating the table if needed); due-ness is read from `clock`.
    pub fn new(conn: Connection, clock: Arc<dyn Clock>) -> rusqlite::Result<Self> {
        migrate(&conn)?;
        Ok(Self { conn, clock })
    }

    fn set(&self, id: i64, sql: &str, values: &[&dyn rusqlite::ToSql]) -> io::Result<()> {
        let mut all = values.to_vec();
        all.push(&id);
        self.conn
            .execute(sql, all.as_slice())
            .map_err(io::Error::other)?;
        Ok(())
    }
}

impl BlockingTransport for SqliteQueue {
    /// The row id.
    type Receipt = i64;

    fn claim(&mut self, limit: usize) -> io::Result<Vec<Claim<i64>>> {
        let now = unix_ms(self.clock.now());
        let mut statement = self
            .conn
            .prepare_cached(
                "UPDATE worker_jobs SET state = 'claimed' WHERE id IN (
                     SELECT id FROM worker_jobs WHERE state = 'pending' AND run_at <= ?1
                     ORDER BY run_at, id LIMIT ?2)
                 RETURNING id, run_at, message_id, message",
            )
            .map_err(io::Error::other)?;
        let mut rows = statement
            .query_map(params![now, limit as i64], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ))
            })
            .and_then(Iterator::collect::<Result<Vec<_>, _>>)
            .map_err(io::Error::other)?;
        // RETURNING does not promise an order.
        rows.sort_by_key(|(id, run_at, ..)| (*run_at, *id));
        Ok(rows
            .into_iter()
            .map(|(receipt, _, id, json)| match serde_json::from_str(&json) {
                Ok(message) => Claim::Message { receipt, message },
                Err(error) => Claim::Malformed {
                    receipt,
                    id,
                    error: error.to_string(),
                },
            })
            .collect())
    }

    fn settle(&mut self, receipt: i64, settlement: Settlement) -> io::Result<()> {
        match settlement {
            Settlement::Done {
                result, attempts, ..
            } => self.set(
                receipt,
                "UPDATE worker_jobs SET state = 'done', attempts = ?1, result = ?2 WHERE id = ?3",
                &[&attempts, &result.to_string()],
            ),
            Settlement::DeadLetter {
                reason,
                attempts,
                error,
                ..
            } => self.set(
                receipt,
                "UPDATE worker_jobs SET state = 'dead', attempts = ?1, reason = ?2, error = ?3 WHERE id = ?4",
                &[&attempts, &format!("{reason:?}"), &error],
            ),
            Settlement::Release => self.set(
                receipt,
                "UPDATE worker_jobs SET state = 'pending' WHERE id = ?1",
                &[],
            ),
        }
    }

    fn recover(&mut self) -> io::Result<()> {
        self.conn
            .execute(
                "UPDATE worker_jobs SET state = 'pending' WHERE state = 'claimed'",
                [],
            )
            .map_err(io::Error::other)?;
        Ok(())
    }
}

fn unix_ms(time: SystemTime) -> i64 {
    // Before 1970 clamps to 0; past i64 milliseconds is not a real clock.
    time.duration_since(UNIX_EPOCH).map_or(0, |elapsed| {
        i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX)
    })
}
