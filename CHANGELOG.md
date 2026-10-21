# Changelog

## 2.0.0 - 2026-10-21

### Changed

- Breaking: `Delivery::message` is now `Arc<MessageEnvelope>`, so
  `WorkerService` no longer deep-clones the message for every attempt; it
  wraps it once per claim and unwraps it again to settle. Build one with
  `message: Arc::new(envelope)` (or `envelope.into()`); a handler that needs
  an owned message or payload clones it (`Arc::unwrap_or_clone`,
  `delivery.message.payload.clone()`). 100k instant jobs over an in-memory
  transport use 0.49 s of CPU instead of 1.08 s (#5).

### Added

- Feature `postgres` (implies `service`, adds `rustclamp-postgres`):
  `postgres::PgQueue`, a `Transport` over a SQLx `PgPool` that several
  consumers can share. `claim` takes what is due on an injected `Clock` with
  `FOR UPDATE SKIP LOCKED` and holds each row for a visibility timeout; a row
  whose lease ran out is claimed again, and a settle from the old owner is
  ignored. `postgres::enqueue` inserts a job on the caller's connection or open
  transaction (the outbox), with the same dedupe key rule as SQLite.
  `postgres::migration(version)` creates `worker_jobs` from the app's own
  `Migrations` plan (ADR 0038, #19).

## 1.0.0 - 2026-10-19

### Changed

- Ready for crates.io: `publish = false` is gone and the manifest carries
  homepage, documentation, keywords and categories. Nothing is uploaded until
  rustclamp's `tools/publish.py --execute` runs.
- `WorkerService` keeps claiming while a backlog lasts: when a claim comes back
  full, it claims again as the runners run low instead of waiting for the next
  `poll` tick. 100k instant jobs (concurrency 8, capacity 64, poll 5 ms) take
  0.28 s instead of 7.0 s; the claim limit is unchanged (#5).
- `mqtt::MqttTransport` reports a broker outage: a stderr warning on the
  first connection error, again every minute while it lasts, and once on
  reconnect; `MqttTransport::outage()` returns how long it has lasted, for
  health checks. `claim` still returns no error (#12).
- Add feature `mqtt` (implies `service`): `mqtt::MqttTransport`, a `Transport`
  over one `rumqttc` QoS 1 subscription with manual acks and a persistent
  session (`Done` acks, `DeadLetter` republishes to a dead-letter topic then
  acks, `Release` leaves the message for redelivery on reconnect), and
  `mqtt::publish` for producers. No TLS (ADR 0028, #12).
- Add feature `sqlite` (implies `service`): `sqlite::SqliteQueue`, a durable
  `BlockingTransport` over one `rusqlite` connection (claim what is due on an
  injected `Clock`, dedupe key, attempts, `recover`), and `sqlite::enqueue`, a
  transactional outbox that joins the caller's transaction (ADR 0026, #3).
- Breaking: `Outcome::Retry` is now `Retry { delay, error }` and
  `ServiceEvent::Retrying` carries the failure as `error`, so apps can log the
  real `last_error`; `Settlement::Done` and `Settlement::DeadLetter` carry the
  `message` (`None` for a malformed item), so receipts need not keep the raw
  payload (#4).
- Add `HandlerFailure::retryable` / `permanent` / `unknown_outcome`, taking
  anything convertible to a boxed error (`"code"`, `String`) (#4).
- Add `service::BlockingTransport` and `service::Blocking`, which run a
  blocking transport's calls on Tokio's blocking pool (#4).

- Add `Transport::recover`: runs once before the first claim, to return
  messages a crashed run left claimed; does nothing by default (#4).
- Add feature `service`: `service::WorkerService` runs a `HandlerRegistry` over a
  `Transport` (claim/settle) with backpressure, bounded concurrency, in-process
  retries, dead-lettering (new `DeadReason::Malformed`), an optional handler
  timeout, and a drain that cancels running attempts at its deadline;
  `ServiceEvent` callbacks and live `ServiceStats` (ADR 0020).
- `HandlerRegistry::deliver` reads the clock eagerly and returns a `Send` future.
- Breaking: handlers take a `Delivery { message, attempt }` and return
  `Result<serde_json::Value, HandlerFailure>`; `dispatch` returns the value and
  `dispatch_json` takes the attempt (ADR 0018 proposal A).
- Add `HandlerDeclaration::typed` (decoded payload in, serialized result out;
  undecodable payloads fail with `DispatchError::InvalidPayload`) and
  `HandlerRegistry::validate` to check a payload without running a handler.
- Add `RetryPolicy` (any backoff function, `linear` helper) and
  `HandlerRegistry::deliver`, which classifies an attempt as `Outcome::Done`,
  `Retry` or `DeadLetter` with a `DeadReason` (including expired deadlines).
- Add `HandlerRegistry::contains` and `HandlerRegistry::routes` for route introspection.
- Classify handler failures as retryable, permanent, or unknown outcome.
- Add handler declarations, duplicate-route validation, envelope decoding, and dispatch.
