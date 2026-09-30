# Changelog

## Unreleased

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
