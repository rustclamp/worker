# Changelog

## Unreleased

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
