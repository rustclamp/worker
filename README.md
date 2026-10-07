<img src="https://docs.rustclamp.com/assets/rustclamp-logo.png" alt="RustClamp logo" width="160">

# rustclamp-worker

Message handlers for [RustClamp](https://github.com/rustclamp/rustclamp)
workers. Modules contribute `HandlerDeclaration`s; `HandlerTarget` validates them
(duplicate `(message name, schema version)` fails at composition time) and compiles
a `HandlerRegistry` that dispatches an exact name/version match. Handlers see only
the transport-neutral envelope; broker ACK, retry and dead-letter stay in the transport adapter.

## Install

Not yet published to crates.io; depend on it from git (Rust 1.96.1+, edition 2024):

```toml
[dependencies]
rustclamp-worker = { git = "https://github.com/rustclamp/worker", features = ["service", "sqlite"] }
```

## Example

```rust
use rustclamp_core::{ContributionTarget, ModuleId};
use rustclamp_worker::{HandlerDeclaration, HandlerFailure, HandlerTarget};
use serde::Deserialize;
use serde_json::json;

#[derive(Deserialize)]
struct Welcome { user: u64 }

const MAIL: ModuleId = ModuleId::new("app.mail");

let registry = HandlerTarget.build(&[(
    MAIL,
    HandlerDeclaration::typed("welcome", 1, |msg: Welcome, delivery| async move {
        if delivery.attempt < 3 && msg.user == 0 {
            return Err(HandlerFailure::retryable("smtp busy"));
        }
        Ok(json!({ "sent": msg.user }))
    }),
)])?;
```

## Main API

- `HandlerDeclaration::new` (raw `Delivery`) and `::typed` (decoded payload in, serialized result out); `Delivery { message, attempt }`.
- `HandlerFailure::retryable` / `permanent` / `unknown_outcome`: lets policy authorize retries and keep timeouts whose side effects are unknown.
- `HandlerRegistry`: `dispatch`, `dispatch_json`, `deliver`, `validate`, `contains`, `routes`.
- `RetryPolicy` (`new`, `linear`) classifies a failure into an `Outcome` (retry with delay and error, or dead letter with a `DeadReason`).

## Features

| Feature | Adds |
| --- | --- |
| `service` | `service::WorkerService`: runs a registry over a `Transport` (`claim` / `settle` / `recover`) with backpressure, bounded concurrency, retries, dead-lettering, optional handler timeout, drain on shutdown, `ServiceEvent` observer and live `ServiceStats`. `BlockingTransport` + `Blocking` run blocking transports on Tokio's blocking pool. |
| `sqlite` | (implies `service`) `sqlite::SqliteQueue`, a durable transport over one `rusqlite` connection, and `sqlite::enqueue`, a transactional outbox that commits or rolls back with the caller's transaction. One consumer per database. |
| `mqtt` | (implies `service`) `mqtt::MqttTransport`, a QoS 1 transport over `rumqttc` (manual acks, persistent session; dead letters republish to a topic, then ack), and `mqtt::publish`. No TLS. |

Changes are tracked in [CHANGELOG.md](CHANGELOG.md).

Full documentation: <https://docs.rustclamp.com>

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option. Unless you state otherwise, any
contribution you submit for inclusion is dual licensed as above, without
additional terms or conditions.
