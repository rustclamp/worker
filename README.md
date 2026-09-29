# rustclamp-worker

Worker-owned handler contributions, route validation, envelope decoding, and
dispatch. Applications compile `HandlerDeclaration`s with the public Kernel
`TargetComposition`; duplicate `(message name, schema version)` routes fail at
composition time. Handler code receives only the transport-neutral envelope.
The target rejects empty names and version zero, then dispatches only an exact
name/version match while preserving decode and handler errors.

Handlers classify failed attempts as retryable, permanent, or unknown outcome.
This lets a transport policy authorize retries explicitly and preserve timeouts
that may have happened after an external side effect. Broker-specific ACK, retry,
and dead-letter operations remain in the transport adapter.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option. Unless you state otherwise, any
contribution you submit for inclusion is dual licensed as above, without
additional terms or conditions.
