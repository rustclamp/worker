# rustclamp-worker

Worker-owned handler contributions, route validation, envelope decoding, and
dispatch. Applications compile `HandlerDeclaration`s with the public Kernel
`TargetComposition`; duplicate `(message name, schema version)` routes fail at
composition time. Handler code receives only the transport-neutral envelope.
The target rejects empty names and version zero, then dispatches only an exact
name/version match while preserving decode and handler errors.

Concurrency, delivery outcomes, retries, and broker integration are layered on
after this handler boundary is proven.
