//! Tests for typed handlers, payload validation and outcome classification.

use std::future::Future;
use std::pin::pin;
use std::sync::Mutex;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rustclamp_core::{Clock, ContributionTarget, ModuleId};
use rustclamp_messaging::MessageEnvelope;
use rustclamp_worker::{
    DeadReason, Delivery, DispatchError, HandlerDeclaration, HandlerFailure, HandlerRegistry,
    HandlerTarget, Outcome, RetryPolicy,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

const OWNER: ModuleId = ModuleId::new("test.module.jobs");

#[derive(Deserialize)]
struct Echo {
    text: String,
}

#[derive(Serialize)]
struct Echoed {
    text: String,
}

#[derive(Deserialize)]
struct Flaky {
    fail_times: u32,
    permanent: bool,
}

// A Mutex, not a Cell: core's Clock is Send + Sync (core#5).
struct FixedClock(Mutex<SystemTime>);

impl Clock for FixedClock {
    fn now(&self) -> SystemTime {
        *self.0.lock().unwrap()
    }
}

// ponytail: every handler here completes on first poll, so a noop-waker loop is enough.
fn block_on<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    let mut context = Context::from_waker(Waker::noop());
    loop {
        if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
            return output;
        }
    }
}

fn registry() -> HandlerRegistry {
    HandlerTarget
        .build(&[
            (
                OWNER,
                HandlerDeclaration::typed("echo", 1, |echo: Echo, _| async move {
                    Ok(Echoed { text: echo.text })
                }),
            ),
            (
                OWNER,
                HandlerDeclaration::typed(
                    "flaky",
                    1,
                    |flaky: Flaky, delivery: Delivery| async move {
                        if delivery.attempt > flaky.fail_times {
                            return Ok(json!({ "attempts": delivery.attempt }));
                        }
                        let error = "boom".into();
                        Err(if flaky.permanent {
                            HandlerFailure::Permanent(error)
                        } else {
                            HandlerFailure::Retryable(error)
                        })
                    },
                ),
            ),
            (
                OWNER,
                HandlerDeclaration::new("raw", 1, |delivery: Delivery| async move {
                    Ok(delivery.message.payload)
                }),
            ),
        ])
        .unwrap()
}

fn delivery(name: &str, payload: Value, attempt: u32) -> Delivery {
    Delivery {
        message: MessageEnvelope {
            id: "m1".into(),
            name: name.into(),
            schema_version: 1,
            correlation_id: "c1".into(),
            causation_id: None,
            deadline_unix_ms: None,
            payload,
        },
        attempt,
    }
}

fn clock() -> FixedClock {
    FixedClock(Mutex::new(UNIX_EPOCH + Duration::from_secs(1_000)))
}

fn deliver(delivery: Delivery, policy: &RetryPolicy) -> Outcome {
    block_on(registry().deliver(delivery, policy, &clock()))
}

#[test]
fn typed_handlers_decode_input_and_return_a_value() {
    let value = block_on(registry().dispatch(delivery("echo", json!({"text": "hi"}), 1))).unwrap();
    assert_eq!(value, json!({"text": "hi"}));
    let raw = block_on(registry().dispatch(delivery("raw", json!([1, 2]), 1))).unwrap();
    assert_eq!(raw, json!([1, 2]));
}

#[test]
fn validate_checks_route_and_payload_without_running() {
    let registry = registry();
    assert!(registry.validate("echo", 1, &json!({"text": "hi"})).is_ok());
    assert!(matches!(
        registry.validate("echo", 1, &json!({"text": 3})),
        Err(DispatchError::InvalidPayload(_))
    ));
    assert!(matches!(
        registry.validate("echo", 2, &json!({})),
        Err(DispatchError::NoHandler { .. })
    ));
    assert!(registry.validate("raw", 1, &json!("anything")).is_ok());
}

#[test]
fn outcomes_follow_the_retry_policy() {
    let policy = RetryPolicy::linear(3, Duration::from_millis(100));
    let retryable = json!({"fail_times": 5, "permanent": false});

    assert!(matches!(
        deliver(delivery("flaky", retryable.clone(), 2), &policy),
        Outcome::Retry(delay) if delay == Duration::from_millis(200)
    ));
    assert!(matches!(
        deliver(delivery("flaky", retryable, 3), &policy),
        Outcome::DeadLetter {
            reason: DeadReason::RetryExhausted,
            ..
        }
    ));
    assert!(matches!(
        deliver(
            delivery("flaky", json!({"fail_times": 1, "permanent": true}), 1),
            &policy
        ),
        Outcome::DeadLetter {
            reason: DeadReason::Permanent,
            ..
        }
    ));
    assert!(matches!(
        deliver(delivery("flaky", json!({"fail_times": 1, "permanent": false}), 2), &policy),
        Outcome::Done(value) if value == json!({"attempts": 2})
    ));
    assert!(matches!(
        deliver(delivery("echo", json!({}), 1), &policy),
        Outcome::DeadLetter {
            reason: DeadReason::InvalidPayload,
            ..
        }
    ));
    assert!(matches!(
        deliver(delivery("missing", json!({}), 1), &policy),
        Outcome::DeadLetter {
            reason: DeadReason::NoHandler,
            ..
        }
    ));
}

#[test]
fn a_stepped_backoff_table_is_a_policy() {
    let policy = RetryPolicy::new(5, |attempt| match attempt {
        1 => Duration::from_secs(1),
        2 => Duration::from_secs(5),
        _ => Duration::from_secs(30),
    });
    let retryable = json!({"fail_times": 9, "permanent": false});
    assert!(matches!(
        deliver(delivery("flaky", retryable, 4), &policy),
        Outcome::Retry(delay) if delay == Duration::from_secs(30)
    ));
}

#[test]
fn an_expired_message_is_dead_lettered_without_running() {
    let mut expired = delivery("echo", json!({"text": "late"}), 1);
    expired.message.deadline_unix_ms = Some(999_000);
    let outcome = deliver(expired, &RetryPolicy::linear(3, Duration::ZERO));
    assert!(matches!(
        outcome,
        Outcome::DeadLetter {
            reason: DeadReason::Expired,
            error: None
        }
    ));
}
