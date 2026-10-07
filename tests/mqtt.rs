//! Tests for the MQTT transport against a real broker.

#![cfg(feature = "mqtt")]

use std::time::Duration;

use rumqttc::MqttOptions;
use rustclamp_messaging::MessageEnvelope;
use rustclamp_worker::DeadReason;
use rustclamp_worker::mqtt::{MqttTransport, publish};
use rustclamp_worker::service::{Claim, Settlement, Transport};
use serde_json::json;
use tokio::time::sleep;

type Claims = Vec<Claim<rumqttc::Publish>>;

/// Claims until `count` arrived or five seconds passed.
async fn claim(transport: &mut MqttTransport, count: usize) -> Claims {
    let mut claims = Vec::new();
    for _ in 0..100 {
        claims.extend(transport.claim(count - claims.len()).await.unwrap());
        if claims.len() == count {
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }
    claims
}

fn ids(claims: &Claims) -> Vec<String> {
    claims
        .iter()
        .map(|claim| match claim {
            Claim::Message { message, .. } => message.id.clone(),
            Claim::Malformed { .. } => "malformed".into(),
        })
        .collect()
}

fn message(id: &str) -> MessageEnvelope {
    let mut message = MessageEnvelope::new("echo", 1, json!({ "text": "hi" }));
    message.id = id.into();
    message
}

#[tokio::test]
async fn a_real_broker() {
    let Ok(url) = std::env::var("RUSTCLAMP_TEST_MQTT_URL") else {
        return;
    };
    let address = url.trim_start_matches("mqtt://");
    let (host, port) = address.rsplit_once(':').unwrap_or((address, "1883"));
    let options = |client: &str| MqttOptions::new(client, host, port.parse().unwrap());
    let run = std::process::id();
    let (jobs, dead) = (format!("rc-test/{run}/jobs"), format!("rc-test/{run}/dead"));
    let consumer = format!("rc-test-{run}-consumer");

    let connect = || MqttTransport::connect(options(&consumer), &jobs, &dead);
    let mut transport = connect().await.unwrap();
    let mut dead_letters =
        MqttTransport::connect(options(&format!("rc-test-{run}-dead")), &dead, "unused")
            .await
            .unwrap();
    // Subscriptions complete in the background; publishes before SUBACK are dropped.
    sleep(Duration::from_secs(1)).await;

    publish(transport.client(), &jobs, &message("done"))
        .await
        .unwrap();
    transport
        .client()
        .publish(&jobs, rumqttc::QoS::AtLeastOnce, false, "not json")
        .await
        .unwrap();
    // Last, since some brokers (rumqttd) redeliver everything after the oldest unacked message.
    publish(transport.client(), &jobs, &message("unsettled"))
        .await
        .unwrap();

    let claims = claim(&mut transport, 3).await;
    assert_eq!(ids(&claims), ["done", "malformed", "unsettled"]);
    // Settled in arrival order: MQTT 3.1.1 asks for in-order PUBACKs and rumqttd relies on it.
    for (claim, settlement) in claims.into_iter().zip(["done", "dead", "release"]) {
        let (receipt, message) = match claim {
            Claim::Message { receipt, message } => (receipt, Some(message)),
            Claim::Malformed { receipt, .. } => (receipt, None),
        };
        let settlement = match settlement {
            "done" => Settlement::Done {
                result: json!(null),
                attempts: 1,
                message: message.unwrap(),
            },
            "dead" => Settlement::DeadLetter {
                reason: DeadReason::Malformed,
                attempts: 0,
                error: None,
                message,
            },
            _ => Settlement::Release,
        };
        transport.settle(receipt, settlement).await.unwrap();
    }

    let dead_claims = claim(&mut dead_letters, 1).await;
    let [Claim::Malformed { receipt, .. }] = dead_claims.as_slice() else {
        panic!("{:?}", ids(&dead_claims));
    };
    assert_eq!(&receipt.payload[..], b"not json");

    // Reconnecting with the same client id: the broker redelivers only the unacked message.
    sleep(Duration::from_millis(200)).await;
    drop(transport);
    let mut transport = connect().await.unwrap();
    let claims = claim(&mut transport, 2).await;
    assert_eq!(ids(&claims), ["unsettled"]);
}
