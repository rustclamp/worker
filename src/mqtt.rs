//! MQTT consumer transport and publish helper (feature `mqtt`, ADR 0028).
//!
//! [`MqttTransport`] subscribes to a topic filter at QoS 1 with manual acks and
//! a persistent session: a message is PUBACKed only when it settles, so one the
//! worker never settled is redelivered by the broker after the next connect.
//! MQTT has no nack, so:
//!
//! - `Done` acks;
//! - `DeadLetter` republishes the original payload to the dead-letter topic
//!   (QoS 1), then acks;
//! - `Release` (shutdown) does not ack; the broker redelivers on reconnect.
//!
//! Retries stay in-process in the service (ADR 0020), as for every transport.
//! Delivery is at least once: a reconnect while a message runs redelivers it,
//! and acks still queued when the transport drops are lost, so those messages
//! come back too.
//!
//! ponytail: acks go out in settle order, not arrival order. Mosquitto accepts
//! that; brokers that need in-order PUBACKs (rumqttd) redeliver out-of-order
//! acked messages on reconnect. Run with `concurrency: 1` there, or reorder
//! acks here if such a broker matters.

use std::io;

use rumqttc::{AsyncClient, ClientError, Event, EventLoop, MqttOptions, Packet, Publish, QoS};
use rustclamp_messaging::MessageEnvelope;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::task::JoinHandle;
use tokio::time::{Duration, sleep};

use crate::service::{Claim, Settlement, Transport};

/// A [`Transport`] over one MQTT subscription.
///
/// Publishes are read by a background task and buffered until claimed; the
/// broker's in-flight limit for QoS 1 bounds the buffer.
pub struct MqttTransport {
    client: AsyncClient,
    received: UnboundedReceiver<Publish>,
    driver: JoinHandle<()>,
    dead_topic: String,
}

impl MqttTransport {
    /// Connects with `options` (forced to manual acks and a persistent
    /// session, so keep the client id stable across runs), subscribes to
    /// `filter` at QoS 1, and dead-letters to `dead_topic`.
    ///
    /// Must be called inside a Tokio runtime.
    pub async fn connect(
        mut options: MqttOptions,
        filter: &str,
        dead_topic: impl Into<String>,
    ) -> Result<Self, ClientError> {
        options.set_manual_acks(true).set_clean_session(false);
        let (client, events) = AsyncClient::new(options, 64);
        client.subscribe(filter, QoS::AtLeastOnce).await?;
        let (sender, received) = unbounded_channel();
        let driver = tokio::spawn(drive(events, sender));
        Ok(Self {
            client,
            received,
            driver,
            dead_topic: dead_topic.into(),
        })
    }

    /// The connection's client, e.g. to [`publish`] on it.
    pub fn client(&self) -> &AsyncClient {
        &self.client
    }
}

impl Drop for MqttTransport {
    fn drop(&mut self) {
        self.driver.abort();
    }
}

// ponytail: connection errors are retried every second and never surfaced, so a
// broker that stays down looks like an empty queue; report them through an
// event if operators need it.
async fn drive(mut events: EventLoop, sender: UnboundedSender<Publish>) {
    loop {
        match events.poll().await {
            Ok(Event::Incoming(Packet::Publish(publish))) => {
                if sender.send(publish).is_err() {
                    return;
                }
            }
            Ok(_) => {}
            Err(_) => sleep(Duration::from_secs(1)).await,
        }
    }
}

impl Transport for MqttTransport {
    /// The received publish; settling acks its packet id.
    type Receipt = Publish;

    async fn claim(&mut self, limit: usize) -> io::Result<Vec<Claim<Publish>>> {
        let mut claims = Vec::new();
        while claims.len() < limit {
            let Ok(publish) = self.received.try_recv() else {
                break;
            };
            claims.push(match serde_json::from_slice(&publish.payload) {
                Ok(message) => Claim::Message {
                    receipt: publish,
                    message,
                },
                Err(error) => Claim::Malformed {
                    id: format!("{}#{}", publish.topic, publish.pkid),
                    error: error.to_string(),
                    receipt: publish,
                },
            });
        }
        Ok(claims)
    }

    async fn settle(&mut self, receipt: Publish, settlement: Settlement) -> io::Result<()> {
        match settlement {
            Settlement::Done { .. } => {}
            // ponytail: the reason and error are not carried (MQTT 3.1.1 has no
            // headers); they are in ServiceEvent::DeadLettered. MQTT 5 user
            // properties if consumers of the dead-letter topic need them.
            Settlement::DeadLetter { .. } => self
                .client
                .publish(
                    &self.dead_topic,
                    QoS::AtLeastOnce,
                    false,
                    receipt.payload.to_vec(),
                )
                .await
                .map_err(io::Error::other)?,
            Settlement::Release => return Ok(()),
        }
        self.client.ack(&receipt).await.map_err(io::Error::other)
    }
}

/// Publishes `message` as JSON to `topic` at QoS 1.
///
/// Returns once the publish is queued on the client's event loop, not when the
/// broker has acknowledged it.
pub async fn publish(
    client: &AsyncClient,
    topic: &str,
    message: &MessageEnvelope,
) -> Result<(), ClientError> {
    let json = serde_json::to_vec(message).expect("a message envelope serializes");
    client.publish(topic, QoS::AtLeastOnce, false, json).await
}
