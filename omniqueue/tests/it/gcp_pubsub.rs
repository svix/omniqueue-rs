//! Support for Google Cloud Pub/Sub.
//!
//! In this system subscriptions are like queue bindings to topics.
//! Consumers need a subscription id to start receiving messages.
//! We don't have any public API for managing/creating/deleting subscriptions in
//! this module, so this is left to the user to do via whatever method they
//! like.
//!
//! - <https://cloud.google.com/pubsub/docs/create-topic>
//! - <https://cloud.google.com/pubsub/docs/create-subscription#pubsub_create_push_subscription-gcloud>
//! - <https://cloud.google.com/pubsub/docs/publisher> (how to publish messages
//!   ad hoc, helpful for debugging)
//!
//! Don't have a better place to mention this just yet.
//! When testing against the gcloud emulator, you need to set
//! `PUBSUB_EMULATOR_HOST` to the bind address, and `PUBSUB_PROJECT_ID`
//! (matching however the emulator was configured). This should bypass the need
//! for credentials and so on. ```sh
//! export PUBSUB_EMULATOR_HOST=localhost:8085
//! export PUBSUB_PROJECT_ID=local-project
//! ```
//! > N.b. the rust client hardcodes the project id to `local-project` when it
//! > sees the
//! > `PUBSUB_EMULATOR_HOST` env var in use, so if you see errors about
//! > resources not found etc, it
//! > might be because of a project mismatch.
//!
//! To use the `gcloud` CLI with the emulator (useful for creating
//! topics/subscriptions), you have to configure an override for the pubsub API:
//! ```sh
//! gcloud config set api_endpoint_overrides/pubsub "http://${PUBSUB_EMULATOR_HOST}/"
//! ```
//! Note that you'll also have to manually set it back to the default as needed:
//! ```sh
//! gcloud config unset api_endpoint_overrides/pubsub
//! ```
//! h/t <https://stackoverflow.com/a/73059126>
//!
//! Also note, and this is odd, `gcloud` will prompt you to login even though
//! you're trying to connect to a local process.
//! Go ahead and follow the prompts to get your CLI working.
//!
//! I guess it still wants to talk to GCP for other interactions other than the
//! pubsub API.
//!
//! ## Example `gcloud` usage:
//! ```sh
//! gcloud --project=local-project pubsub topics create tester
//! gcloud --project=local-project pubsub topics create dead-letters
//! gcloud --project=local-project pubsub subscriptions create local-1 \
//!   --topic=tester \
//!   --dead-letter-topic=dead-letters \
//!   --max-delivery-attempts=5
//! gcloud --project local-project pubsub topics publish tester --message='{"my
//! message": 1234}' ```

use std::time::{Duration, Instant};

use assert_matches::assert_matches;
use gcloud_googleapis::pubsub::v1::DeadLetterPolicy;
use gcloud_pubsub::{
    client::{Client, ClientConfig},
    subscription::SubscriptionConfig,
};
use omniqueue::{
    backends::{GcpPubSubBackend, GcpPubSubConfig, GcpPubSubConsumer},
    Delivery, QueueBuilder, QueueError,
};
use serde::{Deserialize, Serialize};

const DEFAULT_PUBSUB_EMULATOR_HOST: &str = "localhost:8085";
/// Controls how many times a message can be nack'd before it lands on the dead
/// letter topic.
const MAX_DELIVERY_ATTEMPTS: i32 = 5;

async fn get_client() -> Client {
    // The `Default` impl for `ClientConfig` looks for this env var. When set it
    // branches for local-mode use using the addr in the env var and a hardcoded
    // project id of `local-project`.
    if std::env::var("PUBSUB_EMULATOR_HOST").is_err() {
        std::env::set_var("PUBSUB_EMULATOR_HOST", DEFAULT_PUBSUB_EMULATOR_HOST);
    }
    Client::new(ClientConfig::default()).await.unwrap()
}

// FIXME: check to see if there's already one of these in here somewhere
fn random_chars() -> impl Iterator<Item = char> {
    std::iter::repeat_with(fastrand::alphanumeric)
}

/// Creates a temporary topic/subscription on the GCP emulator instance spawned
/// by the file `testing-docker-compose.yaml` in the root of the repository, and
/// returns the [`GcpPubSubConfig`] pointing at it.
///
/// The topic and subscription are unique to the calling test such as to ensure
/// there is no stealing.
async fn make_test_queue_config() -> GcpPubSubConfig {
    let client = get_client().await;

    let topic_name: String = "topic-".chars().chain(random_chars().take(8)).collect();
    // Need to define a dead letter topic to avoid the "bad" test cases from
    // pulling the nacked messages again and again.
    let dead_letter_topic_name: String = "topic-".chars().chain(random_chars().take(8)).collect();
    let subscription_name: String = "subscription-"
        .chars()
        .chain(random_chars().take(8))
        .collect();

    let topic = client.create_topic(&topic_name, None, None).await.unwrap();
    let dead_letter_topic = client
        .create_topic(&dead_letter_topic_name, None, None)
        .await
        .unwrap();
    let subscription = client
        .create_subscription(
            &subscription_name,
            &topic_name,
            SubscriptionConfig {
                // Messages published to the topic need to supply a unique ID to make use of this
                enable_exactly_once_delivery: true,
                dead_letter_policy: Some(DeadLetterPolicy {
                    dead_letter_topic: dead_letter_topic.fully_qualified_name().into(),
                    max_delivery_attempts: MAX_DELIVERY_ATTEMPTS,
                }),
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();

    GcpPubSubConfig {
        topic_id: topic.id(),
        subscription_id: subscription.id(),
        credentials_file: None,
    }
}

/// Returns a [`QueueBuilder`] pointed at a fresh temporary topic/subscription.
///
/// See [`make_test_queue_config`].
async fn make_test_queue() -> QueueBuilder<GcpPubSubBackend> {
    GcpPubSubBackend::builder(make_test_queue_config().await)
}

/// Receives `count` deliveries, pulling in batches.
///
/// Gives up early if a pull comes back empty, so that a test asserting on the
/// count fails rather than hangs.
async fn receive_n(c: &mut GcpPubSubConsumer, count: usize) -> Vec<Delivery> {
    let timeout = Duration::from_secs(1);
    let mut deliveries = Vec::with_capacity(count);
    while deliveries.len() < count {
        let batch = c.receive_all(count, timeout).await.unwrap();
        if batch.is_empty() {
            break;
        }
        deliveries.extend(batch);
    }

    deliveries
}

#[tokio::test]
async fn test_raw_send_recv() {
    let payload = b"{\"test\": \"data\"}";
    let (p, mut c) = make_test_queue().await.build_pair().await.unwrap();

    p.send_raw(payload).await.unwrap();

    let d = c.receive().await.unwrap();
    assert_eq!(d.borrow_payload().unwrap(), payload);
}

#[tokio::test]
async fn test_bytes_send_recv() {
    use omniqueue::QueueProducer as _;

    let payload = b"hello";
    let (p, mut c) = make_test_queue().await.build_pair().await.unwrap();

    p.send_bytes(payload).await.unwrap();

    let d = c.receive().await.unwrap();
    assert_eq!(d.borrow_payload().unwrap(), payload);
    d.ack().await.unwrap();
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct ExType {
    a: u8,
}

#[tokio::test]
async fn test_serde_send_recv() {
    let payload = ExType { a: 2 };
    let (p, mut c) = make_test_queue().await.build_pair().await.unwrap();

    p.send_serde_json(&payload).await.unwrap();

    let d = c.receive().await.unwrap();
    assert_eq!(d.payload_serde_json::<ExType>().unwrap().unwrap(), payload);
    d.ack().await.unwrap();
}

/// Consumer will return immediately if there are fewer than max messages to
/// start with.
#[tokio::test]
async fn test_send_recv_all_partial() {
    let payload = ExType { a: 2 };
    let (p, mut c) = make_test_queue().await.build_pair().await.unwrap();

    p.send_serde_json(&payload).await.unwrap();
    let timeout = Duration::from_secs(1);

    let now = Instant::now();
    let mut xs = c.receive_all(2, timeout).await.unwrap();
    assert_eq!(xs.len(), 1);
    let d = xs.remove(0);
    assert_eq!(d.payload_serde_json::<ExType>().unwrap().unwrap(), payload);
    d.ack().await.unwrap();
    assert!(now.elapsed() <= timeout);
}

/// Consumer should yield items immediately if there's a full batch ready on the
/// first poll.
#[tokio::test]
async fn test_send_recv_all_full() {
    let payload1 = ExType { a: 1 };
    let payload2 = ExType { a: 2 };
    let (p, mut c) = make_test_queue().await.build_pair().await.unwrap();

    p.send_serde_json(&payload1).await.unwrap();
    p.send_serde_json(&payload2).await.unwrap();
    let timeout = Duration::from_secs(1);

    let now = Instant::now();
    let mut xs = c.receive_all(2, timeout).await.unwrap();
    assert_eq!(xs.len(), 2);
    let d1 = xs.remove(0);
    assert_eq!(
        d1.payload_serde_json::<ExType>().unwrap().unwrap(),
        payload1
    );
    d1.ack().await.unwrap();

    let d2 = xs.remove(0);
    assert_eq!(
        d2.payload_serde_json::<ExType>().unwrap().unwrap(),
        payload2
    );
    d2.ack().await.unwrap();
    // N.b. it's still possible this could turn up false if the test runs too
    // slow.
    assert!(now.elapsed() < timeout);
}

/// Consumer will return the full batch immediately, but also return immediately
/// if a partial batch is ready.
#[tokio::test]
async fn test_send_recv_all_full_then_partial() {
    let payload1 = ExType { a: 1 };
    let payload2 = ExType { a: 2 };
    let payload3 = ExType { a: 3 };
    let (p, mut c) = make_test_queue().await.build_pair().await.unwrap();

    p.send_serde_json(&payload1).await.unwrap();
    p.send_serde_json(&payload2).await.unwrap();
    p.send_serde_json(&payload3).await.unwrap();

    let timeout = Duration::from_secs(1);
    let now1 = Instant::now();
    let mut xs = c.receive_all(2, timeout).await.unwrap();
    assert_eq!(xs.len(), 2);
    let d1 = xs.remove(0);
    assert_eq!(
        d1.payload_serde_json::<ExType>().unwrap().unwrap(),
        payload1
    );
    d1.ack().await.unwrap();

    let d2 = xs.remove(0);
    assert_eq!(
        d2.payload_serde_json::<ExType>().unwrap().unwrap(),
        payload2
    );
    d2.ack().await.unwrap();
    assert!(now1.elapsed() < timeout);

    // 2nd call
    let now2 = Instant::now();
    let mut ys = c.receive_all(2, timeout).await.unwrap();
    assert_eq!(ys.len(), 1);
    let d3 = ys.remove(0);
    assert_eq!(
        d3.payload_serde_json::<ExType>().unwrap().unwrap(),
        payload3
    );
    d3.ack().await.unwrap();
    assert!(now2.elapsed() <= timeout);
}

/// Consumer will NOT wait indefinitely for at least one item.
#[tokio::test]
async fn test_send_recv_all_late_arriving_items() {
    let (_p, mut c) = make_test_queue().await.build_pair().await.unwrap();

    let timeout = Duration::from_secs(1);
    let now = Instant::now();
    let xs = c.receive_all(2, timeout).await.unwrap();
    let elapsed = now.elapsed();

    assert_eq!(xs.len(), 0);
    // Elapsed should be around the timeout, ballpark
    assert!(elapsed >= timeout);
    assert!(elapsed <= timeout + Duration::from_millis(200));
}

/// Nacking a message returns it to the subscription, so it is redelivered.
#[tokio::test]
async fn test_nack_redelivers() {
    let payload = ExType { a: 1 };
    let (p, mut c) = make_test_queue().await.build_pair().await.unwrap();
    p.send_serde_json(&payload).await.unwrap();

    // `receive()` blocks with no timeout, so pull with one instead.
    let mut xs = c.receive_all(1, Duration::from_secs(5)).await.unwrap();
    assert_eq!(xs.len(), 1);
    let d = xs.remove(0);
    d.nack().await.unwrap();

    // The nack puts it back, so it comes around again.
    let mut ys = c.receive_all(1, Duration::from_secs(5)).await.unwrap();
    assert_eq!(ys.len(), 1);
    let d = ys.remove(0);
    assert_eq!(d.payload_serde_json::<ExType>().unwrap().unwrap(), payload);
    d.ack().await.unwrap();
}

/// `send_raw_batch` is overridden to publish in bulk. Every message still
/// arrives.
#[tokio::test]
async fn test_send_raw_batch() {
    use omniqueue::QueueProducer as _;
    const COUNT: usize = 25;
    let (p, mut c) = make_test_queue().await.build_pair().await.unwrap();

    let payloads: Vec<Vec<u8>> = (0..COUNT)
        .map(|i| format!("payload-{i}").into_bytes())
        .collect();
    p.send_raw_batch(payloads.clone()).await.unwrap();

    let deliveries = receive_n(&mut c, COUNT).await;
    assert_eq!(deliveries.len(), COUNT);
    let mut received: Vec<Vec<u8>> = deliveries
        .iter()
        .map(|d| d.borrow_payload().unwrap().to_owned())
        .collect();
    received.sort();
    let mut expected = payloads;
    expected.sort();
    assert_eq!(received, expected);
}

/// The same bulk publishing applies to `send_serde_json_batch`.
#[tokio::test]
async fn test_send_serde_json_batch() {
    use omniqueue::QueueProducer as _;
    const COUNT: usize = 25;
    let (p, mut c) = make_test_queue().await.build_pair().await.unwrap();

    let payloads: Vec<ExType> = (0..COUNT).map(|i| ExType { a: i as u8 }).collect();
    p.send_serde_json_batch(payloads).await.unwrap();

    let deliveries = receive_n(&mut c, COUNT).await;
    assert_eq!(deliveries.len(), COUNT);
    let mut received: Vec<u8> = deliveries
        .iter()
        .map(|d| d.payload_serde_json::<ExType>().unwrap().unwrap().a)
        .collect();
    received.sort_unstable();
    let expected: Vec<u8> = (0..COUNT as u8).collect();
    assert_eq!(received, expected);
}

/// `send_bytes_batch` funnels into the overridden `send_raw_batch`.
#[tokio::test]
async fn test_send_bytes_batch() {
    use omniqueue::QueueProducer as _;
    const COUNT: usize = 25;
    let (p, mut c) = make_test_queue().await.build_pair().await.unwrap();

    let payloads: Vec<Vec<u8>> = (0..COUNT)
        .map(|i| format!("payload-{i}").into_bytes())
        .collect();
    p.send_bytes_batch(payloads.clone()).await.unwrap();

    let deliveries = receive_n(&mut c, COUNT).await;
    assert_eq!(deliveries.len(), COUNT);
    let mut received: Vec<Vec<u8>> = deliveries
        .iter()
        .map(|d| d.borrow_payload().unwrap().to_owned())
        .collect();
    received.sort();
    let mut expected = payloads;
    expected.sort();
    assert_eq!(received, expected);
}

#[tokio::test]
async fn test_redrive_dlq_unsupported() {
    let (p, _c) = make_test_queue().await.build_pair().await.unwrap();
    let err = p.redrive_dlq().await.unwrap_err();
    assert_matches!(err, QueueError::Unsupported(_));
}

/// The producer and consumer can be built independently from the same config.
#[tokio::test]
async fn test_producing_and_consuming_halves_can_be_built_separately() {
    let payload = ExType { a: 4 };
    let config = make_test_queue_config().await;

    let p = GcpPubSubBackend::builder(config.clone())
        .build_producer()
        .await
        .unwrap();
    let mut c = GcpPubSubBackend::builder(config)
        .build_consumer()
        .await
        .unwrap();

    p.send_serde_json(&payload).await.unwrap();

    let d = c.receive().await.unwrap();
    assert_eq!(d.payload_serde_json::<ExType>().unwrap().unwrap(), payload);
    d.ack().await.unwrap();
}

/// Unlike SQS, GCP Pub/Sub places no cap on how many messages a single pull may
/// request.
#[tokio::test]
async fn test_max_messages_is_unbounded() {
    use omniqueue::QueueConsumer as _;
    let (_p, c) = make_test_queue().await.build_pair().await.unwrap();
    assert_eq!(c.max_messages(), None);
}

/// A deadline too large for Pub/Sub's `i32` ack-deadline seconds is rejected
/// without a round trip.
#[cfg(feature = "beta")]
#[tokio::test]
async fn test_set_ack_deadline_rejects_too_large_duration() {
    let payload = ExType { a: 5 };
    let (p, mut c) = make_test_queue().await.build_pair().await.unwrap();
    p.send_serde_json(&payload).await.unwrap();

    let mut xs = c.receive_all(1, Duration::from_secs(5)).await.unwrap();
    assert_eq!(xs.len(), 1);
    let mut d = xs.remove(0);
    let err = d
        .set_ack_deadline(Duration::from_secs(u32::MAX.into()))
        .await
        .unwrap_err();
    assert_matches!(err, QueueError::Generic(_));
}
