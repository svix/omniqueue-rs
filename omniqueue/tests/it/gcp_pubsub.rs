//! Support for Google Cloud Pub/Sub.
//!
//! In this system subscriptions are like queue bindings to topics.
//! Consumers need a subscription id to start receiving messages.
//! We don't have any public API for managing/creating/deleting subscriptions in this module, so
//! this is left to the user to do via whatever method they like.
//!
//! - <https://cloud.google.com/pubsub/docs/create-topic>
//! - <https://cloud.google.com/pubsub/docs/create-subscription#pubsub_create_push_subscription-gcloud>
//! - <https://cloud.google.com/pubsub/docs/publisher> (how to publish messages ad hoc, helpful for debugging)
//!
//! Don't have a better place to mention this just yet.
//! When testing against the gcloud emulator, you need to set `PUBSUB_EMULATOR_HOST` to the bind
//! address, and `PUBSUB_PROJECT_ID` (matching however the emulator was configured).
//! This should bypass the need for credentials and so on.
//! ```sh
//! export PUBSUB_EMULATOR_HOST=localhost:8085
//! export PUBSUB_PROJECT_ID=local-project
//! ```
//! > N.b. the rust client hardcodes the project id to `local-project` when it sees the
//! > `PUBSUB_EMULATOR_HOST` env var in use, so if you see errors about resources not found etc, it
//! > might be because of a project mismatch.
//!
//! To use the `gcloud` CLI with the emulator (useful for creating topics/subscriptions), you have
//! to configure an override for the pubsub API:
//!
//! ```sh
//! gcloud config set api_endpoint_overrides/pubsub "http://${PUBSUB_EMULATOR_HOST}/"
//! ```
//! Note that you'll also have to manually set it back to the default as needed:
//! ```sh
//! gcloud config unset api_endpoint_overrides/pubsub
//! ```
//! h/t <https://stackoverflow.com/a/73059126>
//!
//! Also note, and this is odd, `gcloud` will prompt you to login even though you're trying to
//! connect to a local process.
//! Go ahead and follow the prompts to get your CLI working.
//!
//! I guess it still wants to talk to GCP for other interactions other than the pubsub API.
//!
//! ## Example `gcloud` usage:
//! ```sh
//! gcloud --project=local-project pubsub topics create tester
//! gcloud --project=local-project pubsub topics create dead-letters
//! gcloud --project=local-project pubsub subscriptions create local-1 \
//!   --topic=tester \
//!   --dead-letter-topic=dead-letters \
//!   --max-delivery-attempts=5
//! gcloud --project local-project pubsub topics publish tester --message='{"my message": 1234}'
//! ```
//!

use google_cloud_googleapis::pubsub::v1::DeadLetterPolicy;
use google_cloud_pubsub::client::{Client, ClientConfig};
use google_cloud_pubsub::subscription::SubscriptionConfig;
use std::time::{Duration, Instant};

use omniqueue::{
    backends::gcp_pubsub::{GcpPubSubBackend, GcpPubSubConfig},
    queue::{consumer::QueueConsumer, producer::QueueProducer},
    QueueBuilder,
};
use serde::{Deserialize, Serialize};

const DEFAULT_PUBSUB_EMULATOR_HOST: &str = "localhost:8085";
/// Controls how many times a message can be nack'd before it lands on the dead letter topic.
const MAX_DELIVERY_ATTEMPTS: i32 = 5;

async fn get_client() -> Client {
    // The `Default` impl for `ClientConfig` looks for this env var. When set it branches for
    // local-mode use using the addr in the env var and a hardcoded project id of `local-project`.
    if std::env::var("PUBSUB_EMULATOR_HOST").is_err() {
        std::env::set_var("PUBSUB_EMULATOR_HOST", DEFAULT_PUBSUB_EMULATOR_HOST);
    }
    Client::new(ClientConfig::default()).await.unwrap()
}

// FIXME: check to see if there's already one of these in here somewhere
fn random_chars() -> impl Iterator<Item = char> {
    std::iter::repeat_with(fastrand::alphanumeric)
}

/// Returns a [`QueueBuilder`] configured to connect to the GCP emulator instance spawned by the
/// file `testing-docker-compose.yaml` in the root of the repository.
///
/// Additionally this will make a temporary topic/subscription on that instance for the duration of
/// the test such as to ensure there is no stealing.
async fn make_test_queue() -> QueueBuilder<GcpPubSubBackend> {
    let client = get_client().await;

    let topic_name: String = "topic-".chars().chain(random_chars().take(8)).collect();
    // Need to define a dead letter topic to avoid the "bad" test cases from pulling the nacked
    // messages again and again.
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

    let config = GcpPubSubConfig {
        topic_id: topic.id(),
        subscription_id: subscription.id(),
        credentials_file: None,
    };

    GcpPubSubBackend::builder(config)
}

#[tokio::test]
async fn test_raw_send_recv() {
    let payload = b"{\"test\": \"data\"}";
    let (p, mut c) = make_test_queue().await.build_pair().await.unwrap();

    p.send_raw(&payload.to_vec()).await.unwrap();

    let d = c.receive().await.unwrap();
    assert_eq!(d.borrow_payload().unwrap(), payload);
}

#[tokio::test]
async fn test_bytes_send_recv() {
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

#[tokio::test]
async fn test_custom_send_recv() {
    let payload = ExType { a: 3 };

    let encoder = |p: &ExType| Ok(vec![p.a]);
    let decoder = |p: &Vec<u8>| {
        Ok(ExType {
            a: p.first().copied().unwrap_or(0),
        })
    };

    let (p, mut c) = make_test_queue()
        .await
        .with_encoder(encoder)
        .with_decoder(decoder)
        .build_pair()
        .await
        .unwrap();

    p.send_custom(&payload).await.unwrap();

    let d = c.receive().await.unwrap();
    assert_eq!(d.payload_custom::<ExType>().unwrap().unwrap(), payload);

    // Because it doesn't use JSON, this should fail:
    d.payload_serde_json::<ExType>().unwrap_err();
    d.ack().await.unwrap();
}

/// Consumer will return immediately if there are fewer than max messages to start with.
#[tokio::test]
async fn test_send_recv_all_partial() {
    let payload = ExType { a: 2 };
    let (p, mut c) = make_test_queue().await.build_pair().await.unwrap();

    p.send_serde_json(&payload).await.unwrap();
    let deadline = Duration::from_secs(1);

    let now = Instant::now();
    let mut xs = c.receive_all(2, deadline).await.unwrap();
    assert_eq!(xs.len(), 1);
    let d = xs.remove(0);
    assert_eq!(d.payload_serde_json::<ExType>().unwrap().unwrap(), payload);
    d.ack().await.unwrap();
    assert!(now.elapsed() <= deadline);
}

/// Consumer should yield items immediately if there's a full batch ready on the first poll.
#[tokio::test]
async fn test_send_recv_all_full() {
    let payload1 = ExType { a: 1 };
    let payload2 = ExType { a: 2 };
    let (p, mut c) = make_test_queue().await.build_pair().await.unwrap();

    p.send_serde_json(&payload1).await.unwrap();
    p.send_serde_json(&payload2).await.unwrap();
    let deadline = Duration::from_secs(1);

    let now = Instant::now();
    let mut xs = c.receive_all(2, deadline).await.unwrap();
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
    // N.b. it's still possible this could turn up false if the test runs too slow.
    assert!(now.elapsed() < deadline);
}

/// Consumer will return the full batch immediately, but also return immediately if a partial batch is ready.
#[tokio::test]
async fn test_send_recv_all_full_then_partial() {
    let payload1 = ExType { a: 1 };
    let payload2 = ExType { a: 2 };
    let payload3 = ExType { a: 3 };
    let (p, mut c) = make_test_queue().await.build_pair().await.unwrap();

    p.send_serde_json(&payload1).await.unwrap();
    p.send_serde_json(&payload2).await.unwrap();
    p.send_serde_json(&payload3).await.unwrap();

    let deadline = Duration::from_secs(1);
    let now1 = Instant::now();
    let mut xs = c.receive_all(2, deadline).await.unwrap();
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
    assert!(now1.elapsed() < deadline);

    // 2nd call
    let now2 = Instant::now();
    let mut ys = c.receive_all(2, deadline).await.unwrap();
    assert_eq!(ys.len(), 1);
    let d3 = ys.remove(0);
    assert_eq!(
        d3.payload_serde_json::<ExType>().unwrap().unwrap(),
        payload3
    );
    d3.ack().await.unwrap();
    assert!(now2.elapsed() <= deadline);
}

/// Consumer will NOT wait indefinitely for at least one item.
#[tokio::test]
async fn test_send_recv_all_late_arriving_items() {
    let (_p, mut c) = make_test_queue().await.build_pair().await.unwrap();

    let deadline = Duration::from_secs(1);
    let now = Instant::now();
    let xs = c.receive_all(2, deadline).await.unwrap();
    let elapsed = now.elapsed();

    assert_eq!(xs.len(), 0);
    // Elapsed should be around the deadline, ballpark
    assert!(elapsed >= deadline);
    assert!(elapsed <= deadline + Duration::from_millis(200));
}
