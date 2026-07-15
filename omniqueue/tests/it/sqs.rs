use std::{
    num::NonZeroUsize,
    time::{Duration, Instant},
};

use assert_matches::assert_matches;
use aws_sdk_sqs::{types::QueueAttributeName, Client};
use omniqueue::{
    backends::{SqsBackend, SqsConfig, SqsConsumer},
    Delivery, QueueBuilder, QueueError,
};
use serde::{Deserialize, Serialize};

const ROOT_URL: &str = "http://localhost:9324";
const DEFAULT_CFG: [(&str, &str); 3] = [
    ("AWS_DEFAULT_REGION", "localhost"),
    ("AWS_ACCESS_KEY_ID", "x"),
    ("AWS_SECRET_ACCESS_KEY", "x"),
];

/// A visibility timeout short enough to observe redelivery within a test, but
/// long enough that a busy CI runner cannot stall past it between two receives.
const VISIBILITY_TIMEOUT: Duration = Duration::from_secs(3);

/// Creates a temporary queue on the SQS instance spawned by the file
/// `testing-docker-compose.yaml` in the root of the repository, and returns the
/// [`SqsConfig`] pointing at it.
///
/// The queue is unique to the calling test such as to ensure there is no
/// stealing. `visibility_timeout` sets how long a received message stays hidden
/// from subsequent receives before SQS redelivers it.
async fn make_test_queue_config(visibility_timeout: Option<Duration>) -> SqsConfig {
    for (var, val) in &DEFAULT_CFG {
        if std::env::var(var).is_err() {
            std::env::set_var(var, val);
        }
    }

    let config = aws_config::from_env().endpoint_url(ROOT_URL).load().await;
    let client = Client::new(&config);

    let queue_name: String = std::iter::repeat_with(fastrand::alphanumeric)
        .take(8)
        .collect();
    let mut create_queue = client.create_queue().queue_name(&queue_name);
    if let Some(visibility_timeout) = visibility_timeout {
        create_queue = create_queue.attributes(
            QueueAttributeName::VisibilityTimeout,
            visibility_timeout.as_secs().to_string(),
        );
    }
    create_queue.send().await.unwrap();

    SqsConfig {
        queue_dsn: format!("{ROOT_URL}/queue/{queue_name}"),
        override_endpoint: true,
    }
}

/// Returns a [`QueueBuilder`] configured to connect to a fresh temporary queue.
///
/// See [`make_test_queue_config`].
async fn make_test_queue(visibility_timeout: Option<Duration>) -> QueueBuilder<SqsBackend> {
    SqsBackend::builder(make_test_queue_config(visibility_timeout).await)
}

/// A hand-built `aws_sdk_sqs::Config` addressing `endpoint_url`.
///
/// `Config::builder` reads nothing from the environment, so the region and
/// credentials have to be supplied here even though they repeat what
/// `DEFAULT_CFG` puts into the environment.
fn sdk_config(endpoint_url: &str) -> aws_sdk_sqs::Config {
    use aws_sdk_sqs::config::{BehaviorVersion, Credentials, Region};

    aws_sdk_sqs::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("localhost"))
        .credentials_provider(Credentials::new("x", "x", None, None, "test"))
        .endpoint_url(endpoint_url)
        .build()
}

/// Receives `count` deliveries, which SQS hands out at most 10 at a time.
///
/// Gives up early if a receive comes back empty, so that a test asserting on
/// the count fails rather than hangs.
async fn receive_n(c: &SqsConsumer, count: usize) -> Vec<Delivery> {
    let timeout = Duration::from_secs(1);
    let mut deliveries = Vec::with_capacity(count);

    while deliveries.len() < count {
        let batch = c.receive_all(10, timeout).await.unwrap();
        if batch.is_empty() {
            break;
        }
        deliveries.extend(batch);
    }

    deliveries
}

#[tokio::test]
async fn test_raw_send_recv() {
    let payload = "{\"test\": \"data\"}";
    let (p, c) = make_test_queue(None).await.build_pair().await.unwrap();

    p.send_raw(payload).await.unwrap();

    let d = c.receive().await.unwrap();
    assert_eq!(d.borrow_payload().unwrap(), payload.as_bytes());
}

#[tokio::test]
async fn test_bytes_send_recv() {
    use omniqueue::QueueProducer as _;

    let payload = b"hello";
    let (p, c) = make_test_queue(None).await.build_pair().await.unwrap();

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
    let (p, c) = make_test_queue(None).await.build_pair().await.unwrap();

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
    let (p, c) = make_test_queue(None).await.build_pair().await.unwrap();

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
    let (p, c) = make_test_queue(None).await.build_pair().await.unwrap();

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
    let (p, c) = make_test_queue(None).await.build_pair().await.unwrap();

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
    assert!(now2.elapsed() < timeout);
}

/// Consumer will NOT wait indefinitely for at least one item.
#[tokio::test]
async fn test_send_recv_all_late_arriving_items() {
    let (_p, c) = make_test_queue(None).await.build_pair().await.unwrap();

    let timeout = Duration::from_secs(1);
    let now = Instant::now();
    let xs = c.receive_all(2, timeout).await.unwrap();
    let elapsed = now.elapsed();

    assert_eq!(xs.len(), 0);
    // Elapsed should be around the timeout, ballpark
    assert!(elapsed >= timeout);
    assert!(elapsed <= timeout + Duration::from_millis(200));
}

#[tokio::test]
async fn test_scheduled() {
    let payload1 = ExType { a: 1 };
    let (p, c) = make_test_queue(None).await.build_pair().await.unwrap();

    let delay = Duration::from_secs(3);
    let now = Instant::now();
    p.send_serde_json_scheduled(&payload1, delay).await.unwrap();
    let delivery = c
        .receive_all(1, delay * 2)
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    assert!(now.elapsed() >= delay);
    assert!(now.elapsed() < delay * 2);
    assert_eq!(Some(payload1), delivery.payload_serde_json().unwrap());
}

/// Unlike most backends, SQS has no way to explicitly return a message to the
/// queue: `nack` is a no-op, and redelivery happens only once the visibility
/// timeout elapses.
#[tokio::test]
async fn test_nack_is_a_noop_and_redelivery_awaits_visibility_timeout() {
    let payload = ExType { a: 1 };
    let (p, c) = make_test_queue(Some(VISIBILITY_TIMEOUT))
        .await
        .build_pair()
        .await
        .unwrap();

    p.send_serde_json(&payload).await.unwrap();

    let d = c.receive().await.unwrap();
    assert_eq!(d.payload_serde_json::<ExType>().unwrap().unwrap(), payload);
    d.nack().await.unwrap();

    // The nack did not put the message back: it is still invisible.
    assert_matches!(c.receive().await.unwrap_err(), QueueError::NoData);

    // Once the visibility timeout lapses, the message comes back on its own.
    tokio::time::sleep(VISIBILITY_TIMEOUT + Duration::from_millis(500)).await;

    let d = c.receive().await.unwrap();
    assert_eq!(d.payload_serde_json::<ExType>().unwrap().unwrap(), payload);
    d.ack().await.unwrap();
}

/// An acked message is deleted, so it is not redelivered once the visibility
/// timeout elapses.
#[tokio::test]
async fn test_ack_prevents_redelivery() {
    let payload = ExType { a: 2 };
    let (p, c) = make_test_queue(Some(VISIBILITY_TIMEOUT))
        .await
        .build_pair()
        .await
        .unwrap();

    p.send_serde_json(&payload).await.unwrap();

    let d = c.receive().await.unwrap();
    d.ack().await.unwrap();

    tokio::time::sleep(VISIBILITY_TIMEOUT + Duration::from_millis(500)).await;

    assert_matches!(c.receive().await.unwrap_err(), QueueError::NoData);
}

/// `set_ack_deadline` maps onto SQS's `ChangeMessageVisibility`, extending the
/// window before the message is redelivered.
#[cfg(feature = "beta")]
#[tokio::test]
async fn test_set_ack_deadline_extends_the_visibility_timeout() {
    let payload = ExType { a: 3 };
    let (p, c) = make_test_queue(Some(VISIBILITY_TIMEOUT))
        .await
        .build_pair()
        .await
        .unwrap();

    p.send_serde_json(&payload).await.unwrap();

    let mut d = c.receive().await.unwrap();
    d.set_ack_deadline(Duration::from_secs(30)).await.unwrap();

    // Well past the queue's own visibility timeout, but inside the extended
    // deadline, so the message must not be redelivered.
    tokio::time::sleep(VISIBILITY_TIMEOUT + Duration::from_millis(500)).await;
    assert_matches!(c.receive().await.unwrap_err(), QueueError::NoData);

    d.ack().await.unwrap();
}

/// `send_raw_batch` is overridden for SQS to use `SendMessageBatch`, which
/// accepts at most 10 entries per request, so larger batches are chunked.
#[tokio::test]
async fn test_send_raw_batch_chunks_larger_batches() {
    use omniqueue::QueueProducer as _;

    // Deliberately spans three chunks: 10 + 10 + 5.
    const COUNT: usize = 25;

    let (p, c) = make_test_queue(None).await.build_pair().await.unwrap();

    // `Item: AsRef<Self::Payload>` is satisfied by `Box<String>`, which is what
    // `QueuePayload::from_bytes_naive` hands back.
    let payloads: Vec<Box<String>> = (0..COUNT)
        .map(|i| Box::new(format!("payload-{i}")))
        .collect();
    p.send_raw_batch(payloads.clone()).await.unwrap();

    let deliveries = receive_n(&c, COUNT).await;
    assert_eq!(deliveries.len(), COUNT);

    let mut received: Vec<String> = deliveries
        .iter()
        .map(|d| String::from_utf8(d.borrow_payload().unwrap().to_owned()).unwrap())
        .collect();
    received.sort();

    let mut expected: Vec<String> = payloads.into_iter().map(|p| *p).collect();
    expected.sort();

    assert_eq!(received, expected);
}

/// The same chunking applies to `send_serde_json_batch`.
#[tokio::test]
async fn test_send_serde_json_batch_chunks_larger_batches() {
    use omniqueue::QueueProducer as _;

    // Deliberately spans three chunks: 10 + 10 + 5.
    const COUNT: usize = 25;

    let (p, c) = make_test_queue(None).await.build_pair().await.unwrap();

    let payloads: Vec<ExType> = (0..COUNT).map(|i| ExType { a: i as u8 }).collect();
    p.send_serde_json_batch(payloads).await.unwrap();

    let deliveries = receive_n(&c, COUNT).await;
    assert_eq!(deliveries.len(), COUNT);

    let mut received: Vec<u8> = deliveries
        .iter()
        .map(|d| d.payload_serde_json::<ExType>().unwrap().unwrap().a)
        .collect();
    received.sort_unstable();

    let expected: Vec<u8> = (0..COUNT as u8).collect();
    assert_eq!(received, expected);
}

/// A payload over the configured limit is rejected locally, without ever being
/// handed to SQS.
#[tokio::test]
async fn test_send_raw_rejects_oversized_payload() {
    let (p, c) = make_test_queue(None)
        .await
        .max_payload_size(8)
        .build_pair()
        .await
        .unwrap();

    let err = p.send_raw("123456789").await.unwrap_err();
    assert_matches!(
        err,
        QueueError::PayloadTooLarge {
            limit: 8,
            actual: 9
        }
    );

    assert_matches!(c.receive().await.unwrap_err(), QueueError::NoData);
}

/// For batches, every payload is size-checked before the first chunk is
/// submitted, so one oversized entry means nothing is sent at all.
#[tokio::test]
async fn test_send_raw_batch_rejects_oversized_payload_before_sending() {
    use omniqueue::QueueProducer as _;

    let (p, c) = make_test_queue(None)
        .await
        .max_payload_size(8)
        .build_pair()
        .await
        .unwrap();

    // The oversized payload lands in the second chunk of 10, so a check done
    // per-chunk rather than up front would already have sent the first chunk.
    let mut payloads: Vec<Box<String>> = (0..10).map(|i| Box::new(format!("ok-{i}"))).collect();
    payloads.push(Box::new("far too large".to_owned()));

    let err = p.send_raw_batch(payloads).await.unwrap_err();
    assert_matches!(err, QueueError::PayloadTooLarge { .. });

    // Not even the first chunk went out.
    assert_matches!(c.receive().await.unwrap_err(), QueueError::NoData);
}

/// A delay too large for SQS's `i32` delay seconds is rejected without a round
/// trip.
#[tokio::test]
async fn test_send_scheduled_rejects_too_large_delay() {
    let (p, _c) = make_test_queue(None).await.build_pair().await.unwrap();

    let err = p
        .send_raw_scheduled("{}", Duration::from_secs(u32::MAX.into()))
        .await
        .unwrap_err();
    assert_matches!(err, QueueError::Generic(_));
}

/// `send_bytes_batch` funnels into the overridden `send_raw_batch`, so it is
/// chunked the same way. It is also the batch entry point that accepts ordinary
/// owned payloads.
#[tokio::test]
async fn test_send_bytes_batch_chunks_larger_batches() {
    use omniqueue::QueueProducer as _;

    // Deliberately spans three chunks: 10 + 10 + 5.
    const COUNT: usize = 25;

    let (p, c) = make_test_queue(None).await.build_pair().await.unwrap();

    let payloads: Vec<Vec<u8>> = (0..COUNT)
        .map(|i| format!("payload-{i}").into_bytes())
        .collect();
    p.send_bytes_batch(payloads.clone()).await.unwrap();

    let deliveries = receive_n(&c, COUNT).await;
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

/// A deadline too large for SQS's `i32` visibility timeout is rejected without
/// a round trip.
#[cfg(feature = "beta")]
#[tokio::test]
async fn test_set_ack_deadline_rejects_too_large_duration() {
    let payload = ExType { a: 7 };
    let (p, c) = make_test_queue(None).await.build_pair().await.unwrap();

    p.send_serde_json(&payload).await.unwrap();

    let mut d = c.receive().await.unwrap();
    let err = d
        .set_ack_deadline(Duration::from_secs(u32::MAX.into()))
        .await
        .unwrap_err();
    assert_matches!(err, QueueError::Generic(_));
}

/// `override_endpoint` points the AWS client at the queue DSN. A bare DSN
/// otherwise leaves it false, deferring to the environment.
#[tokio::test]
async fn test_override_endpoint_config_option() {
    let payload = ExType { a: 8 };
    let queue_dsn = make_test_queue_config(None).await.queue_dsn;

    let (p, c) = SqsBackend::builder(queue_dsn)
        .override_endpoint(true)
        .build_pair()
        .await
        .unwrap();

    p.send_serde_json(&payload).await.unwrap();

    let d = c.receive().await.unwrap();
    assert_eq!(d.payload_serde_json::<ExType>().unwrap().unwrap(), payload);
    d.ack().await.unwrap();
}

/// An explicitly supplied `aws_sdk_sqs::Config` is used in place of the
/// environment.
///
/// The endpoint is what makes that observable: the fallback in
/// `take_sqs_config` calls `load_from_env`, which sets no endpoint at all, so
/// dropping `sqs_config` here points the client at real AWS, where the region
/// fails to resolve.
#[tokio::test]
async fn test_sqs_config_is_used_instead_of_the_environment() {
    let payload = ExType { a: 9 };
    let queue_dsn = make_test_queue_config(None).await.queue_dsn;

    let (p, c) = SqsBackend::builder(queue_dsn)
        .sqs_config(sdk_config(ROOT_URL))
        .build_pair()
        .await
        .unwrap();

    p.send_serde_json(&payload).await.unwrap();

    let d = c.receive().await.unwrap();
    assert_eq!(d.payload_serde_json::<ExType>().unwrap().unwrap(), payload);
    d.ack().await.unwrap();
}

/// `take_sqs_config` consults `sqs_config` before `override_endpoint`, so an
/// explicit config wins even when `override_endpoint` would have pointed the
/// client somewhere that works.
#[tokio::test]
async fn test_sqs_config_takes_precedence_over_override_endpoint() {
    let payload = ExType { a: 10 };
    let queue_dsn = make_test_queue_config(None).await.queue_dsn;

    // Nothing is listening on port 1. A failed send therefore means the explicit
    // config is what reached the wire: `override_endpoint` on its own would have
    // addressed the live queue.
    let p = SqsBackend::builder(queue_dsn.clone())
        .override_endpoint(true)
        .sqs_config(sdk_config("http://127.0.0.1:1"))
        .build_producer()
        .await
        .unwrap();

    assert_matches!(
        p.send_serde_json(&payload).await.unwrap_err(),
        QueueError::Generic(_)
    );

    // Positive control: the same builder without `sqs_config` does reach the
    // queue, so the failure above is precedence and not a broken fixture.
    let (p, c) = SqsBackend::builder(queue_dsn)
        .override_endpoint(true)
        .build_pair()
        .await
        .unwrap();

    p.send_serde_json(&payload).await.unwrap();

    let d = c.receive().await.unwrap();
    assert_eq!(d.payload_serde_json::<ExType>().unwrap().unwrap(), payload);
    d.ack().await.unwrap();
}

/// Unlike the in-memory backend, SQS supports building each half on its own.
#[tokio::test]
async fn test_producing_and_consuming_halves_can_be_built_separately() {
    let payload = ExType { a: 4 };
    let config = make_test_queue_config(None).await;

    let p = SqsBackend::builder(config.clone())
        .build_producer()
        .await
        .unwrap();
    let c = SqsBackend::builder(config).build_consumer().await.unwrap();

    p.send_serde_json(&payload).await.unwrap();

    let d = c.receive().await.unwrap();
    assert_eq!(d.payload_serde_json::<ExType>().unwrap().unwrap(), payload);
    d.ack().await.unwrap();
}

#[tokio::test]
async fn test_redrive_dlq_unsupported() {
    let (p, _c) = make_test_queue(None).await.build_pair().await.unwrap();

    let err = p.redrive_dlq().await.unwrap_err();
    assert_matches!(err, QueueError::Unsupported(_));
}

/// SQS caps `ReceiveMessage` at 10 messages per call.
#[tokio::test]
async fn test_max_messages() {
    use omniqueue::QueueConsumer as _;

    let (_p, c) = make_test_queue(None).await.build_pair().await.unwrap();

    assert_eq!(c.max_messages(), NonZeroUsize::new(10));
}
