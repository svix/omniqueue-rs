use std::time::{Duration, Instant};

use lapin::{
    options::{
        BasicConsumeOptions, BasicPublishOptions, ExchangeDeclareOptions, QueueDeclareOptions,
    },
    types::{AMQPValue, FieldTable},
    BasicProperties, Connection, ConnectionProperties, ExchangeKind,
};
use omniqueue::{
    backends::{RabbitMqBackend, RabbitMqConfig},
    QueueBuilder, QueueError,
};
use serde::{Deserialize, Serialize};

const MQ_URI: &str = "amqp://guest:guest@localhost:5672/%2f";

/// Returns a [`QueueBuilder`] configured to connect to the RabbitMQ instance
/// spawned by the file `testing-docker-compose.yaml` in the root of the
/// repository.
///
/// Additionally this will make a temporary queue on that instance for the
/// duration of the test such as to ensure there is no stealing.w
async fn make_test_queue(
    prefetch_count: Option<u16>,
    reinsert_on_nack: bool,
) -> QueueBuilder<RabbitMqBackend> {
    let options = ConnectionProperties::default().with_connection_name(
        std::iter::repeat_with(fastrand::alphanumeric)
            .take(8)
            .collect::<String>()
            .into(),
    );
    let connection = Connection::connect_with_runtime(
        MQ_URI,
        options.clone(),
        async_rs::Runtime::tokio_current(),
    )
    .await
    .unwrap();
    let channel = connection.create_channel().await.unwrap();

    let queue_name: String = std::iter::repeat_with(fastrand::alphanumeric)
        .take(8)
        .collect();

    channel
        .queue_declare(
            queue_name.as_str().into(),
            QueueDeclareOptions {
                auto_delete: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .unwrap();

    const DELAY_EXCHANGE: &str = "later-alligator";
    let mut args = FieldTable::default();
    args.insert(
        "x-delayed-type".into(),
        AMQPValue::LongString("direct".into()),
    );
    channel
        .exchange_declare(
            DELAY_EXCHANGE.into(),
            ExchangeKind::Custom("x-delayed-message".to_string()),
            ExchangeDeclareOptions {
                auto_delete: true,
                ..Default::default()
            },
            args,
        )
        .await
        .unwrap();
    channel
        .queue_bind(
            queue_name.as_str().into(),
            DELAY_EXCHANGE.into(),
            queue_name.as_str().into(),
            Default::default(),
            Default::default(),
        )
        .await
        .unwrap();

    let config = RabbitMqConfig {
        uri: MQ_URI.to_owned(),
        connection_properties: options,
        publish_exchange: DELAY_EXCHANGE.to_string(),
        publish_routing_key: queue_name.clone(),
        publish_options: BasicPublishOptions::default(),
        publish_properties: BasicProperties::default(),
        consume_queue: queue_name,
        consumer_tag: "test".to_owned(),
        consume_options: BasicConsumeOptions::default(),
        consume_arguments: FieldTable::default(),
        consume_prefetch_count: prefetch_count,
        requeue_on_nack: reinsert_on_nack,
    };

    RabbitMqBackend::builder(config)
}

#[tokio::test]
async fn test_bytes_send_recv() {
    use omniqueue::QueueProducer as _;

    let payload = b"hello";
    let (p, mut c) = make_test_queue(None, false)
        .await
        .build_pair()
        .await
        .unwrap();

    p.send_bytes(payload).await.unwrap();

    let d = c.receive().await.unwrap();
    assert_eq!(d.borrow_payload().unwrap(), payload);
    d.ack().await.unwrap();

    // The RabbitMQ native payload type is a Vec<u8>, so we can also send raw
    p.send_raw(payload).await.unwrap();

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
    let (p, mut c) = make_test_queue(None, false)
        .await
        .build_pair()
        .await
        .unwrap();

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
    let (p, mut c) = make_test_queue(None, false)
        .await
        .build_pair()
        .await
        .unwrap();

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

/// Consumer should yield items immediately if there's a full batch ready on the
/// first poll.
#[tokio::test]
async fn test_send_recv_all_full() {
    let payload1 = ExType { a: 1 };
    let payload2 = ExType { a: 2 };
    let (p, mut c) = make_test_queue(None, false)
        .await
        .build_pair()
        .await
        .unwrap();

    p.send_serde_json(&payload1).await.unwrap();
    p.send_serde_json(&payload2).await.unwrap();

    // XXX: rabbit's receive_all impl relies on stream items to be in a ready
    // state in order for them to be batched together. Sleeping to help them
    // settle before we poll.
    tokio::time::sleep(Duration::from_millis(100)).await;
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
    // N.b. it's still possible this could turn up false if the test runs too
    // slow.
    assert!(now.elapsed() < deadline);
}

/// Consumer will return the full batch immediately, but also return immediately
/// if a partial batch is ready.
#[tokio::test]
async fn test_send_recv_all_full_then_partial() {
    let payload1 = ExType { a: 1 };
    let payload2 = ExType { a: 2 };
    let payload3 = ExType { a: 3 };
    let (p, mut c) = make_test_queue(None, false)
        .await
        .build_pair()
        .await
        .unwrap();

    p.send_serde_json(&payload1).await.unwrap();
    p.send_serde_json(&payload2).await.unwrap();
    p.send_serde_json(&payload3).await.unwrap();

    // XXX: rabbit's receive_all impl relies on stream items to be in a ready
    // state in order for them to be batched together. Sleeping to help them
    // settle before we poll.
    tokio::time::sleep(Duration::from_millis(100)).await;

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
    let (_p, mut c) = make_test_queue(None, false)
        .await
        .build_pair()
        .await
        .unwrap();

    let deadline = Duration::from_secs(1);
    let now = Instant::now();
    let xs = c.receive_all(2, deadline).await.unwrap();
    let elapsed = now.elapsed();

    assert_eq!(xs.len(), 0);
    // Elapsed should be around the deadline, ballpark
    assert!(elapsed >= deadline);
    assert!(elapsed <= deadline + Duration::from_millis(200));
}

#[tokio::test]
async fn test_scheduled() {
    let payload1 = ExType { a: 1 };
    let (p, mut c) = make_test_queue(None, false)
        .await
        .build_pair()
        .await
        .unwrap();

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

/// With `requeue_on_nack` enabled, a nacked delivery is put back on the queue
/// and redelivered.
#[tokio::test]
async fn test_nack_requeues() {
    let payload = ExType { a: 42 };
    let (p, mut c) = make_test_queue(None, true)
        .await
        .build_pair()
        .await
        .unwrap();

    p.send_serde_json(&payload).await.unwrap();

    let delivery = c.receive().await.unwrap();
    assert_eq!(
        delivery.payload_serde_json::<ExType>().unwrap().unwrap(),
        payload
    );
    delivery.nack().await.unwrap();

    // The requeued message should be delivered again.
    let redelivery = c.receive().await.unwrap();
    assert_eq!(
        redelivery.payload_serde_json::<ExType>().unwrap().unwrap(),
        payload
    );
    redelivery.ack().await.unwrap();
}

/// With `requeue_on_nack` disabled, a nacked delivery is discarded rather than
/// redelivered.
#[tokio::test]
async fn test_nack_without_requeue_drops() {
    let payload = ExType { a: 42 };
    let (p, mut c) = make_test_queue(None, false)
        .await
        .build_pair()
        .await
        .unwrap();

    p.send_serde_json(&payload).await.unwrap();

    let delivery = c.receive().await.unwrap();
    assert_eq!(
        delivery.payload_serde_json::<ExType>().unwrap().unwrap(),
        payload
    );
    delivery.nack().await.unwrap();

    // Nothing should be redelivered.
    let deadline = Duration::from_secs(1);
    assert!(c.receive_all(1, deadline).await.unwrap().is_empty());
}

/// With a prefetch count of 1, the consumer is not delivered a second message
/// until the first has been acked.
#[tokio::test]
async fn test_prefetch_count_limits_unacked() {
    let payload1 = ExType { a: 1 };
    let payload2 = ExType { a: 2 };
    let (p, mut c) = make_test_queue(Some(1), false)
        .await
        .build_pair()
        .await
        .unwrap();

    p.send_serde_json(&payload1).await.unwrap();
    p.send_serde_json(&payload2).await.unwrap();

    // The first message is delivered.
    let d1 = c.receive().await.unwrap();
    assert_eq!(
        d1.payload_serde_json::<ExType>().unwrap().unwrap(),
        payload1
    );

    // With prefetch=1 and the first still unacked, the second is held back.
    let deadline = Duration::from_secs(1);
    assert!(c.receive_all(1, deadline).await.unwrap().is_empty());

    // Once the first is acked, the second is delivered.
    d1.ack().await.unwrap();
    let d2 = c.receive().await.unwrap();
    assert_eq!(
        d2.payload_serde_json::<ExType>().unwrap().unwrap(),
        payload2
    );
    d2.ack().await.unwrap();
}

/// RabbitMQ does not support redriving a dead-letter queue.
#[tokio::test]
async fn test_redrive_dlq_unsupported() {
    let (p, _c) = make_test_queue(None, false)
        .await
        .build_pair()
        .await
        .unwrap();

    assert!(matches!(
        p.redrive_dlq().await,
        Err(QueueError::Unsupported(_))
    ));
}
