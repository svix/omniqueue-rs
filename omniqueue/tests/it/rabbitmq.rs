use lapin::options::ExchangeDeclareOptions;
use lapin::types::AMQPValue;
use lapin::{
    options::{BasicConsumeOptions, BasicPublishOptions, QueueDeclareOptions},
    types::FieldTable,
    BasicProperties, Connection, ConnectionProperties, ExchangeKind,
};
use omniqueue::{
    backends::{RabbitMqBackend, RabbitMqConfig},
    QueueBuilder, QueueConsumer, QueueProducer, ScheduledQueueProducer,
};
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};

const MQ_URI: &str = "amqp://guest:guest@localhost:5672/%2f";

/// Returns a [`QueueBuilder`] configured to connect to the RabbitMQ instance spawned by the file
/// `testing-docker-compose.yaml` in the root of the repository.
///
/// Additionally this will make a temporary queue on that instance for the duration of the test such
/// as to ensure there is no stealing.w
async fn make_test_queue(
    prefetch_count: Option<u16>,
    reinsert_on_nack: bool,
) -> QueueBuilder<RabbitMqBackend> {
    let options = ConnectionProperties::default()
        .with_connection_name(
            std::iter::repeat_with(fastrand::alphanumeric)
                .take(8)
                .collect::<String>()
                .into(),
        )
        .with_executor(tokio_executor_trait::Tokio::current())
        .with_reactor(tokio_reactor_trait::Tokio);
    let connection = Connection::connect(MQ_URI, options.clone()).await.unwrap();
    let channel = connection.create_channel().await.unwrap();

    let queue_name: String = std::iter::repeat_with(fastrand::alphanumeric)
        .take(8)
        .collect();

    channel
        .queue_declare(
            &queue_name,
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
            DELAY_EXCHANGE,
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
            &queue_name,
            DELAY_EXCHANGE,
            &queue_name,
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
    let payload = "hello";
    let (p, mut c) = make_test_queue(None, false)
        .await
        .build_pair()
        .await
        .unwrap();

    p.send_raw(payload).await.unwrap();

    let d = c.receive().await.unwrap();
    assert_eq!(d.borrow_payload().unwrap(), payload);
    d.ack().await.unwrap();
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct ExType {
    a: char,
}

#[tokio::test]
async fn test_serde_send_recv() {
    let payload = ExType { a: '2' };
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

#[tokio::test]
async fn test_custom_send_recv() {
    let payload = ExType { a: '3' };

    let encoder = |p: &ExType| Ok([p.a].into_iter().collect());
    let decoder = |p: &str| {
        Ok(ExType {
            a: p.chars().next().unwrap_or('\0'),
        })
    };

    let (p, mut c) = make_test_queue(None, false)
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
    let payload = ExType { a: '2' };
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

/// Consumer should yield items immediately if there's a full batch ready on the first poll.
#[tokio::test]
async fn test_send_recv_all_full() {
    let payload1 = ExType { a: '1' };
    let payload2 = ExType { a: '2' };
    let (p, mut c) = make_test_queue(None, false)
        .await
        .build_pair()
        .await
        .unwrap();

    p.send_serde_json(&payload1).await.unwrap();
    p.send_serde_json(&payload2).await.unwrap();

    // XXX: rabbit's receive_all impl relies on stream items to be in a ready state in order for
    // them to be batched together. Sleeping to help them settle before we poll.
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
    // N.b. it's still possible this could turn up false if the test runs too slow.
    assert!(now.elapsed() < deadline);
}

/// Consumer will return the full batch immediately, but also return immediately if a partial batch is ready.
#[tokio::test]
async fn test_send_recv_all_full_then_partial() {
    let payload1 = ExType { a: '1' };
    let payload2 = ExType { a: '2' };
    let payload3 = ExType { a: '3' };
    let (p, mut c) = make_test_queue(None, false)
        .await
        .build_pair()
        .await
        .unwrap();

    p.send_serde_json(&payload1).await.unwrap();
    p.send_serde_json(&payload2).await.unwrap();
    p.send_serde_json(&payload3).await.unwrap();

    // XXX: rabbit's receive_all impl relies on stream items to be in a ready state in order for
    // them to be batched together. Sleeping to help them settle before we poll.
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
    let payload1 = ExType { a: '1' };
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
