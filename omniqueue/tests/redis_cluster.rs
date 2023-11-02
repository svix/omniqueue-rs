use omniqueue::{
    backends::redis::{RedisClusterQueueBackend, RedisConfig},
    queue::{consumer::QueueConsumer, producer::QueueProducer, QueueBackend, QueueBuilder, Static},
    scheduled::ScheduledProducer,
};
use redis::{cluster::ClusterClient, AsyncCommands, Commands};
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};

const ROOT_URL: &str = "redis://localhost:6380";

pub struct RedisStreamDrop(String);
impl Drop for RedisStreamDrop {
    fn drop(&mut self) {
        let client = ClusterClient::new(vec![ROOT_URL]).unwrap();
        let mut conn = client.get_connection().unwrap();
        let _: () = conn.del(&self.0).unwrap();
    }
}

/// Returns a [`QueueBuilder`] configured to connect to the Redis instance spawned by the file
/// `testing-docker-compose.yaml` in the root of the repository.
///
/// Additionally this will make a temporary stream on that instance for the duration of the test
/// such as to ensure there is no stealing
///
/// This will also return a [`RedisStreamDrop`] to clean up the stream after the test ends.
async fn make_test_queue() -> (
    QueueBuilder<RedisClusterQueueBackend, Static>,
    RedisStreamDrop,
) {
    let stream_name: String = std::iter::repeat_with(fastrand::alphanumeric)
        .take(8)
        .collect();

    let client = ClusterClient::new(vec![ROOT_URL]).unwrap();
    let mut conn = client.get_async_connection().await.unwrap();

    let _: () = conn
        .xgroup_create_mkstream(&stream_name, "test_cg", 0i8)
        .await
        .unwrap();

    let config = RedisConfig {
        dsn: ROOT_URL.to_owned(),
        max_connections: 8,
        reinsert_on_nack: false,
        queue_key: stream_name.clone(),
        delayed_queue_key: format!("{stream_name}::delay"),
        consumer_group: "test_cg".to_owned(),
        consumer_name: "test_cn".to_owned(),
        payload_key: "payload".to_owned(),
        ack_deadline_ms: 5_000,
    };

    (
        RedisClusterQueueBackend::builder(config),
        RedisStreamDrop(stream_name),
    )
}

#[tokio::test]
async fn test_raw_send_recv() {
    let (builder, _drop) = make_test_queue().await;
    let payload = b"{\"test\": \"data\"}";
    let (p, mut c) = builder.build_pair().await.unwrap();

    p.send_raw(&payload.to_vec()).await.unwrap();

    let d = c.receive().await.unwrap();
    assert_eq!(d.borrow_payload().unwrap(), payload);
}

#[tokio::test]
async fn test_bytes_send_recv() {
    let (builder, _drop) = make_test_queue().await;
    let payload = b"hello";
    let (p, mut c) = builder.build_pair().await.unwrap();

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
    let (builder, _drop) = make_test_queue().await;
    let payload = ExType { a: 2 };
    let (p, mut c) = builder.build_pair().await.unwrap();

    p.send_serde_json(&payload).await.unwrap();

    let d = c.receive().await.unwrap();
    assert_eq!(d.payload_serde_json::<ExType>().unwrap().unwrap(), payload);
    d.ack().await.unwrap();
}

#[tokio::test]
async fn test_custom_send_recv() {
    let (builder, _drop) = make_test_queue().await;
    let payload = ExType { a: 3 };

    let encoder = |p: &ExType| Ok(vec![p.a]);
    let decoder = |p: &Vec<u8>| {
        Ok(ExType {
            a: *p.first().unwrap_or(&0),
        })
    };

    let (p, mut c) = builder
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
    let (builder, _drop) = make_test_queue().await;

    let payload = ExType { a: 2 };
    let (p, mut c) = builder.build_pair().await.unwrap();

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

    let (builder, _drop) = make_test_queue().await;

    let (p, mut c) = builder.build_pair().await.unwrap();

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

    let (builder, _drop) = make_test_queue().await;

    let (p, mut c) = builder.build_pair().await.unwrap();

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
    assert!(now2.elapsed() < deadline);
}

/// Consumer will NOT wait indefinitely for at least one item.
#[tokio::test]
async fn test_send_recv_all_late_arriving_items() {
    let (builder, _drop) = make_test_queue().await;

    let (_p, mut c) = builder.build_pair().await.unwrap();

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

    let (builder, _drop) = make_test_queue().await;
    let (p, mut c) = builder.build_pair().await.unwrap();

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

#[tokio::test]
async fn test_pending() {
    let payload1 = ExType { a: 1 };
    let payload2 = ExType { a: 2 };
    let (builder, _drop) = make_test_queue().await;

    let (p, mut c) = builder.build_pair().await.unwrap();

    p.send_serde_json(&payload1).await.unwrap();
    p.send_serde_json(&payload2).await.unwrap();
    let delivery1 = c.receive().await.unwrap();
    let delivery2 = c.receive().await.unwrap();

    // All items claimed, but not yet ack'd. There shouldn't be anything available yet.
    assert!(c
        .receive_all(1, Duration::from_millis(1))
        .await
        .unwrap()
        .is_empty());

    assert_eq!(
        Some(&payload1),
        delivery1.payload_serde_json().unwrap().as_ref()
    );
    assert_eq!(
        Some(&payload2),
        delivery2.payload_serde_json().unwrap().as_ref()
    );

    // ack 2, but neglect 1
    let _ = delivery2.ack().await;

    // After the deadline, the first payload should appear again.
    let delivery3 = c.receive().await.unwrap();
    assert_eq!(
        Some(&payload1),
        delivery3.payload_serde_json().unwrap().as_ref()
    );

    // queue should be empty once again
    assert!(c
        .receive_all(1, Duration::from_millis(1))
        .await
        .unwrap()
        .is_empty());
}
