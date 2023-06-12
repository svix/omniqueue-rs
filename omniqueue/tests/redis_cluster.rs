use omniqueue::{
    backends::redis::{RedisClusterQueueBackend, RedisConfig},
    queue::{consumer::QueueConsumer, producer::QueueProducer, QueueBackend, QueueBuilder, Static},
};
use redis::{cluster::ClusterClient, AsyncCommands, Commands};
use serde::{Deserialize, Serialize};

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
        consumer_group: "test_cg".to_owned(),
        consumer_name: "test_cn".to_owned(),
        payload_key: "payload".to_owned(),
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
