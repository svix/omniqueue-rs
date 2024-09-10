use std::time::{Duration, Instant};

use bb8_redis::RedisConnectionManager;
#[cfg(feature = "redis_sentinel")]
use omniqueue::backends::redis::RedisSentinelConnectionManager;
use omniqueue::{
    backends::{
        redis::{DeadLetterQueueConfig, RedisBackendBuilder, RedisConnection, SentinelConfig},
        RedisBackend, RedisConfig,
    },
    Delivery,
};
use redis::{AsyncCommands, Client, Commands};
use rstest::rstest;
use serde::{Deserialize, Serialize};

const ROOT_URL: &str = "redis://localhost";
#[cfg(feature = "redis_sentinel")]
const SENTINEL_ROOT_URL: &str = "redis://localhost:26379";

pub struct RedisStreamDrop(String);
impl Drop for RedisStreamDrop {
    fn drop(&mut self) {
        let client = Client::open(ROOT_URL).unwrap();
        let mut conn = client.get_connection().unwrap();
        let _: () = conn.del(&self.0).unwrap();
    }
}

/// Returns a [`QueueBuilder`] configured to connect to the Redis instance
/// spawned by the file `testing-docker-compose.yaml` in the root of the
/// repository.
///
/// Additionally this will make a temporary stream on that instance for the
/// duration of the test such as to ensure there is no stealing
///
/// This will also return a [`RedisStreamDrop`] to clean up the stream after the
/// test ends.
async fn make_test_queue<R: RedisConnection>(
    dsn: String,
) -> (RedisBackendBuilder<R>, RedisStreamDrop) {
    let stream_name: String = std::iter::repeat_with(fastrand::alphanumeric)
        .take(8)
        .collect();

    #[cfg(feature = "redis")]
    {
        let client = Client::open(ROOT_URL).unwrap();
        let mut conn = client.get_multiplexed_async_connection().await.unwrap();
        let _: () = conn
            .xgroup_create_mkstream(&stream_name, "test_cg", 0i8)
            .await
            .unwrap();
    }

    #[cfg(feature = "redis_sentinel")]
    {
        let mut client = redis::sentinel::SentinelClient::build(
            vec![SENTINEL_ROOT_URL],
            "master0".to_string(),
            None,
            redis::sentinel::SentinelServerType::Master,
        )
        .unwrap();
        let mut conn = client.get_async_connection().await.unwrap();
        let _: () = conn
            .xgroup_create_mkstream(&stream_name, "test_cg", 0i8)
            .await
            .unwrap();
    }
    let config = RedisConfig {
        dsn,
        max_connections: 8,
        reinsert_on_nack: false,
        queue_key: stream_name.clone(),
        delayed_queue_key: format!("{stream_name}::delayed"),
        delayed_lock_key: format!("{stream_name}::delayed_lock"),
        consumer_group: "test_cg".to_owned(),
        consumer_name: "test_cn".to_owned(),
        payload_key: "payload".to_owned(),
        ack_deadline_ms: 5_000,
        dlq_config: None,
        sentinel_config: Some(SentinelConfig {
            service_name: "master0".to_owned(),
            redis_tls_mode_secure: false,
            redis_db: None,
            redis_username: None,
            redis_password: None,
            redis_use_resp3: true,
        }),
    };

    (
        RedisBackendBuilder::new(config),
        RedisStreamDrop(stream_name),
    )
}

#[rstest]
#[cfg_attr(feature = "redis", case(async { make_test_queue::<RedisConnectionManager>(ROOT_URL.to_owned()).await }))]
#[cfg_attr(feature = "redis_sentinel", case(async { make_test_queue::<RedisSentinelConnectionManager>(SENTINEL_ROOT_URL.to_owned()).await }))]
#[tokio::test]
async fn test_raw_send_recv<R: RedisConnection>(
    #[future]
    #[case]
    get_builder: (RedisBackendBuilder<R>, RedisStreamDrop),
) {
    let (builder, _drop) = get_builder.await;
    let payload = b"{\"test\": \"data\"}";
    let (p, mut c) = builder.build_pair().await.unwrap();

    p.send_raw(payload).await.unwrap();

    let d = c.receive().await.unwrap();
    assert_eq!(d.borrow_payload().unwrap(), payload);
}

#[rstest]
#[cfg_attr(feature = "redis", case(async { make_test_queue::<RedisConnectionManager>(ROOT_URL.to_owned()).await }))]
#[cfg_attr(feature = "redis_sentinel", case(async { make_test_queue::<RedisSentinelConnectionManager>(SENTINEL_ROOT_URL.to_owned()).await }))]
#[tokio::test]
async fn test_bytes_send_recv<R: RedisConnection>(
    #[future]
    #[case]
    get_builder: (RedisBackendBuilder<R>, RedisStreamDrop),
) {
    use omniqueue::QueueProducer as _;

    let (builder, _drop) = get_builder.await;
    let payload = b"hello";
    let (p, mut c) = builder.build_pair().await.unwrap();

    p.send_bytes(payload).await.unwrap();

    let d = c.receive().await.unwrap();
    assert_eq!(d.borrow_payload().unwrap(), payload);
    d.ack().await.unwrap();
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct ExType {
    a: u8,
}
#[rstest]
#[cfg_attr(feature = "redis", case(async { make_test_queue::<RedisConnectionManager>(ROOT_URL.to_owned()).await }))]
#[cfg_attr(feature = "redis_sentinel", case(async { make_test_queue::<RedisSentinelConnectionManager>(SENTINEL_ROOT_URL.to_owned()).await }))]
#[tokio::test]
async fn test_serde_send_recv<R: RedisConnection>(
    #[future]
    #[case]
    get_builder: (RedisBackendBuilder<R>, RedisStreamDrop),
) {
    let (builder, _drop) = get_builder.await;
    let payload = ExType { a: 2 };
    let (p, mut c) = builder.build_pair().await.unwrap();

    p.send_serde_json(&payload).await.unwrap();

    let d = c.receive().await.unwrap();
    assert_eq!(d.payload_serde_json::<ExType>().unwrap().unwrap(), payload);
    d.ack().await.unwrap();
}

/// Consumer will return immediately if there are fewer than max messages to
/// start with.
#[rstest]
#[cfg_attr(feature = "redis", case(async { make_test_queue::<RedisConnectionManager>(ROOT_URL.to_owned()).await }))]
#[cfg_attr(feature = "redis_sentinel", case(async { make_test_queue::<RedisSentinelConnectionManager>(SENTINEL_ROOT_URL.to_owned()).await }))]
#[tokio::test]
async fn test_send_recv_all_partial<R: RedisConnection>(
    #[future]
    #[case]
    get_builder: (RedisBackendBuilder<R>, RedisStreamDrop),
) {
    let (builder, _drop) = get_builder.await;

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

/// Consumer should yield items immediately if there's a full batch ready on the
/// first poll.
#[rstest]
#[cfg_attr(feature = "redis", case(async { make_test_queue::<RedisConnectionManager>(ROOT_URL.to_owned()).await }))]
#[cfg_attr(feature = "redis_sentinel", case(async { make_test_queue::<RedisSentinelConnectionManager>(SENTINEL_ROOT_URL.to_owned()).await }))]
#[tokio::test]
async fn test_send_recv_all_full<R: RedisConnection>(
    #[future]
    #[case]
    get_builder: (RedisBackendBuilder<R>, RedisStreamDrop),
) {
    let payload1 = ExType { a: 1 };
    let payload2 = ExType { a: 2 };

    let (builder, _drop) = get_builder.await;

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
    // N.b. it's still possible this could turn up false if the test runs too
    // slow.
    assert!(now.elapsed() < deadline);
}

/// Consumer will return the full batch immediately, but also return immediately
/// if a partial batch is ready.
#[rstest]
#[cfg_attr(feature = "redis", case(async { make_test_queue::<RedisConnectionManager>(ROOT_URL.to_owned()).await }))]
#[cfg_attr(feature = "redis_sentinel", case(async { make_test_queue::<RedisSentinelConnectionManager>(SENTINEL_ROOT_URL.to_owned()).await }))]
#[tokio::test]
async fn test_send_recv_all_full_then_partial<R: RedisConnection>(
    #[future]
    #[case]
    get_builder: (RedisBackendBuilder<R>, RedisStreamDrop),
) {
    let (builder, _drop) = get_builder.await;

    let payload1 = ExType { a: 1 };
    let payload2 = ExType { a: 2 };
    let payload3 = ExType { a: 3 };

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
#[rstest]
#[cfg_attr(feature = "redis", case(async { make_test_queue::<RedisConnectionManager>(ROOT_URL.to_owned()).await }))]
#[cfg_attr(feature = "redis_sentinel", case(async { make_test_queue::<RedisSentinelConnectionManager>(SENTINEL_ROOT_URL.to_owned()).await }))]
#[tokio::test]
async fn test_send_recv_all_late_arriving_items<R: RedisConnection>(
    #[future]
    #[case]
    get_builder: (RedisBackendBuilder<R>, RedisStreamDrop),
) {
    let (builder, _drop) = get_builder.await;

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

#[rstest]
#[cfg_attr(feature = "redis", case(async { make_test_queue::<RedisConnectionManager>(ROOT_URL.to_owned()).await }))]
#[cfg_attr(feature = "redis_sentinel", case(async { make_test_queue::<RedisSentinelConnectionManager>(SENTINEL_ROOT_URL.to_owned()).await }))]
#[tokio::test]
async fn test_scheduled<R: RedisConnection>(
    #[future]
    #[case]
    get_builder: (RedisBackendBuilder<R>, RedisStreamDrop),
) {
    let payload1 = ExType { a: 1 };
    let (builder, _drop) = get_builder.await;

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

#[rstest]
#[cfg_attr(feature = "redis", case(async { make_test_queue::<RedisConnectionManager>(ROOT_URL.to_owned()).await }))]
#[cfg_attr(feature = "redis_sentinel", case(async { make_test_queue::<RedisSentinelConnectionManager>(SENTINEL_ROOT_URL.to_owned()).await }))]
#[tokio::test]
async fn test_pending<R: RedisConnection>(
    #[future]
    #[case]
    get_builder: (RedisBackendBuilder<R>, RedisStreamDrop),
) {
    let payload1 = ExType { a: 1 };
    let payload2 = ExType { a: 2 };
    let (builder, _drop) = get_builder.await;

    let (p, mut c) = builder.build_pair().await.unwrap();

    p.send_serde_json(&payload1).await.unwrap();
    p.send_serde_json(&payload2).await.unwrap();
    let delivery1 = c.receive().await.unwrap();
    let delivery2 = c.receive().await.unwrap();

    // All items claimed, but not yet ack'd. There shouldn't be anything available
    // yet.
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

#[tokio::test]
async fn test_deadletter_config() {
    let payload = ExType { a: 1 };
    let payload_str = serde_json::to_string(&payload).unwrap();

    let stream_name: String = std::iter::repeat_with(fastrand::alphanumeric)
        .take(8)
        .collect();

    let dlq_key: String = std::iter::repeat_with(fastrand::alphanumeric)
        .take(8)
        .collect();

    let client = Client::open(ROOT_URL).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();

    let _: () = conn
        .xgroup_create_mkstream(&stream_name, "test_cg", 0i8)
        .await
        .unwrap();

    let max_receives = 5;

    let config = RedisConfig {
        dsn: ROOT_URL.to_owned(),
        max_connections: 8,
        reinsert_on_nack: false,
        queue_key: stream_name.clone(),
        delayed_queue_key: format!("{stream_name}::delayed"),
        delayed_lock_key: format!("{stream_name}::delayed_lock"),
        consumer_group: "test_cg".to_owned(),
        consumer_name: "test_cn".to_owned(),
        payload_key: "payload".to_owned(),
        ack_deadline_ms: 20,
        dlq_config: Some(DeadLetterQueueConfig {
            queue_key: dlq_key.to_owned(),
            max_receives,
        }),
        sentinel_config: None,
    };

    let check_dlq = |asserted_len: usize| {
        let dlq_key = dlq_key.clone();
        async move {
            let client = Client::open(ROOT_URL).unwrap();
            let mut conn = client.get_multiplexed_async_connection().await.unwrap();
            let mut res: Vec<String> = conn.lrange(&dlq_key, 0, 0).await.unwrap();
            assert!(res.len() == asserted_len);
            res.pop()
        }
    };

    let (builder, _drop) = (
        RedisBackend::builder(config),
        RedisStreamDrop(stream_name.clone()),
    );

    let (p, mut c) = builder.build_pair().await.unwrap();

    // Test send to DLQ via `ack_deadline_ms` expiration:
    p.send_serde_json(&payload).await.unwrap();

    for _ in 0..max_receives {
        check_dlq(0).await;
        let delivery = c.receive().await.unwrap();
        assert_eq!(
            Some(&payload),
            delivery.payload_serde_json().unwrap().as_ref()
        );
    }

    // Give this some time because the reenqueuing can sleep for up to 500ms
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    let delivery = c
        .receive_all(1, std::time::Duration::from_millis(1))
        .await
        .unwrap();
    assert!(delivery.is_empty());

    // Expected message should be on DLQ:
    let res = check_dlq(1).await;
    assert_eq!(payload_str, res.unwrap());

    // Test send to DLQ via explicit `nack`ing:
    let _: () = conn
        .xadd(&stream_name, "*", &[("payload", payload_str.as_bytes())])
        .await
        .unwrap();

    let assert_delivery = |delivery: &Delivery| {
        assert_eq!(
            Some(&payload),
            delivery.payload_serde_json().unwrap().as_ref()
        );
    };

    for _ in 0..max_receives {
        let delivery = c.receive().await.unwrap();
        assert_delivery(&delivery);
        delivery.nack().await.unwrap();
    }

    // Give this some time because the reenqueuing can sleep for up to 500ms
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    let delivery = c
        .receive_all(1, std::time::Duration::from_millis(1))
        .await
        .unwrap();
    assert!(delivery.is_empty());

    // Expected message should be on DLQ:
    let res = check_dlq(1).await;
    assert_eq!(payload_str, res.unwrap());

    // Redrive DLQ, receive from main queue, ack:
    p.redrive_dlq().await.unwrap();

    let delivery = c.receive().await.unwrap();
    assert_delivery(&delivery);
    delivery.ack().await.unwrap();

    check_dlq(0).await;
}

// Assert that ordering is as expected. I don't know
// that we need to guarantee this in our docs, but it's
// good to at least validate it for now:
#[tokio::test]
async fn test_deadletter_config_order() {
    let payload1 = ExType { a: 1 };
    let payload2 = ExType { a: 2 };
    let payload3 = ExType { a: 3 };

    let stream_name: String = std::iter::repeat_with(fastrand::alphanumeric)
        .take(8)
        .collect();

    let dlq_key: String = std::iter::repeat_with(fastrand::alphanumeric)
        .take(8)
        .collect();

    let client = Client::open(ROOT_URL).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();

    let _: () = conn
        .xgroup_create_mkstream(&stream_name, "test_cg", 0i8)
        .await
        .unwrap();

    let max_receives = 1;

    let config = RedisConfig {
        dsn: ROOT_URL.to_owned(),
        max_connections: 8,
        reinsert_on_nack: false,
        queue_key: stream_name.clone(),
        delayed_queue_key: format!("{stream_name}::delayed"),
        delayed_lock_key: format!("{stream_name}::delayed_lock"),
        consumer_group: "test_cg".to_owned(),
        consumer_name: "test_cn".to_owned(),
        payload_key: "payload".to_owned(),
        ack_deadline_ms: 20,
        dlq_config: Some(DeadLetterQueueConfig {
            queue_key: dlq_key.to_owned(),
            max_receives,
        }),
        sentinel_config: None,
    };

    let check_dlq = |asserted_len: usize| {
        let dlq_key = dlq_key.clone();
        async move {
            let client = Client::open(ROOT_URL).unwrap();
            let mut conn = client.get_multiplexed_async_connection().await.unwrap();
            let mut res: Vec<String> = conn.lrange(&dlq_key, 0, -1).await.unwrap();
            assert!(res.len() == asserted_len);
            res.pop()
        }
    };

    let (builder, _drop) = (
        RedisBackend::builder(config),
        RedisStreamDrop(stream_name.clone()),
    );

    let (p, mut c) = builder.build_pair().await.unwrap();

    // Test send to DLQ via `ack_deadline_ms` expiration:
    p.send_serde_json(&payload1).await.unwrap();
    p.send_serde_json(&payload2).await.unwrap();
    p.send_serde_json(&payload3).await.unwrap();

    for payload in [&payload1, &payload2, &payload3] {
        let delivery = c.receive().await.unwrap();
        assert_eq!(
            Some(payload),
            delivery.payload_serde_json().unwrap().as_ref()
        );
        delivery.nack().await.unwrap();
    }

    // Give this some time because the reenqueuing can sleep for up to 500ms
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // Expected messages should be on DLQ:
    check_dlq(3).await;

    // Redrive DLQ, receive from main queue, ack:
    p.redrive_dlq().await.unwrap();

    for payload in [&payload1, &payload2, &payload3] {
        let delivery = c.receive().await.unwrap();
        assert_eq!(
            Some(payload),
            delivery.payload_serde_json().unwrap().as_ref()
        );
        delivery.ack().await.unwrap();
    }
}

// A message without a `num_receives` field shouldn't
// cause issues:
#[tokio::test]
async fn test_backward_compatible() {
    let stream_name: String = std::iter::repeat_with(fastrand::alphanumeric)
        .take(8)
        .collect();

    let dlq_key: String = std::iter::repeat_with(fastrand::alphanumeric)
        .take(8)
        .collect();

    let client = Client::open(ROOT_URL).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();

    let _: () = conn
        .xgroup_create_mkstream(&stream_name, "test_cg", 0i8)
        .await
        .unwrap();

    let max_receives = 5;

    let config = RedisConfig {
        dsn: ROOT_URL.to_owned(),
        max_connections: 8,
        reinsert_on_nack: false,
        queue_key: stream_name.clone(),
        delayed_queue_key: format!("{stream_name}::delayed"),
        delayed_lock_key: format!("{stream_name}::delayed_lock"),
        consumer_group: "test_cg".to_owned(),
        consumer_name: "test_cn".to_owned(),
        payload_key: "payload".to_owned(),
        ack_deadline_ms: 20,
        dlq_config: Some(DeadLetterQueueConfig {
            queue_key: dlq_key.to_owned(),
            max_receives,
        }),
        sentinel_config: None,
    };

    let (builder, _drop) = (
        RedisBackend::builder(config),
        RedisStreamDrop(stream_name.clone()),
    );

    let (_p, mut c) = builder.build_pair().await.unwrap();

    let org_payload = ExType { a: 1 };
    let org_payload_str = serde_json::to_string(&org_payload).unwrap();

    let _: () = conn
        .xadd(
            &stream_name,
            "*",
            // We don't have the `num_receives` field:
            &[("payload", org_payload_str.as_bytes())],
        )
        .await
        .unwrap();

    for _ in 0..max_receives {
        let delivery = c.receive().await.unwrap();
        assert_eq!(
            Some(&org_payload),
            delivery.payload_serde_json().unwrap().as_ref()
        );
    }

    // Give this some time because the reenqueuing can sleep for up to 500ms
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    let delivery = c
        .receive_all(1, std::time::Duration::from_millis(1))
        .await
        .unwrap();
    assert!(delivery.is_empty());
}
