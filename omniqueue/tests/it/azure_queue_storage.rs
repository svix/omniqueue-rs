use std::{
    collections::HashSet,
    time::{Duration, Instant},
};

use azure_storage::StorageCredentials;
use azure_storage_queues::QueueServiceClientBuilder;
use omniqueue::{
    backends::{AqsQueueBackend, AqsQueueConfig, AqsQueueConsumer, AqsQueueProducer},
    QueueBackend, QueueError,
};
use serde::{Deserialize, Serialize};

async fn create_queue_get_a_pair() -> (AqsQueueProducer, AqsQueueConsumer) {
    let queue_name: String = std::iter::repeat_with(fastrand::lowercase)
        .take(8)
        .collect();

    let cfg = AqsQueueConfig {
        queue_name,
        empty_receive_delay: Duration::from_millis(1),
        message_ttl: Duration::from_secs(90),
        storage_account: azure_storage::EMULATOR_ACCOUNT.to_string(),
        access_key: azure_storage::EMULATOR_ACCOUNT_KEY.to_string(),
        cloud_uri: Some(format!(
            "http://localhost:10001/{}",
            azure_storage::EMULATOR_ACCOUNT
        )),
    };

    let storage_credentials =
        StorageCredentials::access_key(cfg.storage_account.clone(), cfg.access_key.clone());

    let cli = QueueServiceClientBuilder::new(cfg.storage_account.clone(), storage_credentials)
        .cloud_location(azure_storage::CloudLocation::Custom {
            account: cfg.storage_account.clone(),
            uri: cfg.cloud_uri.clone().unwrap(),
        })
        .build()
        .queue_client(cfg.queue_name.clone());

    cli.create().into_future().await.unwrap();

    AqsQueueBackend::new_pair(cfg).await.unwrap()
}

#[derive(Debug, Deserialize, Serialize, Eq, Hash, PartialEq)]
pub struct ExType {
    a: String,
}

#[tokio::test]
async fn test_raw_send_recv() {
    let (producer, mut consumer) = create_queue_get_a_pair().await;

    let payload = "test123";
    producer.send_raw(payload).await.unwrap();

    let mut d = consumer.receive().await.unwrap();
    assert_eq!(
        payload,
        &String::from_utf8(d.take_payload().unwrap()).unwrap()
    );
    d.ack().await.unwrap();
}

#[tokio::test]
async fn test_serde_send_recv() {
    let (producer, mut consumer) = create_queue_get_a_pair().await;

    let payload = ExType {
        a: "test123".to_string(),
    };
    producer.send_serde_json(&payload).await.unwrap();

    let d = consumer.receive().await.unwrap();
    assert_eq!(d.payload_serde_json::<ExType>().unwrap().unwrap(), payload);
    d.ack().await.unwrap();
}

// Note: Azure Queue Storage doesn't guarantee order of messages, hence
// the HashSet popping and length validation instead of assuming
// particular values:
#[tokio::test]
async fn test_send_recv_all_partial() {
    let (producer, mut consumer) = create_queue_get_a_pair().await;

    let mut res = (0..10usize)
        .map(|i| ExType {
            a: format!("test{i}"),
        })
        .collect::<HashSet<_>>();

    for payload in &res {
        producer.send_serde_json(payload).await.unwrap();
    }

    // Receive more than was sent, should return immediately
    let now = Instant::now();
    let deadline = Duration::from_secs(5);
    let d = consumer.receive_all(20, deadline).await.unwrap();
    assert!(now.elapsed() < deadline);
    assert_eq!(d.len(), 10);
    for i in d {
        res.remove(&i.payload_serde_json::<ExType>().unwrap().unwrap());
        i.ack().await.unwrap();
    }
}

#[tokio::test]
async fn test_send_recv_all_full() {
    let (producer, mut consumer) = create_queue_get_a_pair().await;

    let mut res = (0..10usize)
        .map(|i| ExType {
            a: format!("test{i}"),
        })
        .collect::<HashSet<_>>();

    for payload in &res {
        producer.send_serde_json(payload).await.unwrap();
    }

    let d = consumer
        .receive_all(10, Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(d.len(), 10);
    for i in d {
        res.remove(&i.payload_serde_json::<ExType>().unwrap().unwrap());
        i.ack().await.unwrap();
    }
    assert!(res.is_empty());
}

#[tokio::test]
async fn test_send_recv_all_full_then_partial() {
    let (producer, mut consumer) = create_queue_get_a_pair().await;

    let mut res = (0..10usize)
        .map(|i| ExType {
            a: format!("test{i}"),
        })
        .collect::<HashSet<_>>();

    for payload in &res {
        producer.send_serde_json(payload).await.unwrap();
    }

    for (received_count, remaining_item_count) in [(6, 4), (4, 0)] {
        let now = Instant::now();
        let deadline = Duration::from_secs(2);
        let d = consumer.receive_all(6, deadline).await.unwrap();
        assert_eq!(d.len(), received_count);
        for i in d {
            let p = i.payload_serde_json::<ExType>().unwrap().unwrap();
            res.remove(&p);
            i.ack().await.unwrap();
        }
        assert_eq!(res.len(), remaining_item_count);
        assert!(now.elapsed() < deadline);
    }
}

#[tokio::test]
async fn test_scheduled_recv() {
    let (producer, mut consumer) = create_queue_get_a_pair().await;

    let payload = "test123";
    let delay = Duration::from_secs(1);
    producer.send_raw_scheduled(payload, delay).await.unwrap();

    let d = consumer.receive().await;
    match d {
        Err(QueueError::NoData) => {}
        _ => panic!("Unexpected result"),
    }

    // Give it some buffer:
    tokio::time::sleep(delay + Duration::from_millis(100)).await;

    let mut d = consumer.receive().await.unwrap();
    assert_eq!(
        payload,
        &String::from_utf8(d.take_payload().unwrap()).unwrap()
    );
    d.ack().await.unwrap();
}

#[tokio::test]
async fn test_scheduled_recv_all() {
    let (producer, mut consumer) = create_queue_get_a_pair().await;

    let payload = "test123";
    let delay = Duration::from_secs(1);
    producer.send_raw_scheduled(payload, delay).await.unwrap();

    let d = consumer.receive_all(1, Duration::ZERO).await.unwrap();
    assert!(d.is_empty());

    tokio::time::sleep(delay + Duration::from_millis(100)).await;

    let mut d = consumer.receive_all(1, Duration::ZERO).await.unwrap();
    assert_eq!(d.len(), 1);
    let mut d = d.pop().unwrap();
    assert_eq!(
        payload,
        &String::from_utf8(d.take_payload().unwrap()).unwrap()
    );
    d.ack().await.unwrap();
}
