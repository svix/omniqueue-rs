use std::time::{Duration, Instant};

use aws_sdk_sqs::Client;
use omniqueue::{
    backends::{SqsBackend, SqsConfig},
    QueueBuilder,
};
use serde::{Deserialize, Serialize};

const ROOT_URL: &str = "http://localhost:9324";
const DEFAULT_CFG: [(&str, &str); 3] = [
    ("AWS_DEFAULT_REGION", "localhost"),
    ("AWS_ACCESS_KEY_ID", "x"),
    ("AWS_SECRET_ACCESS_KEY", "x"),
];

/// Returns a [`QueueBuilder`] configured to connect to the SQS instance spawned
/// by the file `testing-docker-compose.yaml` in the root of the repository.
///
/// Additionally this will make a temporary queue on that instance for the
/// duration of the test such as to ensure there is no stealing.w
async fn make_test_queue() -> QueueBuilder<SqsBackend> {
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
    client
        .create_queue()
        .queue_name(&queue_name)
        .send()
        .await
        .unwrap();

    let config = SqsConfig {
        queue_dsn: format!("{ROOT_URL}/queue/{queue_name}"),
        override_endpoint: true,
        message_attribute_names: Vec::new(),
        dlq_config: None,
    };

    SqsBackend::builder(config)
}

#[tokio::test]
async fn test_raw_send_recv() {
    let payload = "{\"test\": \"data\"}";
    let (p, c) = make_test_queue().await.build_pair().await.unwrap();

    p.send_raw(payload).await.unwrap();

    let d = c.receive().await.unwrap();
    assert_eq!(d.borrow_payload().unwrap(), payload.as_bytes());
}

#[tokio::test]
async fn test_bytes_send_recv() {
    use omniqueue::QueueProducer as _;

    let payload = b"hello";
    let (p, c) = make_test_queue().await.build_pair().await.unwrap();

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
    let (p, c) = make_test_queue().await.build_pair().await.unwrap();

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
    let (p, c) = make_test_queue().await.build_pair().await.unwrap();

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
    let (p, c) = make_test_queue().await.build_pair().await.unwrap();

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
#[tokio::test]
async fn test_send_recv_all_full_then_partial() {
    let payload1 = ExType { a: 1 };
    let payload2 = ExType { a: 2 };
    let payload3 = ExType { a: 3 };
    let (p, c) = make_test_queue().await.build_pair().await.unwrap();

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
    let (_p, c) = make_test_queue().await.build_pair().await.unwrap();

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
    let (p, c) = make_test_queue().await.build_pair().await.unwrap();

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
async fn test_receive_with_filter() {
    use aws_sdk_sqs::types::{MessageAttributeValue, MessageSystemAttributeName};

    let config = aws_config::from_env().endpoint_url(ROOT_URL).load().await;
    let client = Client::new(&config);

    let queue_name: String = std::iter::repeat_with(fastrand::alphanumeric)
        .take(8)
        .collect();
    client
        .create_queue()
        .queue_name(&queue_name)
        .send()
        .await
        .unwrap();

    let queue_url = format!("{ROOT_URL}/queue/{queue_name}");

    // Send messages with different attributes
    client
        .send_message()
        .queue_url(&queue_url)
        .message_body("message1")
        .message_attributes(
            "version",
            MessageAttributeValue::builder()
                .data_type("String")
                .string_value("1.0")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    client
        .send_message()
        .queue_url(&queue_url)
        .message_body("message2")
        .message_attributes(
            "version",
            MessageAttributeValue::builder()
                .data_type("String")
                .string_value("2.0")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    client
        .send_message()
        .queue_url(&queue_url)
        .message_body("message3")
        .message_attributes(
            "version",
            MessageAttributeValue::builder()
                .data_type("String")
                .string_value("2.0")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let config = omniqueue::backends::sqs::SqsConfig {
        queue_dsn: queue_url,
        override_endpoint: true,
        message_attribute_names: vec![MessageSystemAttributeName::ApproximateReceiveCount],
        dlq_config: None,
    };

    let (_p, c) = omniqueue::backends::sqs::SqsBackend::builder(config)
        .build_pair()
        .await
        .unwrap();

    // Filter for version = "2.0"
    let delivery = c
        .receive_with_filter(
            |msg| {
                msg.message_attributes()
                    .and_then(|attrs| attrs.get("version"))
                    .and_then(|attr| attr.string_value())
                    .map(|v| v == "2.0")
                    .unwrap_or(false)
            },
            Duration::from_secs(10),
        )
        .await
        .unwrap();

    let body = String::from_utf8(delivery.borrow_payload().unwrap().to_vec()).unwrap();
    assert!(body == "message2" || body == "message3");
    delivery.ack().await.unwrap();
}

#[tokio::test]
async fn test_receive_all_with_filter() {
    use aws_sdk_sqs::types::{MessageAttributeValue, MessageSystemAttributeName};

    let config = aws_config::from_env().endpoint_url(ROOT_URL).load().await;
    let client = Client::new(&config);

    let queue_name: String = std::iter::repeat_with(fastrand::alphanumeric)
        .take(8)
        .collect();
    client
        .create_queue()
        .queue_name(&queue_name)
        .send()
        .await
        .unwrap();

    let queue_url = format!("{ROOT_URL}/queue/{queue_name}");

    // Send 5 messages: 2 with priority=high, 3 with priority=low
    for i in 0..5 {
        let priority = if i < 2 { "high" } else { "low" };
        client
            .send_message()
            .queue_url(&queue_url)
            .message_body(format!("message{}", i))
            .message_attributes(
                "priority",
                MessageAttributeValue::builder()
                    .data_type("String")
                    .string_value(priority)
                    .build()
                    .unwrap(),
            )
            .send()
            .await
            .unwrap();
    }

    let config = omniqueue::backends::sqs::SqsConfig {
        queue_dsn: queue_url,
        override_endpoint: true,
        message_attribute_names: vec![MessageSystemAttributeName::ApproximateReceiveCount],
        dlq_config: None,
    };

    let (_p, c) = omniqueue::backends::sqs::SqsBackend::builder(config)
        .build_pair()
        .await
        .unwrap();

    // Filter for priority = "high"
    let deliveries = c
        .receive_all_with_filter(
            |msg| {
                msg.message_attributes()
                    .and_then(|attrs| attrs.get("priority"))
                    .and_then(|attr| attr.string_value())
                    .map(|v| v == "high")
                    .unwrap_or(false)
            },
            10,
            Duration::from_secs(10),
        )
        .await
        .unwrap();

    assert_eq!(deliveries.len(), 2);
    for delivery in deliveries {
        delivery.ack().await.unwrap();
    }
}

#[tokio::test]
async fn test_filter_with_dlq() {
    use aws_sdk_sqs::types::{MessageAttributeValue, MessageSystemAttributeName};
    use omniqueue::backends::sqs::DeadLetterQueueConfig;

    let config = aws_config::from_env().endpoint_url(ROOT_URL).load().await;
    let client = Client::new(&config);

    // Create main queue
    let main_queue_name: String = std::iter::repeat_with(fastrand::alphanumeric)
        .take(8)
        .collect();
    client
        .create_queue()
        .queue_name(&main_queue_name)
        .send()
        .await
        .unwrap();

    // Create DLQ
    let dlq_name = format!("{}_dlq", main_queue_name);
    client
        .create_queue()
        .queue_name(&dlq_name)
        .send()
        .await
        .unwrap();

    let main_queue_url = format!("{ROOT_URL}/queue/{main_queue_name}");
    let dlq_url = format!("{ROOT_URL}/queue/{dlq_name}");

    // Send a message that won't match the filter
    client
        .send_message()
        .queue_url(&main_queue_url)
        .message_body("wrong_version_message")
        .message_attributes(
            "version",
            MessageAttributeValue::builder()
                .data_type("String")
                .string_value("1.0")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let max_filter_failures = 3;

    let config = omniqueue::backends::sqs::SqsConfig {
        queue_dsn: main_queue_url.clone(),
        override_endpoint: true,
        message_attribute_names: vec![MessageSystemAttributeName::ApproximateReceiveCount],
        dlq_config: Some(DeadLetterQueueConfig {
            queue_url: dlq_url.clone(),
            max_filter_failures,
        }),
    };

    let (_p, c) = omniqueue::backends::sqs::SqsBackend::builder(config)
        .build_pair()
        .await
        .unwrap();

    // Helper to check message count in a queue
    async fn check_queue_count(client: &Client, queue_url: &str, expected_count: i32) {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let attrs = client
            .get_queue_attributes()
            .queue_url(queue_url)
            .attribute_names(aws_sdk_sqs::types::QueueAttributeName::ApproximateNumberOfMessages)
            .send()
            .await
            .unwrap();

        let count = attrs
            .attributes()
            .and_then(|a| {
                a.get(&aws_sdk_sqs::types::QueueAttributeName::ApproximateNumberOfMessages)
            })
            .and_then(|v| v.parse::<i32>().ok())
            .unwrap_or(0);

        assert_eq!(
            count, expected_count,
            "Queue {} has {} messages, expected {}",
            queue_url, count, expected_count
        );
    }

    // Try to receive messages with filter for version="2.0"
    // The message with version="1.0" should be rejected and eventually go to DLQ
    for i in 0..max_filter_failures {
        println!("Filter attempt {}/{}", i + 1, max_filter_failures);

        let result = c
            .receive_with_filter(
                |msg| {
                    msg.message_attributes()
                        .and_then(|attrs| attrs.get("version"))
                        .and_then(|attr| attr.string_value())
                        .map(|v| v == "2.0")
                        .unwrap_or(false)
                },
                Duration::from_secs(3),
            )
            .await;

        // Should timeout since no matching message
        assert!(
            result.is_err(),
            "Expected no matching message on attempt {}",
            i + 1
        );

        // Wait for message to be re-queued
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // After max_filter_failures attempts, message should be in DLQ
    check_queue_count(&client, &dlq_url, 1).await;

    // Verify message is no longer in main queue
    check_queue_count(&client, &main_queue_url, 0).await;
}
