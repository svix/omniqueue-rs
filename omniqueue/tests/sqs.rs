use aws_sdk_sqs::Client;
use omniqueue::{
    backends::sqs::{SqsConfig, SqsQueueBackend},
    queue::{consumer::QueueConsumer, producer::QueueProducer, QueueBackend, QueueBuilder, Static},
};
use serde::{Deserialize, Serialize};

const ROOT_URL: &str = "http://localhost:9324";
const DEFAULT_CFG: [(&str, &str); 3] = [
    ("AWS_DEFAULT_REGION", "localhost"),
    ("AWS_ACCESS_KEY_ID", "x"),
    ("AWS_SECRET_ACCESS_KEY", "x"),
];

/// Returns a [`QueueBuilder`] configured to connect to the SQS instance spawned by the file
/// `testing-docker-compose.yaml` in the root of the repository.
///
/// Additionally this will make a temporary queue on that instance for the duration of the test such
/// as to ensure there is no stealing.w
async fn make_test_queue() -> QueueBuilder<SqsQueueBackend, Static> {
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
    };

    SqsQueueBackend::builder(config)
}

#[tokio::test]
async fn test_raw_send_recv() {
    let payload = "{\"test\": \"data\"}";
    let (p, mut c) = make_test_queue().await.build_pair().await.unwrap();

    p.send_raw(&payload.to_owned()).await.unwrap();

    let d = c.receive().await.unwrap();
    assert_eq!(d.borrow_payload().unwrap(), payload.as_bytes());
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

    let encoder = |p: &ExType| Ok(format!("{}", p.a));
    let decoder = |p: &String| {
        Ok(ExType {
            a: p.parse().unwrap_or(0),
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
