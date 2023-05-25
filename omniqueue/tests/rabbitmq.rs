use lapin::{
    options::{BasicConsumeOptions, BasicPublishOptions, QueueDeclareOptions},
    types::FieldTable,
    BasicProperties, Connection, ConnectionProperties,
};
use omniqueue::{
    backends::rabbitmq::{RabbitMqBackend, RabbitMqConfig},
    queue::{consumer::QueueConsumer, producer::QueueProducer, QueueBackend, QueueBuilder, Static},
};
use serde::{Deserialize, Serialize};

const MQ_URI: &str = "amqp://guest:guest@localhost:5672/%2f";

/// Returns a [`QueueBuilder`] configured to connect to the RabbitMQ instance spawned by the file
/// `testing-docker-compose.yaml` in the root of the repository.
///
/// Additionally this will make a temporary queue on that instance for the duration of the test such
/// as to ensure there is no stealing.w
async fn make_test_queue(reinsert_on_nack: bool) -> QueueBuilder<RabbitMqBackend, Static> {
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

    let config = RabbitMqConfig {
        uri: MQ_URI.to_owned(),
        connection_properties: options,
        publish_exchange: "".to_owned(),
        publish_routing_key: queue_name.clone(),
        publish_options: BasicPublishOptions::default(),
        publish_properites: BasicProperties::default(),
        consume_queue: queue_name,
        consumer_tag: "test".to_owned(),
        consume_options: BasicConsumeOptions::default(),
        consume_arguments: FieldTable::default(),
        requeue_on_nack: reinsert_on_nack,
    };

    RabbitMqBackend::builder(config)
}

#[tokio::test]
async fn test_bytes_send_recv() {
    let payload = b"hello";
    let (p, mut c) = make_test_queue(false).await.build_pair().await.unwrap();

    p.send_bytes(payload).await.unwrap();

    let d = c.receive().await.unwrap();
    assert_eq!(d.borrow_payload().unwrap(), payload);
    d.ack().await.unwrap();

    // The RabbitMQ native payload type is a Vec<u8>, so we can also send raw
    p.send_raw(&payload.to_vec()).await.unwrap();

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
    let (p, mut c) = make_test_queue(false).await.build_pair().await.unwrap();

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
            a: *p.first().unwrap_or(&0),
        })
    };

    let (p, mut c) = make_test_queue(false)
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
