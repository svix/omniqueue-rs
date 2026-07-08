<picture>
  <source media="(prefers-color-scheme: dark)", srcset="./assets/banner_dark.png">
  <source media="(prefers-color-scheme: light)", srcset="./assets/banner_light.png">
  <img src="./assets/banner_light.png">
</picture>

# Omniqueue

Omniqueue is an abstraction layer over queue backends for Rust. It includes support for RabbitMQ,
Redis streams, and SQS.

Omniqueue provides a high level interface which allows sending and receiving raw byte arrays, or
any `serde` `Deserialize` and `Serialize` implementors via JSON encoded byte arrays.

It is designed to be flexible and to be able to adapt to fit your existing queue configurations, but
with a set of defaults that makes it simple to start sending and receiving quickly.

## How to use Omniqueue

While the exact configuration will depend on the backend used, usage is roughly as follows.

1. Add `omniqueue` to your `Cargo.toml`. All backends are enabled by default including RabbitMQ,
   Redis (via their stream type), SQS, and an in-memory queue based off of `tokio`'s mpsc
   channel which is perfect for testing.

   If you only need some backends, then simply disable the default features, and enable any backends
   that you require.

2. Construct and use your queue.

   The exact configuration type used will depend on your backend, but it's as simple as:

   ```rust
   let cfg = SqsConfig {
       queue_dsn: "http://localhost:9324/queue/queue_name".to_owned(),
       override_endpoint: true,
   };
   let (producer, mut consumer) = SqsBackend::builder(cfg).build_pair().await?;

   producer.send_serde_json(&ExampleType::default()).await?;

   let delivery = consumer.receive().await?;
   assert_eq!(
       delivery.payload_serde_json::<ExampleType>()?,
       Some(ExampleType::default())
   );

   delivery.ack().await.map_err(|(e, _)| e)?;
   ```

   The producer and consumers returned implement the `QueueProducer` and `QueueConsumer` traits
   respectively. This means you can make functions generic over any queue backend. Alternatively, if
   you need dynamic dispatch, it's as simple as one extra line in the builder:

   ```rust
   let cfg = SqsConfig {
       queue_dsn: "http://localhost:9324/queue/queue_name".to_owned(),
       override_endpoint: true,
   };
   let (producer, mut consumer) = SqsBackend::builder(cfg)
       .make_dynamic()
       .build_pair()
       .await?;
   ```
