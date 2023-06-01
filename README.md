<picture>
  <source media="(prefers-color-scheme: dark)", srcset="./assets/banner_dark.png">
  <source media="(prefers-color-scheme: light)", srcset="./assets/banner_light.png">
  <img src="./assets/banner_light.png">
</picture>

# Omniqueue

Omniqueue is an abstraction layer over queue backends for Rust. It includes support for RabbitMQ,
Redis streams, and SQS out of the box. The trait may also be implemented for other backends to meet
your own needs.

Omniqueue provides a high level interface which allows sending and receiving raw byte arrays, any
`serde` `Deserailize` and `Serialize` implementors via JSON encoded byte arrays, or any arbitrary
types for which you have provided an encoding and/or decoding function.

It is designed to be flexible and to be able to adapt to fit your existing queue configurations, but
with a set of defaults that makes it simple to start seding and reciving quickly.

Omniqueue is still early in development.

## How to use Omniqueue

While the exact configuration will depend on the backend used, usage is roughly as follows.

1. Add `omniqueue` to your `Cargo.toml`. All backends are enabled by default including RabbitMQ,
   Redis (via their stream type), SQS, and an in-memory queue based off ot `tokio` broadcast
   channel which is perfect for testing.

   If you only need some backends, then simply disable the default features, and enable any backends
   that you require.

2. Construct anud use your queue.

   The exact configuration type used will depend on your backend, but it's as simple as:

   ```rust
   let cfg = SqsConfig {
       queue_dsn: "http://localhost:9324/queue/queue_name".to_owned(),
       override_endpoint: true,
   };
   let (producer, mut consumer) = SqsQueueBackend::builder(cfg).build_pair().await?;

   producer.send_serde_json(&ExampleType::default()).await?;

   let delivery = c.receive().await?;
   assert_eq!(
       delivery.payload_serde_json::<ExampleType>().await?,
       Some(ExampleType::default())
   );

   delivery.ack().await?;
   ```

   The producer and consumers returned implement the `QueueProducer` and `QueueConsumer` traits
   respectively. This means you can make functions generic over any queue backend. Alternatively, if
   you need dynamic dispatch, it's as simple as one extra line ih the builder:

   ```rust
   let cfg = SqsConfig {
       queue_dsn: "http://localhost:9324/queue/queue_name".to_owned(),
       override_endpoint: true,
   };
   let (producer, mut consumer) = SqsQueueBackend::builder(cfg)
       .make_dynamic()
       .build_pair()
       .await?;
   ```

## Encoders and Decoders

Part of the design of this crate was a clear separation of responsibility. The users of the queue
generically should never have to concern themselves with how any given item is represented within
the queue backend. Instead, they should be allowed to think only in Rust types.

On the other hand, the users who define which backend to use should be the only ones concerned with
getting the queue's internal representations to the Rust types.

Enter `CustomEncoder`s and `CustomDecoder`s: these are a simple as closures or function pointers
that convert from regular Rust types to the type expected by the queue backend's input or output.

They are defined and used as follows:

```rust
#[derive(Debug, PartialEq)]
struct ExampleType {
	field: u8,
}


let (p, mut c) = RabbitMqBackend::builder(cfg)
	// RabbitMQ's internal representation is an arbitrary byte array.
	.with_encoder(|et: &ExampleType| -> Result<Vec<u8>, QueueError> {
		Ok(vec![et.field])
	})
	.with_decoder(|v: &Vec<u8>| -> Result<ExampleType, QueueError> {
		Ok(ExampleType {
			field: *v.first().unwrap_or(i&0),
		})
	})
	.build_pair()
	.await?;

let payload = ExampleType { field: 2 };

p.send_custom(&payload).await?;

let delivery = c.receive().await?;
assert_eq!(d.payload_custom::<ExampleType>()?, Some(payload))

delivery.ack().await?;
```

These functions are called automatically assuming you have an encoder and/or decoder for the right
type. This makes adapting the crate to an existing queue whose internal data layout doesn't match
the defaults to a T as simple as possible.
