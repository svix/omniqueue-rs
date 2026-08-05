//! RabbitMQ queue implementation.
//!
//! # The Implementation
//!
//! Producers publish to an exchange with a routing key, and consumers read from
//! a queue. Omniqueue declares no exchanges, queues or bindings, so they have
//! to exist before a producer or consumer is built.
//!
//! Nacking uses `basic.nack`, and
//! [`requeue_on_nack`][RabbitMqConfig::requeue_on_nack] decides whether the
//! message goes back on the queue or is discarded.
//!
//! # Scheduled Messages
//!
//! Sending with a delay only works if the broker has the
//! [delayed message exchange plugin][plugin] enabled and
//! [`publish_exchange`][RabbitMqConfig::publish_exchange] names an exchange
//! declared with the type `x-delayed-message`.
//!
//! The delay is passed as an `x-delay` header. A broker without the plugin
//! ignores that header and delivers the message immediately instead of failing,
//! so set the exchange up before relying on delays.
//!
//! [plugin]: https://github.com/rabbitmq/rabbitmq-delayed-message-exchange
//!
//! # Unsupported Operations
//!
//! `Delivery::set_ack_deadline` and `QueueProducer::redrive_dlq` both return
//! [`QueueError::Unsupported`]. To dead-letter messages, configure a
//! dead-letter exchange on the queue and set `requeue_on_nack` to `false`.
//!
//! # Example
//!
//! ```no_run
//! # async {
//! use omniqueue::backends::{
//!     rabbitmq::{
//!         BasicConsumeOptions, BasicProperties, BasicPublishOptions, ConnectionProperties,
//!         FieldTable,
//!     },
//!     RabbitMqBackend, RabbitMqConfig,
//! };
//!
//! let cfg = RabbitMqConfig {
//!     uri: "amqp://guest:guest@localhost:5672/%2f".to_owned(),
//!     connection_properties: ConnectionProperties::default(),
//!
//!     // The empty exchange is the default exchange, which routes to the queue
//!     // named by the routing key.
//!     publish_exchange: String::new(),
//!     publish_routing_key: "my-queue".to_owned(),
//!     publish_options: BasicPublishOptions::default(),
//!     publish_properties: BasicProperties::default(),
//!
//!     consume_queue: "my-queue".to_owned(),
//!     consumer_tag: "my-consumer".to_owned(),
//!     consume_options: BasicConsumeOptions::default(),
//!     consume_arguments: FieldTable::default(),
//!
//!     consume_prefetch_count: Some(32),
//!     requeue_on_nack: true,
//! };
//!
//! let (p, mut c) = RabbitMqBackend::builder(cfg).build_pair().await?;
//! # anyhow::Ok(())
//! # };
//! ```

use std::time::{Duration, Instant};

use futures_util::{FutureExt, StreamExt};
use lapin::types::AMQPValue;
pub use lapin::{
    options::{
        BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicPublishOptions,
        BasicQosOptions,
    },
    types::FieldTable,
    Acker as LapinAcker, BasicProperties, Channel, Connection, ConnectionProperties, Consumer,
};
use serde::Serialize;

#[allow(deprecated)]
use crate::{
    builder::{QueueBuilder, Static},
    queue::{Acker, Delivery, QueueBackend},
    QueueError, Result,
};

#[derive(Clone)]
pub struct RabbitMqConfig {
    /// The AMQP URI of the broker, for example
    /// `amqp://guest:guest@localhost:5672/%2f`, where `%2f` is the URL-encoded
    /// name of the default virtual host, `/`.
    pub uri: String,

    /// Connection-level options handed to [`lapin`], such as which executor to
    /// run the connection on. [`ConnectionProperties::default()`] is a
    /// reasonable starting point.
    pub connection_properties: ConnectionProperties,

    /// The exchange to publish to.
    ///
    /// The empty string is the default exchange, which routes to the queue
    /// named by [`publish_routing_key`][Self::publish_routing_key]. Scheduled
    /// sends need an exchange of type `x-delayed-message`. See the
    /// [module docs][self].
    pub publish_exchange: String,

    /// The routing key to publish with.
    ///
    /// How it is interpreted is up to the exchange. On the default exchange it
    /// is the name of the destination queue.
    pub publish_routing_key: String,

    /// Options for every `basic.publish`, such as whether an unroutable message
    /// is returned rather than dropped.
    pub publish_options: BasicPublishOptions,

    /// AMQP properties set on every published message, such as the content type
    /// or the delivery mode. Scheduled sends add an `x-delay` header on top of
    /// these.
    pub publish_properties: BasicProperties,

    /// The name of the queue to consume from. It has to exist already.
    pub consume_queue: String,

    /// The tag identifying this consumer to the broker. An empty string lets
    /// the broker generate one.
    pub consumer_tag: String,

    /// Options for `basic.consume`.
    ///
    /// Leave `no_ack` off (the default) to acknowledge deliveries yourself.
    /// With `no_ack` on, the broker treats a message as delivered as soon as it
    /// is sent, and acking or nacking it has no effect.
    pub consume_options: BasicConsumeOptions,

    /// Extra arguments passed to `basic.consume`, for example to set a consumer
    /// priority.
    pub consume_arguments: FieldTable,

    /// How many unacknowledged messages the broker may have in flight to this
    /// consumer, set via `basic.qos`.
    ///
    /// `None` means no limit, which lets the broker push a whole backlog at one
    /// consumer, so it is usually worth setting.
    pub consume_prefetch_count: Option<u16>,

    /// Whether nacking a delivery puts it back on the queue.
    ///
    /// If `false`, a nacked message is discarded, or dead-lettered if the queue
    /// has a dead-letter exchange configured.
    pub requeue_on_nack: bool,
}

pub struct RabbitMqBackend;

impl RabbitMqBackend {
    /// Creates a new RabbitMQ queue builder with the given configuration.
    pub fn builder(config: RabbitMqConfig) -> QueueBuilder<Self, Static> {
        #[allow(deprecated)]
        QueueBuilder::new(config)
    }
}

async fn consumer(conn: &Connection, cfg: RabbitMqConfig) -> Result<RabbitMqConsumer> {
    let channel_rx = conn.create_channel().await.map_err(QueueError::generic)?;

    if let Some(n) = cfg.consume_prefetch_count {
        channel_rx
            .basic_qos(n, BasicQosOptions::default())
            .await
            .map_err(QueueError::generic)?;
    }

    Ok(RabbitMqConsumer {
        consumer: channel_rx
            .basic_consume(
                cfg.consume_queue.as_str().into(),
                cfg.consumer_tag.as_str().into(),
                cfg.consume_options,
                cfg.consume_arguments.clone(),
            )
            .await
            .map_err(QueueError::generic)?,
        requeue_on_nack: cfg.requeue_on_nack,
    })
}

async fn producer(conn: &Connection, cfg: RabbitMqConfig) -> Result<RabbitMqProducer> {
    let channel_tx = conn.create_channel().await.map_err(QueueError::generic)?;
    Ok(RabbitMqProducer {
        channel: channel_tx,
        exchange: cfg.publish_exchange.clone(),
        routing_key: cfg.publish_routing_key.clone(),
        options: cfg.publish_options,
        properties: cfg.publish_properties.clone(),
    })
}

#[allow(deprecated)]
impl QueueBackend for RabbitMqBackend {
    type PayloadIn = Vec<u8>;
    type PayloadOut = Vec<u8>;

    type Producer = RabbitMqProducer;
    type Consumer = RabbitMqConsumer;

    type Config = RabbitMqConfig;

    async fn new_pair(cfg: RabbitMqConfig) -> Result<(RabbitMqProducer, RabbitMqConsumer)> {
        let conn = Connection::connect(&cfg.uri, cfg.connection_properties.clone())
            .await
            .map_err(QueueError::generic)?;

        Ok((
            producer(&conn, cfg.clone()).await?,
            consumer(&conn, cfg.clone()).await?,
        ))
    }

    async fn producing_half(cfg: RabbitMqConfig) -> Result<RabbitMqProducer> {
        let conn = Connection::connect(&cfg.uri, cfg.connection_properties.clone())
            .await
            .map_err(QueueError::generic)?;

        producer(&conn, cfg.clone()).await
    }

    async fn consuming_half(cfg: RabbitMqConfig) -> Result<RabbitMqConsumer> {
        let conn = Connection::connect(&cfg.uri, cfg.connection_properties.clone())
            .await
            .map_err(QueueError::generic)?;

        consumer(&conn, cfg.clone()).await
    }
}

pub struct RabbitMqProducer {
    channel: Channel,
    exchange: String,
    routing_key: String,
    options: BasicPublishOptions,
    properties: BasicProperties,
}

impl RabbitMqProducer {
    async fn send_raw_with_headers(
        &self,
        payload: &[u8],
        headers: Option<FieldTable>,
    ) -> Result<()> {
        let mut properties = self.properties.clone();
        #[cfg(feature = "rabbitmq-with-message-ids")]
        {
            use svix_ksuid::{KsuidLike as _, KsuidMs};

            let id = KsuidMs::now(None);
            properties = properties.with_message_id(id.to_string().into());
        }
        if let Some(headers) = headers {
            properties = properties.with_headers(headers);
        }

        self.channel
            .basic_publish(
                self.exchange.as_str().into(),
                self.routing_key.as_str().into(),
                self.options,
                payload,
                properties,
            )
            .await
            .map_err(QueueError::generic)?;

        Ok(())
    }

    #[tracing::instrument(
        name = "send",
        skip_all,
        fields(payload_size = payload.len())
    )]
    pub async fn send_raw(&self, payload: &[u8]) -> Result<()> {
        self.send_raw_with_headers(payload, None).await
    }

    pub async fn send_serde_json<P: Serialize + Sync>(&self, payload: &P) -> Result<()> {
        let payload = serde_json::to_vec(payload)?;
        self.send_raw(&payload).await
    }

    #[tracing::instrument(
        name = "send",
        skip_all,
        fields(payload_size = payload.len(), delay)
    )]
    pub async fn send_raw_scheduled(&self, payload: &[u8], delay: Duration) -> Result<()> {
        let mut headers = FieldTable::default();

        let delay_ms: u32 = delay
            .as_millis()
            .try_into()
            .map_err(|_| QueueError::Generic("delay is too large".into()))?;
        headers.insert("x-delay".into(), AMQPValue::LongUInt(delay_ms));

        self.send_raw_with_headers(payload, Some(headers)).await
    }

    pub async fn send_serde_json_scheduled<P: Serialize + Sync>(
        &self,
        payload: &P,
        delay: Duration,
    ) -> Result<()> {
        let payload = serde_json::to_vec(payload)?;
        self.send_raw_scheduled(&payload, delay).await
    }

    pub async fn redrive_dlq(&self) -> Result<()> {
        Err(QueueError::Unsupported(
            "redrive_dlq is not supported by RabbitMqBackend",
        ))
    }
}

impl crate::QueueProducer for RabbitMqProducer {
    type Payload = Vec<u8>;
    omni_delegate!(send_raw, send_serde_json, redrive_dlq);
}
impl crate::ScheduledQueueProducer for RabbitMqProducer {
    omni_delegate!(send_raw_scheduled, send_serde_json_scheduled);
}

pub struct RabbitMqConsumer {
    consumer: Consumer,
    requeue_on_nack: bool,
}

impl RabbitMqConsumer {
    fn wrap_delivery(&self, delivery: lapin::message::Delivery) -> Delivery {
        Delivery::new(
            delivery.data,
            RabbitMqAcker {
                acker: Some(delivery.acker),
                requeue_on_nack: self.requeue_on_nack,
            },
        )
    }

    pub async fn receive(&mut self) -> Result<Delivery> {
        let mut stream =
            self.consumer
                .clone()
                .map(|l: Result<lapin::message::Delivery, lapin::Error>| {
                    let l = l.map_err(QueueError::generic)?;
                    Ok(self.wrap_delivery(l))
                });

        stream.next().await.ok_or(QueueError::NoData)?
    }

    pub async fn receive_all(
        &mut self,
        max_messages: usize,
        deadline: Duration,
    ) -> Result<Vec<Delivery>> {
        let mut stream = self.consumer.clone().map(
            |l: Result<lapin::message::Delivery, lapin::Error>| -> Result<Delivery> {
                let l = l.map_err(QueueError::generic)?;
                Ok(self.wrap_delivery(l))
            },
        );
        let start = Instant::now();
        let mut out = Vec::with_capacity(max_messages);
        match tokio::time::timeout(deadline, stream.next()).await {
            Ok(Some(x)) => out.push(x?),
            // Timeouts and stream termination
            Err(_) | Ok(None) => return Ok(out),
        }

        if max_messages > 1 {
            // `now_or_never` will break the loop if no ready items are already
            // buffered in the stream. This should allow us to opportunistically
            // fill up the buffer in the remaining time.
            while let Some(Some(x)) = stream.next().now_or_never() {
                out.push(x?);
                if out.len() >= max_messages || start.elapsed() >= deadline {
                    break;
                }
            }
        }
        Ok(out)
    }
}

impl crate::QueueConsumer for RabbitMqConsumer {
    type Payload = Vec<u8>;
    omni_delegate!(receive, receive_all);
}

struct RabbitMqAcker {
    acker: Option<LapinAcker>,
    requeue_on_nack: bool,
}

impl Acker for RabbitMqAcker {
    async fn ack(&mut self) -> Result<()> {
        self.acker
            .take()
            .ok_or(QueueError::CannotAckOrNackTwice)?
            .ack(BasicAckOptions { multiple: false })
            .await
            .map(|_| ())
            .map_err(QueueError::generic)
    }

    async fn nack(&mut self) -> Result<()> {
        self.acker
            .take()
            .ok_or(QueueError::CannotAckOrNackTwice)?
            .nack(BasicNackOptions {
                requeue: self.requeue_on_nack,
                multiple: false,
            })
            .await
            .map(|_| ())
            .map_err(QueueError::generic)
    }

    async fn set_ack_deadline(&mut self, _duration: Duration) -> Result<()> {
        Err(QueueError::Unsupported(
            "set_ack_deadline is not supported by RabbitMQ",
        ))
    }
}
