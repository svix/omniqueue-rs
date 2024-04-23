use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures_util::{FutureExt, StreamExt};
use lapin::types::AMQPValue;
pub use lapin::{
    acker::Acker as LapinAcker,
    options::{
        BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicPublishOptions,
        BasicQosOptions,
    },
    types::FieldTable,
    BasicProperties, Channel, Connection, ConnectionProperties, Consumer,
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
    pub uri: String,
    pub connection_properties: ConnectionProperties,

    pub publish_exchange: String,
    pub publish_routing_key: String,
    pub publish_options: BasicPublishOptions,
    pub publish_properties: BasicProperties,

    pub consume_queue: String,
    pub consumer_tag: String,
    pub consume_options: BasicConsumeOptions,
    pub consume_arguments: FieldTable,

    pub consume_prefetch_count: Option<u16>,
    pub requeue_on_nack: bool,
}

pub struct RabbitMqBackend;

impl RabbitMqBackend {
    /// Creates a new RabbitMQ queue builder with the given configuration.
    pub fn builder(config: RabbitMqConfig) -> QueueBuilder<Self, Static> {
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
                &cfg.consume_queue,
                &cfg.consumer_tag,
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
            use time::OffsetDateTime;

            let id = &KsuidMs::new(Some(OffsetDateTime::now_utc()), None);
            properties = properties.with_message_id(id.to_string().into());
        }
        if let Some(headers) = headers {
            properties = properties.with_headers(headers);
        }

        self.channel
            .basic_publish(
                &self.exchange,
                &self.routing_key,
                self.options,
                payload,
                properties,
            )
            .await
            .map_err(QueueError::generic)?;

        Ok(())
    }

    pub async fn send_raw(&self, payload: &[u8]) -> Result<()> {
        self.send_raw_with_headers(payload, None).await
    }

    pub async fn send_serde_json<P: Serialize + Sync>(&self, payload: &P) -> Result<()> {
        let payload = serde_json::to_vec(payload)?;
        self.send_raw(&payload).await
    }

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
}

impl_queue_producer!(RabbitMqProducer, Vec<u8>);
impl_scheduled_queue_producer!(RabbitMqProducer, Vec<u8>);

pub struct RabbitMqConsumer {
    consumer: Consumer,
    requeue_on_nack: bool,
}

impl RabbitMqConsumer {
    fn wrap_delivery(&self, delivery: lapin::message::Delivery) -> Delivery {
        Delivery {
            payload: Some(delivery.data),
            acker: Box::new(RabbitMqAcker {
                acker: Some(delivery.acker),
                requeue_on_nack: self.requeue_on_nack,
            }),
        }
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

impl_queue_consumer!(RabbitMqConsumer, Vec<u8>);

struct RabbitMqAcker {
    acker: Option<LapinAcker>,
    requeue_on_nack: bool,
}

#[async_trait]
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
}
