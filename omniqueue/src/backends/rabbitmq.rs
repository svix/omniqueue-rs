use std::time::{Duration, Instant};
use std::{any::TypeId, collections::HashMap};

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

use crate::{
    builder::{QueueBuilder, Static},
    decoding::DecoderRegistry,
    encoding::{CustomEncoder, EncoderRegistry},
    queue::{Acker, Delivery, QueueBackend, QueueConsumer, QueueProducer},
    scheduled::ScheduledProducer,
    QueueError,
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

async fn consumer(
    conn: &Connection,
    cfg: RabbitMqConfig,
    custom_decoders: DecoderRegistry<Vec<u8>>,
) -> Result<RabbitMqConsumer, QueueError> {
    let channel_rx = conn.create_channel().await.map_err(QueueError::generic)?;

    if let Some(n) = cfg.consume_prefetch_count {
        channel_rx
            .basic_qos(n, BasicQosOptions::default())
            .await
            .map_err(QueueError::generic)?;
    }

    Ok(RabbitMqConsumer {
        registry: custom_decoders,
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

async fn producer(
    conn: &Connection,
    cfg: RabbitMqConfig,
    custom_encoders: EncoderRegistry<Vec<u8>>,
) -> Result<RabbitMqProducer, QueueError> {
    let channel_tx = conn.create_channel().await.map_err(QueueError::generic)?;
    Ok(RabbitMqProducer {
        registry: custom_encoders,
        channel: channel_tx,
        exchange: cfg.publish_exchange.clone(),
        routing_key: cfg.publish_routing_key.clone(),
        options: cfg.publish_options,
        properties: cfg.publish_properties.clone(),
    })
}

impl QueueBackend for RabbitMqBackend {
    type PayloadIn = Vec<u8>;
    type PayloadOut = Vec<u8>;

    type Producer = RabbitMqProducer;
    type Consumer = RabbitMqConsumer;

    type Config = RabbitMqConfig;

    async fn new_pair(
        cfg: RabbitMqConfig,
        custom_encoders: EncoderRegistry<Vec<u8>>,
        custom_decoders: DecoderRegistry<Vec<u8>>,
    ) -> Result<(RabbitMqProducer, RabbitMqConsumer), QueueError> {
        let conn = Connection::connect(&cfg.uri, cfg.connection_properties.clone())
            .await
            .map_err(QueueError::generic)?;

        Ok((
            producer(&conn, cfg.clone(), custom_encoders).await?,
            consumer(&conn, cfg.clone(), custom_decoders).await?,
        ))
    }

    async fn producing_half(
        cfg: RabbitMqConfig,
        custom_encoders: EncoderRegistry<Vec<u8>>,
    ) -> Result<RabbitMqProducer, QueueError> {
        let conn = Connection::connect(&cfg.uri, cfg.connection_properties.clone())
            .await
            .map_err(QueueError::generic)?;

        producer(&conn, cfg.clone(), custom_encoders).await
    }

    async fn consuming_half(
        cfg: RabbitMqConfig,
        custom_decoders: DecoderRegistry<Vec<u8>>,
    ) -> Result<RabbitMqConsumer, QueueError> {
        let conn = Connection::connect(&cfg.uri, cfg.connection_properties.clone())
            .await
            .map_err(QueueError::generic)?;

        consumer(&conn, cfg.clone(), custom_decoders).await
    }
}

pub struct RabbitMqProducer {
    registry: EncoderRegistry<Vec<u8>>,
    channel: Channel,
    exchange: String,
    routing_key: String,
    options: BasicPublishOptions,
    properties: BasicProperties,
}

impl QueueProducer for RabbitMqProducer {
    type Payload = Vec<u8>;

    fn get_custom_encoders(&self) -> &HashMap<TypeId, Box<dyn CustomEncoder<Self::Payload>>> {
        self.registry.as_ref()
    }

    async fn send_raw(&self, payload: &Vec<u8>) -> Result<(), QueueError> {
        self.channel
            .basic_publish(
                &self.exchange,
                &self.routing_key,
                self.options,
                payload,
                self.properties.clone(),
            )
            .await
            .map_err(QueueError::generic)?;

        Ok(())
    }
}

impl ScheduledProducer for RabbitMqProducer {
    async fn send_raw_scheduled(
        &self,
        payload: &Self::Payload,
        delay: Duration,
    ) -> Result<(), QueueError> {
        let mut headers = FieldTable::default();

        let delay_ms: u32 = delay
            .as_millis()
            .try_into()
            .map_err(|_| QueueError::Generic("delay is too large".into()))?;
        headers.insert("x-delay".into(), AMQPValue::LongUInt(delay_ms));

        self.channel
            .basic_publish(
                &self.exchange,
                &self.routing_key,
                self.options,
                payload,
                self.properties.clone().with_headers(headers),
            )
            .await
            .map_err(QueueError::generic)?;

        Ok(())
    }
}

pub struct RabbitMqConsumer {
    registry: DecoderRegistry<Vec<u8>>,
    consumer: Consumer,
    requeue_on_nack: bool,
}

impl RabbitMqConsumer {
    fn wrap_delivery(&self, delivery: lapin::message::Delivery) -> Delivery {
        Delivery {
            decoders: self.registry.clone(),
            payload: Some(delivery.data),
            acker: Box::new(RabbitMqAcker {
                acker: Some(delivery.acker),
                requeue_on_nack: self.requeue_on_nack,
            }),
        }
    }
}

impl QueueConsumer for RabbitMqConsumer {
    type Payload = Vec<u8>;

    async fn receive(&mut self) -> Result<Delivery, QueueError> {
        let mut stream =
            self.consumer
                .clone()
                .map(|l: Result<lapin::message::Delivery, lapin::Error>| {
                    let l = l.map_err(QueueError::generic)?;
                    Ok(self.wrap_delivery(l))
                });

        stream.next().await.ok_or(QueueError::NoData)?
    }

    async fn receive_all(
        &mut self,
        max_messages: usize,
        deadline: Duration,
    ) -> Result<Vec<Delivery>, QueueError> {
        let mut stream = self.consumer.clone().map(
            |l: Result<lapin::message::Delivery, lapin::Error>| -> Result<Delivery, QueueError> {
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
            // `now_or_never` will break the loop if no ready items are already buffered in the stream.
            // This should allow us to opportunistically fill up the buffer in the remaining time.
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

struct RabbitMqAcker {
    acker: Option<LapinAcker>,
    requeue_on_nack: bool,
}

#[async_trait]
impl Acker for RabbitMqAcker {
    async fn ack(&mut self) -> Result<(), QueueError> {
        self.acker
            .take()
            .ok_or(QueueError::CannotAckOrNackTwice)?
            .ack(BasicAckOptions { multiple: false })
            .await
            .map(|_| ())
            .map_err(QueueError::generic)
    }

    async fn nack(&mut self) -> Result<(), QueueError> {
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
