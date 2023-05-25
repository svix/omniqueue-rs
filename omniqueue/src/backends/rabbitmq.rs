use std::{any::TypeId, collections::HashMap};

use async_trait::async_trait;
use futures::StreamExt;
use lapin::{acker::Acker as LapinAcker, Channel, Connection, Consumer};

pub use lapin::{
    options::{BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicPublishOptions},
    types::FieldTable,
    BasicProperties, ConnectionProperties,
};

use crate::{
    decoding::DecoderRegistry,
    encoding::{CustomEncoder, EncoderRegistry},
    queue::{consumer::QueueConsumer, producer::QueueProducer, Acker, Delivery, QueueBackend},
    QueueError,
};

pub struct RabbitMqConfig {
    pub uri: String,
    pub connection_properties: ConnectionProperties,

    pub publish_exchange: String,
    pub publish_routing_key: String,
    pub publish_options: BasicPublishOptions,
    pub publish_properites: BasicProperties,

    pub consume_queue: String,
    pub consumer_tag: String,
    pub consume_options: BasicConsumeOptions,
    pub consume_arguments: FieldTable,

    pub requeue_on_nack: bool,
}

pub struct RabbitMqBackend;

#[async_trait]
impl QueueBackend for RabbitMqBackend {
    type Config = RabbitMqConfig;

    type PayloadIn = Vec<u8>;
    type PayloadOut = Vec<u8>;

    type Consumer = RabbitMqConsumer;
    type Producer = RabbitMqProducer;

    async fn new_pair(
        cfg: RabbitMqConfig,
        custom_encoders: EncoderRegistry<Vec<u8>>,
        custom_decoders: DecoderRegistry<Vec<u8>>,
    ) -> Result<(RabbitMqProducer, RabbitMqConsumer), QueueError> {
        let conn = Connection::connect(&cfg.uri, cfg.connection_properties)
            .await
            .map_err(QueueError::generic)?;

        let channel_tx = conn.create_channel().await.map_err(QueueError::generic)?;
        let channel_rx = conn.create_channel().await.map_err(QueueError::generic)?;

        Ok((
            RabbitMqProducer {
                registry: custom_encoders,
                channel: channel_tx,
                exchange: cfg.publish_exchange,
                routing_key: cfg.publish_routing_key,
                options: cfg.publish_options,
                properties: cfg.publish_properites,
            },
            RabbitMqConsumer {
                registry: custom_decoders,
                consumer: channel_rx
                    .basic_consume(
                        &cfg.consume_queue,
                        &cfg.consumer_tag,
                        cfg.consume_options,
                        cfg.consume_arguments,
                    )
                    .await
                    .map_err(QueueError::generic)?,
                requeue_on_nack: cfg.requeue_on_nack,
            },
        ))
    }

    async fn producing_half(
        cfg: RabbitMqConfig,
        custom_encoders: EncoderRegistry<Vec<u8>>,
    ) -> Result<RabbitMqProducer, QueueError> {
        let conn = Connection::connect(&cfg.uri, cfg.connection_properties)
            .await
            .map_err(QueueError::generic)?;

        let channel_tx = conn.create_channel().await.map_err(QueueError::generic)?;

        Ok(RabbitMqProducer {
            registry: custom_encoders,
            channel: channel_tx,
            exchange: cfg.publish_exchange,
            routing_key: cfg.publish_routing_key,
            options: cfg.publish_options,
            properties: cfg.publish_properites,
        })
    }

    async fn consuming_half(
        cfg: RabbitMqConfig,
        custom_decoders: DecoderRegistry<Vec<u8>>,
    ) -> Result<RabbitMqConsumer, QueueError> {
        let conn = Connection::connect(&cfg.uri, cfg.connection_properties)
            .await
            .map_err(QueueError::generic)?;

        let channel_rx = conn.create_channel().await.map_err(QueueError::generic)?;

        Ok(RabbitMqConsumer {
            registry: custom_decoders,
            consumer: channel_rx
                .basic_consume(
                    &cfg.consume_queue,
                    &cfg.consumer_tag,
                    cfg.consume_options,
                    cfg.consume_arguments,
                )
                .await
                .map_err(QueueError::generic)?,
            requeue_on_nack: cfg.requeue_on_nack,
        })
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

#[async_trait]
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

pub struct RabbitMqConsumer {
    registry: DecoderRegistry<Vec<u8>>,
    consumer: Consumer,
    requeue_on_nack: bool,
}

#[async_trait]
impl QueueConsumer for RabbitMqConsumer {
    type Payload = Vec<u8>;

    async fn receive(&mut self) -> Result<Delivery, QueueError> {
        let mut stream =
            self.consumer
                .clone()
                .map(|l: Result<lapin::message::Delivery, lapin::Error>| {
                    let l = l.map_err(QueueError::generic)?;

                    Ok(Delivery {
                        decoders: self.registry.clone(),
                        payload: Some(l.data),
                        acker: Box::new(RabbitMqAcker {
                            acker: Some(l.acker),
                            requeue_on_nack: self.requeue_on_nack,
                        }),
                    })
                });

        stream.next().await.ok_or(QueueError::NoData)?
    }
}

pub struct RabbitMqAcker {
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
