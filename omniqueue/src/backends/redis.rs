use std::{any::TypeId, collections::HashMap};

use async_trait::async_trait;
use bb8_redis::RedisConnectionManager;
use redis::streams::{StreamReadOptions, StreamReadReply};

use crate::{
    decoding::DecoderRegistry,
    encoding::{CustomEncoder, EncoderRegistry},
    queue::{consumer::QueueConsumer, producer::QueueProducer, Acker, Delivery, QueueBackend},
    QueueError,
};

pub struct RedisConfig {
    pub dsn: String,
    pub max_connections: u16,
    pub reinsert_on_nack: bool,
    pub queue_key: String,
    pub consumer_group: String,
    pub consumer_name: String,
    pub payload_key: String,
}

pub struct RedisQueueBackend;

#[async_trait]
impl QueueBackend for RedisQueueBackend {
    type Config = RedisConfig;

    // FIXME: Is it possible to use the types Redis actually uses?
    type PayloadIn = Vec<u8>;
    type PayloadOut = Vec<u8>;

    type Producer = RedisStreamProducer;
    type Consumer = RedisStreamConsumer;

    async fn new_pair(
        cfg: RedisConfig,
        custom_encoders: EncoderRegistry<Vec<u8>>,
        custom_decoders: DecoderRegistry<Vec<u8>>,
    ) -> Result<(RedisStreamProducer, RedisStreamConsumer), QueueError> {
        let redis = RedisConnectionManager::new(cfg.dsn).map_err(QueueError::generic)?;
        let redis = bb8::Pool::builder()
            .max_size(cfg.max_connections.into())
            .build(redis)
            .await
            .map_err(QueueError::generic)?;

        Ok((
            RedisStreamProducer {
                registry: custom_encoders,
                redis: redis.clone(),
                queue_key: cfg.queue_key.clone(),
                payload_key: cfg.payload_key.clone(),
            },
            RedisStreamConsumer {
                registry: custom_decoders,
                redis,
                queue_key: cfg.queue_key,
                consumer_group: cfg.consumer_group,
                consumer_name: cfg.consumer_name,
                payload_key: cfg.payload_key,
            },
        ))
    }

    async fn producing_half(
        cfg: RedisConfig,
        custom_encoders: EncoderRegistry<Vec<u8>>,
    ) -> Result<RedisStreamProducer, QueueError> {
        let redis = RedisConnectionManager::new(cfg.dsn).map_err(QueueError::generic)?;
        let redis = bb8::Pool::builder()
            .max_size(cfg.max_connections.into())
            .build(redis)
            .await
            .map_err(QueueError::generic)?;

        Ok(RedisStreamProducer {
            registry: custom_encoders,
            redis,
            queue_key: cfg.queue_key,
            payload_key: cfg.payload_key,
        })
    }

    async fn consuming_half(
        cfg: RedisConfig,
        custom_decoders: DecoderRegistry<Vec<u8>>,
    ) -> Result<RedisStreamConsumer, QueueError> {
        let redis = RedisConnectionManager::new(cfg.dsn).map_err(QueueError::generic)?;
        let redis = bb8::Pool::builder()
            .max_size(cfg.max_connections.into())
            .build(redis)
            .await
            .map_err(QueueError::generic)?;

        Ok(RedisStreamConsumer {
            registry: custom_decoders,
            redis,
            queue_key: cfg.queue_key,
            consumer_group: cfg.consumer_group,
            consumer_name: cfg.consumer_name,
            payload_key: cfg.payload_key,
        })
    }
}

pub struct RedisStreamAcker {
    redis: bb8::Pool<RedisConnectionManager>,
    queue_key: String,
    consumer_group: String,
    entry_id: String,

    already_acked_or_nacked: bool,
}

#[async_trait]
impl Acker for RedisStreamAcker {
    async fn ack(&mut self) -> Result<(), QueueError> {
        if self.already_acked_or_nacked {
            return Err(QueueError::CannotAckOrNackTwice);
        }

        self.already_acked_or_nacked = true;

        let mut conn = self.redis.get().await.map_err(QueueError::generic)?;
        redis::Cmd::xack(&self.queue_key, &self.consumer_group, &[&self.entry_id])
            .query_async(&mut *conn)
            .await
            .map_err(QueueError::generic)?;

        Ok(())
    }

    async fn nack(&mut self) -> Result<(), QueueError> {
        if self.already_acked_or_nacked {
            return Err(QueueError::CannotAckOrNackTwice);
        }

        self.already_acked_or_nacked = true;

        Ok(())
    }
}

pub struct RedisStreamProducer {
    registry: EncoderRegistry<Vec<u8>>,
    redis: bb8::Pool<RedisConnectionManager>,
    queue_key: String,
    payload_key: String,
}

#[async_trait]
impl QueueProducer for RedisStreamProducer {
    type Payload = Vec<u8>;

    fn get_custom_encoders(&self) -> &HashMap<TypeId, Box<dyn CustomEncoder<Self::Payload>>> {
        self.registry.as_ref()
    }

    async fn send_raw(&self, payload: &Vec<u8>) -> Result<(), QueueError> {
        let mut conn = self.redis.get().await.map_err(QueueError::generic)?;
        redis::Cmd::xadd(&self.queue_key, "*", &[(&self.payload_key, payload)])
            .query_async(&mut *conn)
            .await
            .map_err(QueueError::generic)?;

        Ok(())
    }
}

pub struct RedisStreamConsumer {
    registry: DecoderRegistry<Vec<u8>>,
    redis: bb8::Pool<RedisConnectionManager>,
    queue_key: String,
    consumer_group: String,
    consumer_name: String,
    payload_key: String,
}

#[async_trait]
impl QueueConsumer for RedisStreamConsumer {
    type Payload = Vec<u8>;

    async fn receive(&mut self) -> Result<Delivery, QueueError> {
        let mut conn = self.redis.get().await.map_err(QueueError::generic)?;

        // Ensure an empty vec is never returned
        let read_out: StreamReadReply = redis::Cmd::xread_options(
            &[&self.queue_key],
            &[">"],
            &StreamReadOptions::default()
                .group(&self.consumer_group, &self.consumer_name)
                .block(100_000)
                .count(1),
        )
        .query_async(&mut *conn)
        .await
        .map_err(QueueError::generic)?;

        let queue = read_out.keys.into_iter().next().ok_or(QueueError::NoData)?;

        let entry = queue.ids.into_iter().next().ok_or(QueueError::NoData)?;

        let entry_id = entry.id.clone();
        let payload = entry.map.get(&self.payload_key).ok_or(QueueError::NoData)?;
        let payload: Vec<u8> = redis::from_redis_value(payload).map_err(QueueError::generic)?;

        Ok(Delivery {
            payload: Some(payload),
            acker: Box::new(RedisStreamAcker {
                redis: self.redis.clone(),
                queue_key: self.queue_key.clone(),
                consumer_group: self.consumer_group.clone(),
                entry_id,
                already_acked_or_nacked: false,
            }),
            decoders: self.registry.clone(),
        })
    }
}
