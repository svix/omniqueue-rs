use std::time::Duration;
use std::{any::TypeId, collections::HashMap, marker::PhantomData};

use async_trait::async_trait;
use bb8::ManageConnection;
pub use bb8_redis::RedisMultiplexedConnectionManager;
use redis::streams::{StreamId, StreamReadOptions, StreamReadReply};

use crate::{
    decoding::DecoderRegistry,
    encoding::{CustomEncoder, EncoderRegistry},
    queue::{consumer::QueueConsumer, producer::QueueProducer, Acker, Delivery, QueueBackend},
    QueueError,
};

mod cluster;
use cluster::RedisClusterConnectionManager;

pub trait RedisConnection
where
    Self: ManageConnection + Sized,
    Self::Connection: redis::aio::ConnectionLike,
    Self::Error: 'static + std::error::Error + Send + Sync,
{
    fn from_dsn(dsn: &str) -> Result<Self, QueueError>;
}

impl RedisConnection for RedisMultiplexedConnectionManager {
    fn from_dsn(dsn: &str) -> Result<Self, QueueError> {
        Self::new(dsn).map_err(QueueError::generic)
    }
}

impl RedisConnection for RedisClusterConnectionManager {
    fn from_dsn(dsn: &str) -> Result<Self, QueueError> {
        Self::new(dsn).map_err(QueueError::generic)
    }
}

pub struct RedisConfig {
    pub dsn: String,
    pub max_connections: u16,
    pub reinsert_on_nack: bool,
    pub queue_key: String,
    pub consumer_group: String,
    pub consumer_name: String,
    pub payload_key: String,
}

pub struct RedisQueueBackend<R = RedisMultiplexedConnectionManager>(PhantomData<R>);
pub type RedisClusterQueueBackend = RedisQueueBackend<RedisClusterConnectionManager>;

#[async_trait]
impl<R> QueueBackend for RedisQueueBackend<R>
where
    R: RedisConnection,
    R::Connection: redis::aio::ConnectionLike + Send + Sync,
    R::Error: 'static + std::error::Error + Send + Sync,
{
    type Config = RedisConfig;

    // FIXME: Is it possible to use the types Redis actually uses?
    type PayloadIn = Vec<u8>;
    type PayloadOut = Vec<u8>;

    type Producer = RedisStreamProducer<R>;
    type Consumer = RedisStreamConsumer<R>;

    async fn new_pair(
        cfg: RedisConfig,
        custom_encoders: EncoderRegistry<Vec<u8>>,
        custom_decoders: DecoderRegistry<Vec<u8>>,
    ) -> Result<(RedisStreamProducer<R>, RedisStreamConsumer<R>), QueueError> {
        let redis = R::from_dsn(&cfg.dsn)?;
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
    ) -> Result<RedisStreamProducer<R>, QueueError> {
        let redis = R::from_dsn(&cfg.dsn)?;
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
    ) -> Result<RedisStreamConsumer<R>, QueueError> {
        let redis = R::from_dsn(&cfg.dsn)?;
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

pub struct RedisStreamAcker<M: ManageConnection> {
    redis: bb8::Pool<M>,
    queue_key: String,
    consumer_group: String,
    entry_id: String,

    already_acked_or_nacked: bool,
}

#[async_trait]
impl<M> Acker for RedisStreamAcker<M>
where
    M: ManageConnection,
    M::Connection: redis::aio::ConnectionLike + Send + Sync,
    M::Error: 'static + std::error::Error + Send + Sync,
{
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

pub struct RedisStreamProducer<M: ManageConnection> {
    registry: EncoderRegistry<Vec<u8>>,
    redis: bb8::Pool<M>,
    queue_key: String,
    payload_key: String,
}

#[async_trait]
impl<M> QueueProducer for RedisStreamProducer<M>
where
    M: ManageConnection,
    M::Connection: redis::aio::ConnectionLike + Send + Sync,
    M::Error: 'static + std::error::Error + Send + Sync,
{
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

pub struct RedisStreamConsumer<M: ManageConnection> {
    registry: DecoderRegistry<Vec<u8>>,
    redis: bb8::Pool<M>,
    queue_key: String,
    consumer_group: String,
    consumer_name: String,
    payload_key: String,
}

impl<M> RedisStreamConsumer<M>
where
    M: ManageConnection,
    M::Connection: redis::aio::ConnectionLike + Send + Sync,
    M::Error: 'static + std::error::Error + Send + Sync,
{
    fn wrap_entry(&self, entry: StreamId) -> Result<Delivery, QueueError> {
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

#[async_trait]
impl<M> QueueConsumer for RedisStreamConsumer<M>
where
    M: ManageConnection,
    M::Connection: redis::aio::ConnectionLike + Send + Sync,
    M::Error: 'static + std::error::Error + Send + Sync,
{
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
        self.wrap_entry(entry)
    }

    async fn receive_all(
        &mut self,
        max_messages: usize,
        deadline: Duration,
    ) -> Result<Vec<Delivery>, QueueError> {
        let mut conn = self.redis.get().await.map_err(QueueError::generic)?;

        let read_out: StreamReadReply = redis::Cmd::xread_options(
            &[&self.queue_key],
            &[">"],
            &StreamReadOptions::default()
                .group(&self.consumer_group, &self.consumer_name)
                .block(
                    deadline
                        .as_millis()
                        .try_into()
                        .map_err(QueueError::generic)?,
                )
                .count(max_messages),
        )
        .query_async(&mut *conn)
        .await
        .map_err(QueueError::generic)?;

        let mut out = Vec::with_capacity(max_messages);

        if let Some(queue) = read_out.keys.into_iter().next() {
            for entry in queue.ids {
                out.push(self.wrap_entry(entry)?);
            }
        }
        Ok(out)
    }
}
