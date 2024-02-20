//! Redis stream-based queue implementation
//!
//! # Redis Streams in Brief
//! Redis has a built-in queue called streams. With consumer groups and consumers, messages in this
//! queue will automatically be put into a pending queue when read and deleted when acknowledged.
//!
//! # The Implementation
//! This implementation uses this to allow worker instances to race for messages to dispatch which
//! are then, ideally, acknowledged. If a message is processing for more than 45 seconds, it is
//! reinserted at the back of the queue to be tried again.
//!
//! This implementation uses the following data structures:
//! - A "tasks to be processed" stream - which is what the consumer listens to for tasks.
//!     AKA: Main
//! - A ZSET for delayed tasks with the sort order being the time-to-be-delivered
//!     AKA: Delayed
//!
//! The implementation spawns an additional worker that monitors both the zset delayed tasks and
//! the tasks currently processing. It monitors the zset task set for tasks that should be
//! processed now, and the currently processing queue for tasks that have timed out and should be
//! put back on the main queue.

// This lint warns on `let _: () = ...` which is used throughout this file for Redis commands which
// have generic return types. This is cleaner than the turbofish operator in my opinion.
#![allow(clippy::let_unit_value)]

use std::sync::Arc;
use std::time::Duration;
use std::{any::TypeId, collections::HashMap, marker::PhantomData};

use async_trait::async_trait;
use bb8::ManageConnection;
pub use bb8_redis::RedisMultiplexedConnectionManager;
use redis::{
    streams::{StreamClaimReply, StreamId, StreamReadOptions, StreamReadReply},
    FromRedisValue, RedisResult,
};
use svix_ksuid::KsuidLike;
use tokio::task::JoinSet;

use crate::{
    builder::{QueueBuilder, Static},
    decoding::DecoderRegistry,
    encoding::{CustomEncoder, EncoderRegistry},
    queue::{Acker, Delivery, QueueBackend, QueueConsumer, QueueProducer},
    QueueError, ScheduledProducer,
};

#[cfg(feature = "redis_cluster")]
mod cluster;
#[cfg(feature = "redis_cluster")]
pub use cluster::RedisClusterConnectionManager;

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

#[cfg(feature = "redis_cluster")]
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
    pub delayed_queue_key: String,
    pub consumer_group: String,
    pub consumer_name: String,
    pub payload_key: String,
    pub ack_deadline_ms: i64,
}

pub struct RedisBackend<R = RedisMultiplexedConnectionManager>(PhantomData<R>);
#[cfg(feature = "redis_cluster")]
pub type RedisClusterBackend = RedisBackend<RedisClusterConnectionManager>;

type RawPayload = Vec<u8>;

impl<R> RedisBackend<R>
where
    R: RedisConnection,
    R::Connection: redis::aio::ConnectionLike + Send + Sync,
    R::Error: 'static + std::error::Error + Send + Sync,
{
    /// Creates a new redis queue builder with the given configuration.
    pub fn builder(config: RedisConfig) -> QueueBuilder<Self, Static> {
        QueueBuilder::new(config)
    }
}

impl<R> QueueBackend for RedisBackend<R>
where
    R: RedisConnection,
    R::Connection: redis::aio::ConnectionLike + Send + Sync,
    R::Error: 'static + std::error::Error + Send + Sync,
{
    // FIXME: Is it possible to use the types Redis actually uses?
    type PayloadIn = RawPayload;
    type PayloadOut = RawPayload;

    type Producer = RedisProducer<R>;
    type Consumer = RedisConsumer<R>;

    type Config = RedisConfig;

    async fn new_pair(
        cfg: RedisConfig,
        custom_encoders: EncoderRegistry<Vec<u8>>,
        custom_decoders: DecoderRegistry<Vec<u8>>,
    ) -> Result<(RedisProducer<R>, RedisConsumer<R>), QueueError> {
        let redis = R::from_dsn(&cfg.dsn)?;
        let redis = bb8::Pool::builder()
            .max_size(cfg.max_connections.into())
            .build(redis)
            .await
            .map_err(QueueError::generic)?;

        let background_tasks = Arc::new(
            start_background_tasks(
                redis.clone(),
                &cfg.queue_key,
                &cfg.delayed_queue_key,
                &cfg.payload_key,
                &cfg.consumer_group,
                &cfg.consumer_name,
                cfg.ack_deadline_ms,
            )
            .await,
        );

        Ok((
            RedisProducer {
                registry: custom_encoders,
                redis: redis.clone(),
                queue_key: cfg.queue_key.clone(),
                delayed_queue_key: cfg.delayed_queue_key,
                payload_key: cfg.payload_key.clone(),
                _background_tasks: background_tasks.clone(),
            },
            RedisConsumer {
                registry: custom_decoders,
                redis,
                queue_key: cfg.queue_key,
                consumer_group: cfg.consumer_group,
                consumer_name: cfg.consumer_name,
                payload_key: cfg.payload_key,
                _background_tasks: background_tasks.clone(),
            },
        ))
    }

    async fn producing_half(
        cfg: RedisConfig,
        custom_encoders: EncoderRegistry<Vec<u8>>,
    ) -> Result<RedisProducer<R>, QueueError> {
        let redis = R::from_dsn(&cfg.dsn)?;
        let redis = bb8::Pool::builder()
            .max_size(cfg.max_connections.into())
            .build(redis)
            .await
            .map_err(QueueError::generic)?;

        let background_tasks = Arc::new(
            start_background_tasks(
                redis.clone(),
                &cfg.queue_key,
                &cfg.delayed_queue_key,
                &cfg.payload_key,
                &cfg.consumer_group,
                &cfg.consumer_name,
                cfg.ack_deadline_ms,
            )
            .await,
        );
        Ok(RedisProducer {
            registry: custom_encoders,
            redis,
            queue_key: cfg.queue_key,
            delayed_queue_key: cfg.delayed_queue_key,
            payload_key: cfg.payload_key,
            _background_tasks: background_tasks,
        })
    }

    async fn consuming_half(
        cfg: RedisConfig,
        custom_decoders: DecoderRegistry<Vec<u8>>,
    ) -> Result<RedisConsumer<R>, QueueError> {
        let redis = R::from_dsn(&cfg.dsn)?;
        let redis = bb8::Pool::builder()
            .max_size(cfg.max_connections.into())
            .build(redis)
            .await
            .map_err(QueueError::generic)?;

        let background_tasks = Arc::new(
            start_background_tasks(
                redis.clone(),
                &cfg.queue_key,
                &cfg.delayed_queue_key,
                &cfg.payload_key,
                &cfg.consumer_group,
                &cfg.consumer_name,
                cfg.ack_deadline_ms,
            )
            .await,
        );

        Ok(RedisConsumer {
            registry: custom_decoders,
            redis,
            queue_key: cfg.queue_key,
            consumer_group: cfg.consumer_group,
            consumer_name: cfg.consumer_name,
            payload_key: cfg.payload_key,
            _background_tasks: background_tasks,
        })
    }
}

// FIXME(onelson): there's a trait, [`SchedulerBackend`], but no obvious way to implement it in a
//   way that makes good sense here.
//   We need access to the pool, and various bits of config to spawn a task, but none of that is
//   available where it matters right now.
//   Doing my own thing for now - standalone function that takes what it needs.
async fn start_background_tasks<R>(
    redis: bb8::Pool<R>,
    queue_key: &str,
    delayed_queue_key: &str,
    payload_key: &str,
    consumer_group: &str,
    consumer_name: &str,
    task_timeout_ms: i64,
) -> JoinSet<Result<(), QueueError>>
where
    R: RedisConnection,
    R::Connection: redis::aio::ConnectionLike + Send + Sync,
    R::Error: 'static + std::error::Error + Send + Sync,
{
    let mut join_set = JoinSet::new();

    // FIXME(onelson): does it even make sense to treat delay support as optional here?
    if delayed_queue_key.is_empty() {
        tracing::warn!("no delayed_queue_key specified - delayed task scheduler disabled");
    } else {
        join_set.spawn({
            let pool = redis.clone();
            let mqn = queue_key.to_string();
            let dqn = delayed_queue_key.to_string();
            // FIXME(onelson): should delayed_lock be configurable?
            //   Should `delayed_queue_name` even? Could be a suffix on `queue_name`.
            let delayed_lock = format!("{delayed_queue_key}__lock");
            let payload_key = payload_key.to_string();
            tracing::debug!(
                "spawning delayed task scheduler: delayed_queue_key=`{delayed_queue_key}`, \
                 delayed_lock=`{delayed_lock}`"
            );

            async move {
                loop {
                    if let Err(err) = background_task_delayed(
                        pool.clone(),
                        mqn.clone(),
                        dqn.clone(),
                        &delayed_lock,
                        &payload_key,
                    )
                    .await
                    {
                        tracing::error!("{}", err);
                        tokio::time::sleep(Duration::from_millis(500)).await;
                        continue;
                    };
                }
            }
        });
    }

    join_set.spawn({
        let pool = redis.clone();

        let mqn = queue_key.to_string();
        // FIXME(onelson): expose in config and confirm this is milliseconds
        let consumer_group = consumer_group.to_string();
        let consumer_name = consumer_name.to_string();

        async move {
            loop {
                if let Err(err) = background_task_pending(
                    pool.clone(),
                    &mqn,
                    &consumer_group,
                    &consumer_name,
                    task_timeout_ms,
                )
                .await
                {
                    tracing::error!("{}", err);
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    continue;
                }
            }
        }
    });
    join_set
}

/// Special ID for XADD command's which generates a stream ID automatically
const GENERATE_STREAM_ID: &str = "*";
/// Special ID for XREADGROUP commands which reads any new messages
const LISTEN_STREAM_ID: &str = ">";

/// Moves "due" messages from a sorted set, where delayed messages are shelved, back onto the main queue.
async fn background_task_delayed<R>(
    pool: bb8::Pool<R>,
    main_queue_name: String,
    delayed_queue_name: String,
    delayed_lock: &str,
    payload_key: &str,
) -> Result<(), QueueError>
where
    R: RedisConnection,
    R::Connection: redis::aio::ConnectionLike + Send + Sync,
    R::Error: 'static + std::error::Error + Send + Sync,
{
    let batch_size: isize = 50;

    let mut conn = pool.get().await.map_err(QueueError::generic)?;

    // There is a lock on the delayed queue processing to avoid race conditions. So first try to
    // acquire the lock should it not already exist. The lock expires after five seconds in case a
    // worker crashes while holding the lock.
    let mut cmd = redis::cmd("SET");
    cmd.arg(delayed_lock)
        .arg(true)
        .arg("NX")
        .arg("PX")
        .arg(5000);
    // WIll be Some("OK") when set or None when not set
    let resp: Option<String> = cmd
        .query_async(&mut *conn)
        .await
        .map_err(QueueError::generic)?;

    if resp.as_deref() == Some("OK") {
        // First look for delayed keys whose time is up and add them to the main queue
        let timestamp: i64 = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(QueueError::generic)?
            .as_secs()
            .try_into()
            .map_err(QueueError::generic)?;

        let keys: Vec<String> = redis::Cmd::zrangebyscore_limit(
            &delayed_queue_name,
            0isize,
            // Subtract 1 from the timestamp to make it exclusive rather than inclusive,
            // preventing premature delivery.
            timestamp - 1,
            0isize,
            batch_size,
        )
        .query_async(&mut *conn)
        .await
        .map_err(QueueError::generic)?;

        if !keys.is_empty() {
            // For each task, XADD them to the MAIN queue
            let mut pipe = redis::pipe();
            for key in &keys {
                // XXX: would be sort of nice if we could borrow a slice of bytes instead
                // of allocating a vec for each payload.
                // I bet serde allows for this somehow, but redis probably ends up allocating
                // before the value hits the wire anyway.
                let payload = from_delayed_queue_key(key)?;
                let _ = pipe.xadd(
                    &main_queue_name,
                    GENERATE_STREAM_ID,
                    &[(payload_key, payload)],
                );
            }
            let _: () = pipe
                .query_async(&mut *conn)
                .await
                .map_err(QueueError::generic)?;

            // Then remove the tasks from the delayed queue so they aren't resent
            let _: () = redis::Cmd::zrem(&delayed_queue_name, keys)
                .query_async(&mut *conn)
                .await
                .map_err(QueueError::generic)?;

            // Make sure to release the lock after done processing
            let _: () = redis::Cmd::del(delayed_lock)
                .query_async(&mut *conn)
                .await
                .map_err(QueueError::generic)?;
        } else {
            // Make sure to release the lock before sleeping
            let _: () = redis::Cmd::del(delayed_lock)
                .query_async(&mut *conn)
                .await
                .map_err(QueueError::generic)?;

            // Wait for half a second before attempting to fetch again if nothing was found
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    } else {
        // Also sleep half a second if the lock could not be fetched
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    Ok(())
}

struct StreamAutoclaimReply {
    ids: Vec<StreamId>,
}

impl FromRedisValue for StreamAutoclaimReply {
    fn from_redis_value(v: &redis::Value) -> RedisResult<Self> {
        // First try the two member array from before Redis 7.0
        match <((), StreamClaimReply)>::from_redis_value(v) {
            Ok(res) => Ok(StreamAutoclaimReply { ids: res.1.ids }),

            // If it's a type error, then try the three member array from Redis 7.0 and after
            Err(e) if e.kind() == redis::ErrorKind::TypeError => {
                <((), StreamClaimReply, ())>::from_redis_value(v)
                    .map(|ok| StreamAutoclaimReply { ids: ok.1.ids })
            }
            // Any other error should be returned as is
            Err(e) => Err(e),
        }
    }
}

/// The maximum number of pending messages to reinsert into the queue after becoming stale per loop
// FIXME(onelson): expose in config?
const PENDING_BATCH_SIZE: i16 = 1000;

/// Scoops up messages that have been claimed but not handled by a deadline, then re-queues them.
async fn background_task_pending<R>(
    pool: bb8::Pool<R>,
    main_queue_name: &str,
    consumer_group: &str,
    consumer_name: &str,
    pending_duration: i64,
) -> Result<(), QueueError>
where
    R: RedisConnection,
    R::Connection: redis::aio::ConnectionLike + Send + Sync,
    R::Error: 'static + std::error::Error + Send + Sync,
{
    let mut conn = pool.get().await.map_err(QueueError::generic)?;

    // Every iteration checks whether the processing queue has items that should be picked back up,
    // claiming them in the process
    let mut cmd = redis::cmd("XAUTOCLAIM");
    cmd.arg(main_queue_name)
        .arg(consumer_group)
        .arg(consumer_name)
        .arg(pending_duration)
        .arg("-")
        .arg("COUNT")
        .arg(PENDING_BATCH_SIZE);

    let StreamAutoclaimReply { ids } = cmd
        .query_async(&mut *conn)
        .await
        .map_err(QueueError::generic)?;

    if !ids.is_empty() {
        let mut pipe = redis::pipe();

        // And reinsert the map of KV pairs into the MAIN queue with a new stream ID
        for StreamId { map, .. } in &ids {
            let _ = pipe.xadd(
                main_queue_name,
                GENERATE_STREAM_ID,
                &map.iter()
                    .filter_map(|(k, v)| {
                        if let redis::Value::Data(data) = v {
                            Some((k.as_str(), data.as_slice()))
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<(&str, &[u8])>>(),
            );
        }

        let _: () = pipe
            .query_async(&mut *conn)
            .await
            .map_err(QueueError::generic)?;

        // Acknowledge all the stale ones so the pending queue is cleared
        let ids: Vec<_> = ids.iter().map(|wrapped| &wrapped.id).collect();

        let mut pipe = redis::pipe();
        pipe.add_command(redis::Cmd::xack(main_queue_name, consumer_group, &ids));
        pipe.add_command(redis::Cmd::xdel(main_queue_name, &ids));

        let _: () = pipe
            .query_async(&mut *conn)
            .await
            .map_err(QueueError::generic)?;
    } else {
        // Wait for half a second before attempting to fetch again if nothing was found
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    Ok(())
}

struct RedisAcker<M: ManageConnection> {
    redis: bb8::Pool<M>,
    queue_key: String,
    consumer_group: String,
    entry_id: String,

    already_acked_or_nacked: bool,
}

#[async_trait]
impl<M> Acker for RedisAcker<M>
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

pub struct RedisProducer<M: ManageConnection> {
    registry: EncoderRegistry<Vec<u8>>,
    redis: bb8::Pool<M>,
    queue_key: String,
    delayed_queue_key: String,
    payload_key: String,
    _background_tasks: Arc<JoinSet<Result<(), QueueError>>>,
}

impl<M> QueueProducer for RedisProducer<M>
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
        redis::Cmd::xadd(
            &self.queue_key,
            GENERATE_STREAM_ID,
            &[(&self.payload_key, payload)],
        )
        .query_async(&mut *conn)
        .await
        .map_err(QueueError::generic)?;

        Ok(())
    }
}

/// Acts as a payload prefix for when payloads are written to zset keys.
///
/// This ensures that messages with identical payloads:
/// - don't only get delivered once instead of N times.
/// - don't replace each other's "delivery due" timestamp.
fn delayed_key_id() -> String {
    svix_ksuid::Ksuid::new(None, None).to_base62()
}

/// Prefixes a payload with an id, separated by a pipe, e.g `ID|payload`.
fn to_delayed_queue_key(payload: &RawPayload) -> Result<String, QueueError> {
    Ok(format!(
        "{}|{}",
        delayed_key_id(),
        serde_json::to_string(payload).map_err(QueueError::generic)?
    ))
}

/// Returns the payload portion of a delayed zset key.
fn from_delayed_queue_key(key: &str) -> Result<RawPayload, QueueError> {
    // All information is stored in the key in which the ID and JSON formatted task
    // are separated by a `|`. So, take the key, then take the part after the `|`.
    serde_json::from_str(
        key.split('|')
            .nth(1)
            .ok_or_else(|| QueueError::Generic("Improper key format".into()))?,
    )
    .map_err(QueueError::generic)
}

impl<M> ScheduledProducer for RedisProducer<M>
where
    M: ManageConnection,
    M::Connection: redis::aio::ConnectionLike + Send + Sync,
    M::Error: 'static + std::error::Error + Send + Sync,
{
    async fn send_raw_scheduled(
        &self,
        payload: &Self::Payload,
        delay: Duration,
    ) -> Result<(), QueueError> {
        let timestamp: i64 = (std::time::SystemTime::now() + delay)
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(QueueError::generic)?
            .as_secs()
            .try_into()
            .map_err(QueueError::generic)?;

        let mut conn = self.redis.get().await.map_err(QueueError::generic)?;
        redis::Cmd::zadd(
            &self.delayed_queue_key,
            to_delayed_queue_key(payload)?,
            timestamp,
        )
        .query_async(&mut *conn)
        .await
        .map_err(QueueError::generic)?;
        tracing::trace!("RedisQueue: event sent > (delay: {:?})", delay);
        Ok(())
    }
}

pub struct RedisConsumer<M: ManageConnection> {
    registry: DecoderRegistry<Vec<u8>>,
    redis: bb8::Pool<M>,
    queue_key: String,
    consumer_group: String,
    consumer_name: String,
    payload_key: String,
    _background_tasks: Arc<JoinSet<Result<(), QueueError>>>,
}

impl<M> RedisConsumer<M>
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
            acker: Box::new(RedisAcker {
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

impl<M> QueueConsumer for RedisConsumer<M>
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
            &[LISTEN_STREAM_ID],
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
            &[LISTEN_STREAM_ID],
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
