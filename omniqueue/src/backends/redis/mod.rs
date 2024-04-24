//! Redis stream-based queue implementation
//!
//! # Redis Streams in Brief
//!
//! Redis has a built-in queue called streams. With consumer groups and
//! consumers, messages in this queue will automatically be put into a pending
//! queue when read and deleted when acknowledged.
//!
//! # The Implementation
//!
//! This implementation uses this to allow worker instances to race for messages
//! to dispatch which are then, ideally, acknowledged. If a message is
//! not acknowledged before a configured deadline, it is reinserted at the back
//! of the queue to be tried again.
//!
//! This implementation uses the following data structures:
//! - A "tasks to be processed" stream - which is what the consumer listens to
//!   for tasks. AKA: Main
//! - A `ZSET` for delayed tasks with the sort order being the
//!   time-to-be-delivered AKA: Delayed
//!
//! The implementation spawns an additional worker that monitors both the zset
//! delayed tasks and the tasks currently processing. It monitors the zset task
//! set for tasks that should be processed now, and the currently processing
//! queue for tasks that have timed out and should be put back on the main
//! queue.

// This lint warns on `let _: () = ...` which is used throughout this file for Redis commands which
// have generic return types. This is cleaner than the turbofish operator in my opinion.
#![allow(clippy::let_unit_value)]

use std::{
    marker::PhantomData,
    sync::Arc,
    time::{Duration, SystemTime, SystemTimeError, UNIX_EPOCH},
};

use bb8::ManageConnection;
pub use bb8_redis::RedisConnectionManager;
use redis::{AsyncCommands, ExistenceCheck, SetExpiry, SetOptions};
use serde::Serialize;
use svix_ksuid::KsuidLike;
use thiserror::Error;
use tokio::task::JoinSet;
use tracing::{debug, error, trace, warn};

#[allow(deprecated)]
use crate::{
    builder::{Dynamic, Static},
    queue::{Delivery, QueueBackend},
    DynConsumer, DynProducer, QueueConsumer as _, QueueError, QueueProducer as _, Result,
};

#[cfg(feature = "redis_cluster")]
mod cluster;
mod streams;

#[cfg(feature = "redis_cluster")]
pub use cluster::RedisClusterConnectionManager;

pub trait RedisConnection:
    ManageConnection<
    Connection = <Self as RedisConnection>::Connection,
    Error = <Self as RedisConnection>::Error,
>
{
    type Connection: redis::aio::ConnectionLike + Send + Sync;
    type Error: std::error::Error + Send + Sync + 'static;

    fn from_dsn(dsn: &str) -> Result<Self>;
}

impl RedisConnection for RedisConnectionManager {
    type Connection = <Self as ManageConnection>::Connection;
    type Error = <Self as ManageConnection>::Error;

    fn from_dsn(dsn: &str) -> Result<Self> {
        Self::new(dsn).map_err(QueueError::generic)
    }
}

#[cfg(feature = "redis_cluster")]
impl RedisConnection for RedisClusterConnectionManager {
    type Connection = <Self as ManageConnection>::Connection;
    type Error = <Self as ManageConnection>::Error;

    fn from_dsn(dsn: &str) -> Result<Self> {
        Self::new(dsn).map_err(QueueError::generic)
    }
}

#[derive(Debug, Error)]
enum EvictionCheckError {
    #[error("Unable to verify eviction policy. Ensure `maxmemory-policy` set to `noeviction` or `volatile-*`")]
    CheckEvictionPolicyFailed,
    #[error("Unsafe eviction policy found. Your queue is at risk of data loss. Please ensure `maxmemory-policy` set to `noeviction` or `volatile-*`")]
    UnsafeEvictionPolicy,
}

async fn check_eviction_policy<R: RedisConnection>(
    pool: bb8::Pool<R>,
) -> std::result::Result<(), EvictionCheckError> {
    let mut conn = pool
        .get()
        .await
        .map_err(|_| EvictionCheckError::CheckEvictionPolicyFailed)?;

    let results: Vec<String> = redis::cmd("CONFIG")
        .arg("GET")
        .arg("maxmemory-policy")
        .query_async::<<R as RedisConnection>::Connection, Vec<String>>(&mut *conn)
        .await
        .map_err(|_| EvictionCheckError::CheckEvictionPolicyFailed)?;

    let eviction_policy = results
        .get(1)
        .ok_or(EvictionCheckError::CheckEvictionPolicyFailed)?;

    if [
        "noeviction",
        "volatile-lru",
        "volatile-lfu",
        "volatile-random",
        "volatile-ttl",
    ]
    .contains(&eviction_policy.as_str())
    {
        tracing::debug!("Eviction policy `{eviction_policy}` found");
        Ok(())
    } else {
        Err(EvictionCheckError::UnsafeEvictionPolicy)
    }
}

pub struct RedisConfig {
    pub dsn: String,
    pub max_connections: u16,
    pub reinsert_on_nack: bool,
    pub queue_key: String,
    pub delayed_queue_key: String,
    pub delayed_lock_key: String,
    pub consumer_group: String,
    pub consumer_name: String,
    pub payload_key: String,
    pub ack_deadline_ms: i64,
}

pub struct RedisBackend<R = RedisConnectionManager>(PhantomData<R>);

#[cfg(feature = "redis_cluster")]
pub type RedisClusterBackend = RedisBackend<RedisClusterConnectionManager>;

type RawPayload = Vec<u8>;

impl RedisBackend {
    /// Creates a new redis queue builder with the given configuration.
    pub fn builder(config: RedisConfig) -> RedisBackendBuilder {
        RedisBackendBuilder::new(config)
    }

    #[cfg(feature = "redis_cluster")]
    /// Creates a new redis cluster queue builder with the given configuration.
    pub fn cluster_builder(config: RedisConfig) -> RedisClusterBackendBuilder {
        RedisBackendBuilder::new(config)
    }
}

#[allow(deprecated)]
impl<R: RedisConnection> QueueBackend for RedisBackend<R> {
    // FIXME: Is it possible to use the types Redis actually uses?
    type PayloadIn = RawPayload;
    type PayloadOut = RawPayload;

    type Producer = RedisProducer<R>;
    type Consumer = RedisConsumer<R>;

    type Config = RedisConfig;

    async fn new_pair(cfg: RedisConfig) -> Result<(RedisProducer<R>, RedisConsumer<R>)> {
        RedisBackendBuilder::new(cfg).build_pair().await
    }

    async fn producing_half(cfg: RedisConfig) -> Result<RedisProducer<R>> {
        RedisBackendBuilder::new(cfg).build_producer().await
    }

    async fn consuming_half(cfg: RedisConfig) -> Result<RedisConsumer<R>> {
        RedisBackendBuilder::new(cfg).build_consumer().await
    }
}

pub struct RedisBackendBuilder<R = RedisConnectionManager, S = Static> {
    config: RedisConfig,
    _phantom: PhantomData<fn() -> (R, S)>,
}

#[cfg(feature = "redis_cluster")]
pub type RedisClusterBackendBuilder = RedisBackendBuilder<RedisClusterConnectionManager>;

impl<R: RedisConnection> RedisBackendBuilder<R> {
    fn new(config: RedisConfig) -> Self {
        Self {
            config,
            _phantom: PhantomData,
        }
    }

    /// Set a custom [`RedisConnection`] mananager to use.
    ///
    /// This method only makes sense to call if you have a custom connection
    /// manager implementation. For clustered redis, use
    /// [`.cluster()`][Self::cluster].
    pub fn connection_manager<R2>(self) -> RedisBackendBuilder<R2> {
        RedisBackendBuilder {
            config: self.config,
            _phantom: PhantomData,
        }
    }

    #[cfg(feature = "redis_cluster")]
    pub fn cluster(self) -> RedisBackendBuilder<RedisClusterConnectionManager> {
        self.connection_manager()
    }

    pub async fn build_pair(self) -> Result<(RedisProducer<R>, RedisConsumer<R>)> {
        let redis = R::from_dsn(&self.config.dsn)?;
        let redis = bb8::Pool::builder()
            .max_size(self.config.max_connections.into())
            .build(redis)
            .await
            .map_err(QueueError::generic)?;

        let background_tasks = self.start_background_tasks(redis.clone()).await;

        Ok((
            RedisProducer {
                redis: redis.clone(),
                queue_key: self.config.queue_key.clone(),
                delayed_queue_key: self.config.delayed_queue_key,
                payload_key: self.config.payload_key.clone(),
                _background_tasks: background_tasks.clone(),
            },
            RedisConsumer {
                redis,
                queue_key: self.config.queue_key,
                consumer_group: self.config.consumer_group,
                consumer_name: self.config.consumer_name,
                payload_key: self.config.payload_key,
                _background_tasks: background_tasks.clone(),
            },
        ))
    }

    pub async fn build_producer(self) -> Result<RedisProducer<R>> {
        let redis = R::from_dsn(&self.config.dsn)?;
        let redis = bb8::Pool::builder()
            .max_size(self.config.max_connections.into())
            .build(redis)
            .await
            .map_err(QueueError::generic)?;

        let _background_tasks = self.start_background_tasks(redis.clone()).await;
        Ok(RedisProducer {
            redis,
            queue_key: self.config.queue_key,
            delayed_queue_key: self.config.delayed_queue_key,
            payload_key: self.config.payload_key,
            _background_tasks,
        })
    }

    pub async fn build_consumer(self) -> Result<RedisConsumer<R>> {
        let redis = R::from_dsn(&self.config.dsn)?;
        let redis = bb8::Pool::builder()
            .max_size(self.config.max_connections.into())
            .build(redis)
            .await
            .map_err(QueueError::generic)?;

        let _background_tasks = self.start_background_tasks(redis.clone()).await;

        Ok(RedisConsumer {
            redis,
            queue_key: self.config.queue_key,
            consumer_group: self.config.consumer_group,
            consumer_name: self.config.consumer_name,
            payload_key: self.config.payload_key,
            _background_tasks,
        })
    }

    pub fn make_dynamic(self) -> RedisBackendBuilder<R, Dynamic> {
        RedisBackendBuilder {
            config: self.config,
            _phantom: PhantomData,
        }
    }

    // FIXME(onelson): there's a trait, `SchedulerBackend`, but no obvious way to
    // implement it in a way that makes good sense here.
    // We need access to the pool, and various bits of config to spawn a task, but
    // none of that is available where it matters right now.
    // Doing my own thing for now - standalone function that takes what it needs.
    async fn start_background_tasks(&self, redis: bb8::Pool<R>) -> Arc<JoinSet<Result<()>>> {
        let mut join_set = JoinSet::new();

        // FIXME(onelson): does it even make sense to treat delay support as optional
        // here?
        if self.config.delayed_queue_key.is_empty() {
            warn!("no delayed_queue_key specified - delayed task scheduler disabled");
        } else {
            join_set.spawn({
                let pool = redis.clone();
                let queue_key = self.config.queue_key.to_owned();
                let delayed_queue_key = self.config.delayed_queue_key.to_owned();
                let delayed_lock_key = self.config.delayed_lock_key.to_owned();
                let payload_key = self.config.payload_key.to_owned();

                #[rustfmt::skip]
                debug!(
                    delayed_queue_key, delayed_lock_key,
                    "spawning delayed task scheduler"
                );

                async move {
                    loop {
                        if let Err(err) = background_task_delayed(
                            &pool,
                            &queue_key,
                            &delayed_queue_key,
                            &delayed_lock_key,
                            &payload_key,
                        )
                        .await
                        {
                            error!("{err}");
                            tokio::time::sleep(Duration::from_millis(500)).await;
                            continue;
                        };
                    }
                }
            });
        }

        join_set.spawn(streams::background_task_pending(
            redis.clone(),
            self.config.queue_key.to_owned(),
            self.config.consumer_group.to_owned(),
            self.config.consumer_name.to_owned(),
            self.config.ack_deadline_ms,
        ));

        join_set.spawn({
            async move {
                if let Err(e) = check_eviction_policy(redis.clone()).await {
                    tracing::warn!("{e}");
                }
                Ok(())
            }
        });

        Arc::new(join_set)
    }
}

impl<R: RedisConnection> RedisBackendBuilder<R, Dynamic> {
    pub async fn build_pair(self) -> Result<(DynProducer, DynConsumer)> {
        #[allow(deprecated)]
        let (p, c) = RedisBackend::<R>::new_pair(self.config).await?;
        Ok((p.into_dyn(), c.into_dyn()))
    }

    pub async fn build_producer(self) -> Result<DynProducer> {
        #[allow(deprecated)]
        let p = RedisBackend::<R>::producing_half(self.config).await?;
        Ok(p.into_dyn())
    }

    pub async fn build_consumer(self) -> Result<DynConsumer> {
        #[allow(deprecated)]
        let c = RedisBackend::<R>::consuming_half(self.config).await?;
        Ok(c.into_dyn())
    }
}

/// Moves "due" messages from a sorted set, where delayed messages are shelved,
/// back onto the main queue.
async fn background_task_delayed<R: RedisConnection>(
    pool: &bb8::Pool<R>,
    main_queue_name: &str,
    delayed_queue_name: &str,
    delayed_lock: &str,
    payload_key: &str,
) -> Result<()> {
    const BATCH_SIZE: isize = 50;

    let mut conn = pool.get().await.map_err(QueueError::generic)?;

    // There is a lock on the delayed queue processing to avoid race conditions.
    // So first try to acquire the lock should it not already exist. The lock
    // expires after five seconds in case a worker crashes while holding the
    // lock.
    //
    // Result will be Some("OK") when set or None when not set.
    let resp: Option<String> = conn
        .set_options(
            delayed_lock,
            true,
            SetOptions::default()
                .conditional_set(ExistenceCheck::NX)
                .with_expiration(SetExpiry::PX(5000)),
        )
        .await
        .map_err(QueueError::generic)?;

    if resp.as_deref() == Some("OK") {
        // First look for delayed keys whose time is up and add them to the main queue
        //
        // Subtract 1 from the timestamp to make it exclusive rather than inclusive,
        // preventing premature delivery.
        let timestamp = unix_timestamp(SystemTime::now() - Duration::from_secs(1))
            .map_err(QueueError::generic)?;

        let keys: Vec<RawPayload> = conn
            .zrangebyscore_limit(delayed_queue_name, 0, timestamp, 0, BATCH_SIZE)
            .await
            .map_err(QueueError::generic)?;

        if !keys.is_empty() {
            trace!("Moving {} messages from delayed to main queue", keys.len());

            streams::add_to_main_queue(&keys, main_queue_name, payload_key, &mut *conn).await?;

            // Then remove the tasks from the delayed queue so they aren't resent
            let _: () = conn
                .zrem(delayed_queue_name, keys)
                .await
                .map_err(QueueError::generic)?;

            // Make sure to release the lock after done processing
            let _: () = conn.del(delayed_lock).await.map_err(QueueError::generic)?;
        } else {
            // Make sure to release the lock before sleeping
            let _: () = conn.del(delayed_lock).await.map_err(QueueError::generic)?;

            // Wait for half a second before attempting to fetch again if nothing was found
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    } else {
        // Also sleep half a second if the lock could not be fetched
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    Ok(())
}

pub struct RedisProducer<M: ManageConnection> {
    redis: bb8::Pool<M>,
    queue_key: String,
    delayed_queue_key: String,
    payload_key: String,
    _background_tasks: Arc<JoinSet<Result<()>>>,
}

impl<R: RedisConnection> RedisProducer<R> {
    pub async fn send_raw(&self, payload: &[u8]) -> Result<()> {
        streams::send_raw(self, payload).await
    }

    pub async fn send_serde_json<P: Serialize + Sync>(&self, payload: &P) -> Result<()> {
        let payload = serde_json::to_vec(payload)?;
        self.send_raw(&payload).await
    }

    pub async fn send_raw_scheduled(&self, payload: &[u8], delay: Duration) -> Result<()> {
        let timestamp = unix_timestamp(SystemTime::now() + delay).map_err(QueueError::generic)?;

        self.redis
            .get()
            .await
            .map_err(QueueError::generic)?
            .zadd(
                &self.delayed_queue_key,
                to_delayed_queue_key(payload),
                timestamp,
            )
            .await
            .map_err(QueueError::generic)?;

        trace!(?delay, "event sent");
        Ok(())
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

impl_queue_producer!(RedisProducer<R: RedisConnection>, Vec<u8>);
impl_scheduled_queue_producer!(RedisProducer<R: RedisConnection>, Vec<u8>);

fn unix_timestamp(time: SystemTime) -> Result<u64, SystemTimeError> {
    Ok(time.duration_since(UNIX_EPOCH)?.as_secs())
}

/// Acts as a payload prefix for when payloads are written to zset keys.
///
/// This ensures that messages with identical payloads:
/// - don't only get delivered once instead of N times.
/// - don't replace each other's "delivery due" timestamp.
fn delayed_key_id() -> RawPayload {
    svix_ksuid::Ksuid::new(None, None).to_base62().into_bytes()
}

/// Prefixes a payload with an id, separated by a pipe, e.g `ID|payload`.
fn to_delayed_queue_key(payload: &[u8]) -> RawPayload {
    // Base62-encoded KSUID is always 27 bytes long, 1 byte for separator.
    let mut result = Vec::with_capacity(payload.len() + 28);

    result.extend(delayed_key_id());
    result.push(b'|');
    result.extend(payload.iter().copied());
    result
}

/// Returns the payload portion of a delayed zset key.
fn from_delayed_queue_key(key: &[u8]) -> Result<&[u8]> {
    // All information is stored in the key in which the ID and JSON formatted task
    // are separated by a `|`. So, take the key, then take the part after the `|`.
    let sep_pos = key
        .iter()
        .position(|&byte| byte == b'|')
        .ok_or_else(|| QueueError::Generic("Improper key format".into()))?;
    Ok(&key[sep_pos + 1..])
}

pub struct RedisConsumer<M: ManageConnection> {
    redis: bb8::Pool<M>,
    queue_key: String,
    consumer_group: String,
    consumer_name: String,
    payload_key: String,
    _background_tasks: Arc<JoinSet<Result<()>>>,
}

impl<R: RedisConnection> RedisConsumer<R> {
    pub async fn receive(&mut self) -> Result<Delivery> {
        streams::receive(self).await
    }

    pub async fn receive_all(
        &mut self,
        max_messages: usize,
        deadline: Duration,
    ) -> Result<Vec<Delivery>> {
        streams::receive_all(self, deadline, max_messages).await
    }
}

impl_queue_consumer!(RedisConsumer<R: RedisConnection>, Vec<u8>);
