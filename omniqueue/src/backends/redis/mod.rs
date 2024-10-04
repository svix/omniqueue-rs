//! Redis queue implementation.
//!
//! By default, this uses redis streams. There is a fallback implementation that
//! you can select via `RedisBackend::builder(cfg).use_redis_streams(false)`.
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
    str,
    sync::Arc,
    time::{Duration, SystemTime, SystemTimeError, UNIX_EPOCH},
};

use bb8::ManageConnection;
pub use bb8_redis::RedisConnectionManager;
#[cfg(feature = "redis_sentinel")]
use redis::{sentinel::SentinelNodeConnectionInfo, ProtocolVersion, RedisConnectionInfo, TlsMode};
use redis::{AsyncCommands, ExistenceCheck, SetExpiry, SetOptions};
use serde::Serialize;
use svix_ksuid::KsuidLike;
use thiserror::Error;
use tokio::task::JoinSet;
use tracing::{debug, error, info, trace, warn};

#[allow(deprecated)]
use crate::{
    builder::{Dynamic, Static},
    queue::{Delivery, QueueBackend},
    DynConsumer, DynProducer, QueueConsumer as _, QueueError, QueueProducer as _, Result,
};

#[cfg(feature = "redis_cluster")]
mod cluster;
mod fallback;
#[cfg(feature = "redis_sentinel")]
mod sentinel;
mod streams;

#[cfg(feature = "redis_cluster")]
pub use cluster::RedisClusterConnectionManager;
#[cfg(feature = "redis_sentinel")]
pub use sentinel::RedisSentinelConnectionManager;

pub trait RedisConnection:
    ManageConnection<
    Connection: redis::aio::ConnectionLike + Send + Sync,
    Error: std::error::Error + Send + Sync + 'static,
>
{
    fn from_config(config: &RedisConfig) -> Result<Self>;
}

impl RedisConnection for RedisConnectionManager {
    fn from_config(config: &RedisConfig) -> Result<Self> {
        Self::new(config.dsn.as_str()).map_err(QueueError::generic)
    }
}

#[cfg(feature = "redis_cluster")]
impl RedisConnection for RedisClusterConnectionManager {
    fn from_config(config: &RedisConfig) -> Result<Self> {
        Self::new(config.dsn.as_str()).map_err(QueueError::generic)
    }
}

#[cfg(feature = "redis_sentinel")]
impl RedisConnection for RedisSentinelConnectionManager {
    fn from_config(config: &RedisConfig) -> Result<Self> {
        let cfg = config
            .sentinel_config
            .clone()
            .ok_or(QueueError::Unsupported("Missing sentinel configuration"))?;

        let tls_mode = cfg.redis_tls_mode_secure.then_some(TlsMode::Secure);
        let protocol = if cfg.redis_use_resp3 {
            ProtocolVersion::RESP3
        } else {
            ProtocolVersion::default()
        };
        RedisSentinelConnectionManager::new(
            vec![config.dsn.as_str()],
            cfg.service_name.clone(),
            Some(SentinelNodeConnectionInfo {
                tls_mode,
                redis_connection_info: Some(RedisConnectionInfo {
                    db: cfg.redis_db.unwrap_or(0),
                    username: cfg.redis_username.clone(),
                    password: cfg.redis_password.clone(),
                    protocol,
                }),
            }),
        )
        .map_err(QueueError::generic)
    }
}

// First element is the raw payload slice, second
// is `num_receives`, the number of the times
// the message has previously been received.
struct InternalPayload<'a> {
    payload: &'a [u8],
    num_receives: usize,
}

impl<'a> InternalPayload<'a> {
    fn new(payload: &'a [u8]) -> Self {
        Self {
            payload,
            num_receives: 0,
        }
    }
}

// The same as `InternalPayload` but with an
// owned payload.
struct InternalPayloadOwned {
    payload: Vec<u8>,
    num_receives: usize,
}

impl From<InternalPayload<'_>> for InternalPayloadOwned {
    fn from(
        InternalPayload {
            payload,
            num_receives,
        }: InternalPayload,
    ) -> Self {
        Self {
            payload: payload.to_vec(),
            num_receives,
        }
    }
}

fn internal_from_list(payload: &[u8]) -> Result<InternalPayload<'_>> {
    // All information is stored in the key in which the ID and the [optional]
    // number of prior receives are separated by a `#`, and the JSON
    // formatted task is delimited by a `|` So, take the key, then take the
    // optional receive count, then take the part after the `|` to get the
    // payload.
    let count_sep_pos = payload.iter().position(|&byte| byte == b'#');
    let payload_sep_pos = payload
        .iter()
        .position(|&byte| byte == b'|')
        .ok_or_else(|| QueueError::Generic("Improper key format".into()))?;

    let id_end_pos = match count_sep_pos {
        Some(count_sep_pos) if count_sep_pos < payload_sep_pos => count_sep_pos,
        _ => payload_sep_pos,
    };
    let _id = str::from_utf8(&payload[..id_end_pos])
        .map_err(|_| QueueError::Generic("Non-UTF8 key ID".into()))?;

    // This should be backward-compatible with messages that don't include
    // `num_receives`
    let num_receives = if let Some(count_sep_pos) = count_sep_pos {
        let num_receives = std::str::from_utf8(&payload[(count_sep_pos + 1)..payload_sep_pos])
            .map_err(|_| QueueError::Generic("Improper key format".into()))?
            .parse::<usize>()
            .map_err(|_| QueueError::Generic("Improper key format".into()))?;
        num_receives + 1
    } else {
        1
    };

    Ok(InternalPayload {
        payload: &payload[payload_sep_pos + 1..],
        num_receives,
    })
}

fn internal_to_list_payload(
    InternalPayload {
        payload,
        num_receives,
    }: InternalPayload,
) -> Vec<u8> {
    let id = delayed_key_id();
    let num_receives = num_receives.to_string();
    let mut result =
        Vec::with_capacity(id.len() + num_receives.as_bytes().len() + payload.len() + 3);
    result.extend(id.as_bytes());
    result.push(b'#');
    result.extend(num_receives.as_bytes());
    result.push(b'|');
    result.extend(payload);
    result
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
        .query_async(&mut *conn)
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
    pub dlq_config: Option<DeadLetterQueueConfig>,
    pub sentinel_config: Option<SentinelConfig>,
}

#[derive(Clone)]
pub struct SentinelConfig {
    pub service_name: String,
    pub redis_tls_mode_secure: bool,
    pub redis_db: Option<i64>,
    pub redis_username: Option<String>,
    pub redis_password: Option<String>,
    pub redis_use_resp3: bool,
}

#[derive(Clone)]
pub struct DeadLetterQueueConfig {
    // The name of the deadletter queue, which is a simple
    // list data type.
    pub queue_key: String,
    /// The number of times a message may be received before
    /// being sent to the deadletter queue.
    pub max_receives: usize,
}

impl DeadLetterQueueConfig {
    fn max_retries_reached(&self, num_receives: usize) -> bool {
        num_receives >= self.max_receives
    }
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

    #[cfg(feature = "redis_sentinel")]
    /// Creates a new redis sentinel queue builder with the given configuration.
    pub fn sentinel_builder(config: RedisConfig) -> RedisSentinelBackendBuilder {
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
    use_redis_streams: bool,
    processing_queue_key: Option<String>,
    _phantom: PhantomData<fn() -> (R, S)>,
}

#[cfg(feature = "redis_cluster")]
pub type RedisClusterBackendBuilder = RedisBackendBuilder<RedisClusterConnectionManager>;

#[cfg(feature = "redis_sentinel")]
pub type RedisSentinelBackendBuilder = RedisBackendBuilder<RedisSentinelConnectionManager>;

impl<R: RedisConnection> RedisBackendBuilder<R> {
    pub fn new(config: RedisConfig) -> Self {
        Self {
            config,
            use_redis_streams: true,
            processing_queue_key: None,
            _phantom: PhantomData,
        }
    }

    fn map_phantom<R2, S2>(self) -> RedisBackendBuilder<R2, S2> {
        RedisBackendBuilder {
            config: self.config,
            use_redis_streams: self.use_redis_streams,
            processing_queue_key: self.processing_queue_key,
            _phantom: PhantomData,
        }
    }

    /// Set a custom [`RedisConnection`] manager to use.
    ///
    /// This method only makes sense to call if you have a custom connection
    /// manager implementation. For clustered redis, use
    /// [`.cluster()`][Self::cluster].
    pub fn connection_manager<R2>(self) -> RedisBackendBuilder<R2> {
        self.map_phantom()
    }

    #[cfg(feature = "redis_cluster")]
    pub fn cluster(self) -> RedisBackendBuilder<RedisClusterConnectionManager> {
        self.connection_manager()
    }

    /// Whether to use redis streams.
    ///
    /// Default: `true`.\
    /// Set this to `false` if you want to use this backend with a version of
    /// redis older than 6.2.0.
    ///
    /// Note: Make sure this setting matches between producers and consumers,
    /// and don't change it as part of an upgrade unless you have made sure to
    /// either empty the previous data, migrate it yourself or use different
    /// queue keys.
    pub fn use_redis_streams(mut self, value: bool) -> Self {
        self.use_redis_streams = value;
        self
    }

    /// Set a custom redis key for the processing queue.
    ///
    /// This secondary queue is only used if you set
    /// <code>[`use_redis_streams`][Self::use_redis_streams](false)</code>.
    /// If you don't set a custom key, one will be selected based on the main
    /// queue key. You only have to call this if you need precise control over
    /// which keys omniqueue uses in redis.
    pub fn processing_queue_key(mut self, value: String) -> Self {
        self.processing_queue_key = Some(value);
        self
    }

    fn get_processing_queue_key(&self) -> String {
        self.processing_queue_key
            .clone()
            .unwrap_or_else(|| format!("{}_processing", self.config.queue_key))
    }

    pub async fn build_pair(self) -> Result<(RedisProducer<R>, RedisConsumer<R>)> {
        let redis = R::from_config(&self.config)?;
        let redis = bb8::Pool::builder()
            .max_size(self.config.max_connections.into())
            .build(redis)
            .await
            .map_err(QueueError::generic)?;

        let background_tasks = self.start_background_tasks(redis.clone()).await;
        let processing_queue_key = self.get_processing_queue_key();

        Ok((
            RedisProducer {
                redis: redis.clone(),
                queue_key: self.config.queue_key.clone(),
                delayed_queue_key: self.config.delayed_queue_key,
                payload_key: self.config.payload_key.clone(),
                use_redis_streams: self.use_redis_streams,
                _background_tasks: background_tasks.clone(),
                dlq_config: self.config.dlq_config.clone(),
            },
            RedisConsumer {
                redis,
                queue_key: self.config.queue_key,
                processing_queue_key,
                consumer_group: self.config.consumer_group,
                consumer_name: self.config.consumer_name,
                payload_key: self.config.payload_key,
                use_redis_streams: self.use_redis_streams,
                _background_tasks: background_tasks.clone(),
                dlq_config: self.config.dlq_config.clone(),
            },
        ))
    }

    pub async fn build_producer(self) -> Result<RedisProducer<R>> {
        let redis = R::from_config(&self.config)?;
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
            use_redis_streams: self.use_redis_streams,
            _background_tasks,
            dlq_config: self.config.dlq_config,
        })
    }

    pub async fn build_consumer(self) -> Result<RedisConsumer<R>> {
        let redis = R::from_config(&self.config)?;
        let redis = bb8::Pool::builder()
            .max_size(self.config.max_connections.into())
            .build(redis)
            .await
            .map_err(QueueError::generic)?;

        let _background_tasks = self.start_background_tasks(redis.clone()).await;
        let processing_queue_key = self.get_processing_queue_key();

        Ok(RedisConsumer {
            redis,
            queue_key: self.config.queue_key,
            processing_queue_key,
            consumer_group: self.config.consumer_group,
            consumer_name: self.config.consumer_name,
            payload_key: self.config.payload_key,
            use_redis_streams: self.use_redis_streams,
            _background_tasks,
            dlq_config: self.config.dlq_config,
        })
    }

    pub fn make_dynamic(self) -> RedisBackendBuilder<R, Dynamic> {
        self.map_phantom()
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
                let use_redis_streams = self.use_redis_streams;

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
                            use_redis_streams,
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

        if self.use_redis_streams {
            join_set.spawn(streams::background_task_pending(
                redis.clone(),
                self.config.queue_key.to_owned(),
                self.config.consumer_group.to_owned(),
                self.config.consumer_name.to_owned(),
                self.config.ack_deadline_ms,
                self.config.payload_key.to_owned(),
                self.config.dlq_config.clone(),
            ));
        } else {
            join_set.spawn(fallback::background_task_processing(
                redis.clone(),
                self.config.queue_key.to_owned(),
                self.get_processing_queue_key(),
                self.config.ack_deadline_ms,
                self.config.dlq_config.clone(),
            ));
        }

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
    use_redis_streams: bool,
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

        let old_keys: Vec<RawPayload> = conn
            .zrangebyscore_limit(delayed_queue_name, 0, timestamp, 0, BATCH_SIZE)
            .await
            .map_err(QueueError::generic)?;

        if !old_keys.is_empty() {
            let new_keys = old_keys
                .iter()
                .map(|x| internal_from_list(x))
                .collect::<Result<Vec<_>>>()?;
            trace!(
                "Moving {} messages from delayed to main queue",
                new_keys.len()
            );

            if use_redis_streams {
                streams::add_to_main_queue(new_keys, main_queue_name, payload_key, &mut *conn)
                    .await?;
            } else {
                fallback::add_to_main_queue(new_keys, main_queue_name, &mut *conn).await?;
            }

            // Then remove the tasks from the delayed queue so they aren't resent
            let _: () = conn
                .zrem(delayed_queue_name, old_keys)
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
    use_redis_streams: bool,
    _background_tasks: Arc<JoinSet<Result<()>>>,
    dlq_config: Option<DeadLetterQueueConfig>,
}

impl<R: RedisConnection> RedisProducer<R> {
    #[tracing::instrument(
        name = "send",
        skip_all,
        fields(payload_size = payload.len())
    )]
    pub async fn send_raw(&self, payload: &[u8]) -> Result<()> {
        if self.use_redis_streams {
            streams::send_raw(self, payload).await
        } else {
            fallback::send_raw(self, payload).await
        }
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
        let timestamp = unix_timestamp(SystemTime::now() + delay).map_err(QueueError::generic)?;

        let _: () = self
            .redis
            .get()
            .await
            .map_err(QueueError::generic)?
            .zadd(
                &self.delayed_queue_key,
                internal_to_list_payload(InternalPayload::new(payload)),
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

    pub async fn redrive_dlq(&self) -> Result<()> {
        const BATCH_SIZE: isize = 50;

        let DeadLetterQueueConfig { queue_key: dlq, .. } = self
            .dlq_config
            .as_ref()
            .ok_or(QueueError::Unsupported("Missing DeadLetterQueueConfig"))?;

        loop {
            let mut conn = self.redis.get().await.map_err(QueueError::generic)?;
            let old_payloads: Vec<RawPayload> = conn
                .lrange(dlq, 0, BATCH_SIZE)
                .await
                .map_err(QueueError::generic)?;

            if old_payloads.is_empty() {
                break;
            }

            let new_payloads = old_payloads
                .iter()
                .map(|x| InternalPayload::new(x))
                .collect::<Vec<_>>();

            if self.use_redis_streams {
                streams::add_to_main_queue(
                    new_payloads,
                    &self.queue_key,
                    &self.payload_key,
                    &mut *conn,
                )
                .await?;
            } else {
                // This may fail if messages in the key are not in their original raw format.
                fallback::add_to_main_queue(new_payloads, &self.queue_key, &mut *conn).await?;
            }

            let payload_len = old_payloads.len();
            for payload in old_payloads {
                let _: () = conn
                    .lrem(dlq, 1, &payload)
                    .await
                    .map_err(QueueError::generic)?;
            }
            info!("Moved {payload_len} items from deadletter queue to main queue");
        }

        Ok(())
    }
}

impl<R: RedisConnection> crate::QueueProducer for RedisProducer<R> {
    type Payload = Vec<u8>;
    omni_delegate!(send_raw, send_serde_json, redrive_dlq);
}
impl<R: RedisConnection> crate::ScheduledQueueProducer for RedisProducer<R> {
    omni_delegate!(send_raw_scheduled, send_serde_json_scheduled);
}

fn unix_timestamp(time: SystemTime) -> Result<u64, SystemTimeError> {
    Ok(time.duration_since(UNIX_EPOCH)?.as_secs())
}

/// Acts as a payload prefix for when payloads are written to zset keys.
///
/// This ensures that messages with identical payloads:
/// - don't only get delivered once instead of N times.
/// - don't replace each other's "delivery due" timestamp.
fn delayed_key_id() -> String {
    svix_ksuid::Ksuid::new(None, None).to_base62()
}

pub struct RedisConsumer<M: ManageConnection> {
    redis: bb8::Pool<M>,
    queue_key: String,
    processing_queue_key: String,
    consumer_group: String,
    consumer_name: String,
    payload_key: String,
    use_redis_streams: bool,
    _background_tasks: Arc<JoinSet<Result<()>>>,
    dlq_config: Option<DeadLetterQueueConfig>,
}

impl<R: RedisConnection> RedisConsumer<R> {
    pub async fn receive(&mut self) -> Result<Delivery> {
        if self.use_redis_streams {
            streams::receive(self).await
        } else {
            fallback::receive(self).await
        }
    }

    pub async fn receive_all(
        &mut self,
        max_messages: usize,
        deadline: Duration,
    ) -> Result<Vec<Delivery>> {
        if self.use_redis_streams {
            streams::receive_all(self, deadline, max_messages).await
        } else {
            fallback::receive_all(self, deadline, max_messages).await
        }
    }
}

impl<R: RedisConnection> crate::QueueConsumer for RedisConsumer<R> {
    type Payload = Vec<u8>;
    omni_delegate!(receive, receive_all);
}
