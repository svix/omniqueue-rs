//! Implementation of the main queue using two lists instead of redis streams,
//! for compatibility with redis versions older than 6.2.0.

use std::time::Duration;

use bb8::ManageConnection;
use redis::AsyncCommands;
use svix_ksuid::{KsuidLike as _, KsuidMs};
use time::OffsetDateTime;
use tracing::{error, trace, warn};

use super::{
    internal_from_list, internal_to_list_payload, DeadLetterQueueConfig, InternalPayload,
    InternalPayloadOwned, RawPayload, RedisConnection, RedisConsumer, RedisProducer,
};
use crate::{queue::Acker, Delivery, QueueError, Result};

pub(super) async fn send_raw<R: RedisConnection>(
    producer: &RedisProducer<R>,
    payload: &[u8],
) -> Result<()> {
    producer
        .redis
        .get()
        .await
        .map_err(QueueError::generic)?
        .lpush(
            &producer.queue_key,
            internal_to_list_payload(InternalPayload::new(payload)),
        )
        .await
        .map_err(QueueError::generic)
}

pub(super) async fn receive<R: RedisConnection>(consumer: &RedisConsumer<R>) -> Result<Delivery> {
    let res = receive_with_timeout(consumer, Duration::ZERO).await?;
    res.ok_or_else(|| QueueError::Generic("No data".into()))
}

pub(super) async fn receive_all<R: RedisConnection>(
    consumer: &RedisConsumer<R>,
    deadline: Duration,
    _max_messages: usize,
) -> Result<Vec<Delivery>> {
    // FIXME: Run up to max_messages RPOPLPUSH'es until there is a null reply?
    let delivery = receive_with_timeout(consumer, deadline).await?;
    Ok(delivery.into_iter().collect())
}

async fn receive_with_timeout<R: RedisConnection>(
    consumer: &RedisConsumer<R>,
    timeout: Duration,
) -> Result<Option<Delivery>> {
    let mut conn = consumer.redis.get().await.map_err(QueueError::generic)?;

    let payload: Option<Vec<u8>> = conn
        .brpop(&consumer.queue_key, timeout.as_secs_f64())
        .await
        .map_err(QueueError::generic)?;

    match payload {
        Some(old_payload) => {
            // Creating a new payload with a new timestamp
            // this is done to avoid the message being re-enqueued
            // too early!
            let new_payload = internal_to_list_payload(internal_from_list(&old_payload)?);

            let _: () = conn
                .lpush(&consumer.processing_queue_key, &new_payload)
                .await
                .map_err(QueueError::generic)?;

            Some(internal_to_delivery(
                internal_from_list(&new_payload)?.into(),
                consumer,
                new_payload,
            ))
            .transpose()
        }
        None => Ok(None),
    }
}

fn internal_to_delivery<R: RedisConnection>(
    InternalPayloadOwned {
        payload,
        num_receives,
    }: InternalPayloadOwned,
    consumer: &RedisConsumer<R>,
    old_payload: Vec<u8>,
) -> Result<Delivery> {
    Ok(Delivery::new(
        payload,
        RedisFallbackAcker {
            redis: consumer.redis.clone(),
            processing_queue_key: consumer.processing_queue_key.clone(),
            old_payload,
            already_acked_or_nacked: false,
            num_receives,
            dlq_config: consumer.dlq_config.clone(),
        },
    ))
}

struct RedisFallbackAcker<M: ManageConnection> {
    redis: bb8::Pool<M>,
    processing_queue_key: String,
    // We delete based on the payload -- and since the
    // `num_receives` changes after receiving it's the
    // `old_payload`, since `num_receives` is part of the
    // payload. Make sense?
    old_payload: RawPayload,

    already_acked_or_nacked: bool,

    num_receives: usize,
    dlq_config: Option<DeadLetterQueueConfig>,
}

impl<R: RedisConnection> Acker for RedisFallbackAcker<R> {
    async fn ack(&mut self) -> Result<()> {
        if self.already_acked_or_nacked {
            return Err(QueueError::CannotAckOrNackTwice);
        }

        let _: () = self
            .redis
            .get()
            .await
            .map_err(QueueError::generic)?
            .lrem(&self.processing_queue_key, 1, &self.old_payload)
            .await
            .map_err(QueueError::generic)?;

        self.already_acked_or_nacked = true;

        Ok(())
    }

    async fn nack(&mut self) -> Result<()> {
        if let Some(dlq_config) = &self.dlq_config {
            if dlq_config.max_retries_reached(self.num_receives) {
                trace!("Maximum attempts reached");
                // Try to get the raw payload, but if that fails (which
                // seems possible given that we're already in a failure
                // scenario), just push the full `InternalPayload` onto the DLQ:
                let payload = match internal_from_list(&self.old_payload) {
                    Ok(InternalPayload { payload, .. }) => payload,
                    Err(e) => {
                        warn!(error = ?e, "Failed to get original payload, sending to DLQ with internal payload");
                        &self.old_payload
                    }
                };
                send_to_dlq(&self.redis, dlq_config, payload).await?;
                return self.ack().await;
            }
        }

        if self.already_acked_or_nacked {
            return Err(QueueError::CannotAckOrNackTwice);
        }

        self.already_acked_or_nacked = true;

        Ok(())
    }

    async fn set_ack_deadline(&mut self, _duration: Duration) -> Result<()> {
        Err(QueueError::Unsupported(
            "set_ack_deadline is not yet supported by redis fallback backend",
        ))
    }
}

pub(super) async fn add_to_main_queue(
    keys: Vec<InternalPayload<'_>>,
    main_queue_name: &str,
    conn: &mut impl AsyncCommands,
) -> Result<()> {
    // We don't care about existing `num_receives`
    // since we're pushing onto a different queue.
    let new_keys = keys
        .into_iter()
        // So reset it to avoid carrying state over:
        .map(|x| InternalPayload::new(x.payload))
        .map(internal_to_list_payload)
        .collect::<Vec<_>>();
    let _: () = conn
        .lpush(main_queue_name, new_keys)
        .await
        .map_err(QueueError::generic)?;
    Ok(())
}

pub(super) async fn background_task_processing<R: RedisConnection>(
    pool: bb8::Pool<R>,
    queue_key: String,
    processing_queue_key: String,
    ack_deadline_ms: i64,
    dlq_config: Option<DeadLetterQueueConfig>,
) -> Result<()> {
    // FIXME: ack_deadline_ms should be unsigned
    let ack_deadline = Duration::from_millis(ack_deadline_ms as _);
    loop {
        if let Err(err) = reenqueue_timed_out_messages(
            &pool,
            &queue_key,
            &processing_queue_key,
            ack_deadline,
            &dlq_config,
        )
        .await
        {
            error!("{err}");
            tokio::time::sleep(Duration::from_millis(500)).await;
            continue;
        }
    }
}

async fn send_to_dlq<R: RedisConnection>(
    redis: &bb8::Pool<R>,
    dlq_config: &DeadLetterQueueConfig,
    payload: &[u8],
) -> Result<()> {
    let DeadLetterQueueConfig { queue_key: dlq, .. } = dlq_config;

    let _: () = redis
        .get()
        .await
        .map_err(QueueError::generic)?
        .rpush(dlq, payload)
        .await
        .map_err(QueueError::generic)?;

    Ok(())
}

async fn reenqueue_timed_out_messages<R: RedisConnection>(
    pool: &bb8::Pool<R>,
    queue_key: &str,
    processing_queue_key: &str,
    ack_deadline: Duration,
    dlq_config: &Option<DeadLetterQueueConfig>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    const BATCH_SIZE: isize = 50;

    let mut conn = pool.get().await?;

    let keys: Vec<RawPayload> = conn.lrange(processing_queue_key, -1, -1).await?;

    // If the key is older than now, it means we should be processing keys
    let validity_limit = KsuidMs::new(Some(OffsetDateTime::now_utc() - ack_deadline), None)
        .to_string()
        .into_bytes();

    if !keys.is_empty() && keys[0] <= validity_limit {
        let keys: Vec<RawPayload> = conn.lrange(processing_queue_key, -BATCH_SIZE, -1).await?;
        for key in keys {
            if key <= validity_limit {
                let internal = internal_from_list(&key)?;
                let num_receives = internal.num_receives;

                match &dlq_config {
                    Some(dlq_config) if dlq_config.max_retries_reached(num_receives) => {
                        trace!(
                            num_receives = num_receives,
                            "Maximum attempts reached for message, moving item to DLQ",
                        );
                        send_to_dlq(pool, dlq_config, internal.payload).await?;
                    }
                    _ => {
                        trace!(
                            num_receives = num_receives,
                            "Pushing back overdue task to queue"
                        );
                        let _: () = conn
                            .rpush(queue_key, internal_to_list_payload(internal))
                            .await?;
                    }
                }

                // We use LREM to be sure we only delete the keys we should be deleting
                let _: () = conn.lrem(processing_queue_key, 1, &key).await?;
            }
        }
    } else {
        // Sleep before attempting to fetch again if nothing was found
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    Ok(())
}
