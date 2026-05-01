//! Implementation of the main queue using two lists instead of redis streams,
//! for compatibility with redis versions older than 6.2.0.

use std::{sync::OnceLock, time::Duration};

use bb8::ManageConnection;
use redis::AsyncCommands;
use svix_ksuid::{KsuidLike as _, KsuidMs};
use time::OffsetDateTime;
use tracing::{error, trace, warn};

use super::{
    internal_from_list, internal_to_list_payload, list_entry_id, DeadLetterQueueConfig,
    InternalPayload, InternalPayloadOwned, RawPayload, RedisConnection, RedisConsumer,
    RedisProducer,
};
use crate::{queue::Acker, Delivery, QueueError, Result};

static REFRESH_TIMESTAMP: OnceLock<redis::Script> = OnceLock::new();
static REENQUEUE_SCRIPT: OnceLock<redis::Script> = OnceLock::new();

fn refresh_timestamp_script() -> &'static redis::Script {
    REFRESH_TIMESTAMP.get_or_init(|| {
        redis::Script::new(
            r"local processing_queue = KEYS[1]
              local old_payload      = ARGV[1]
              local refreshed_payload = ARGV[2]
              redis.call('LPUSH', processing_queue, refreshed_payload)
              redis.call('LREM',  processing_queue, 1, old_payload)",
        )
    })
}

fn reenqueue_script() -> &'static redis::Script {
    REENQUEUE_SCRIPT.get_or_init(|| {
        // RPUSH before LREM so a crash never loses the message (it ends up in
        // both queues rather than neither). If LREM returns 0, another process
        // already handled this item, so we undo our RPUSH. new_payload carries
        // a unique KSUID so the undo-LREM only removes the payload we just pushed.
        redis::Script::new(
            "local processing_queue = KEYS[1]
             local dest_queue       = KEYS[2]
             local old_payload      = ARGV[1]
             local new_payload      = ARGV[2]
             redis.call('RPUSH', dest_queue, new_payload)
             local removed = redis.call('LREM', processing_queue, 1, old_payload)
             if removed == 0 then
                 redis.call('LREM', dest_queue, 1, new_payload)
             end
             return removed",
        )
    })
}

fn payload_is_older_than(raw_payload: &[u8], threshold: Duration) -> bool {
    let id = list_entry_id(raw_payload);
    let threshold_ksuid = KsuidMs::new(Some(OffsetDateTime::now_utc() - threshold), None)
        .to_string()
        .into_bytes();
    id <= threshold_ksuid.as_slice()
}

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

    let Some(list_entry): Option<Vec<u8>> = conn
        .brpoplpush(
            &consumer.queue_key,
            &consumer.processing_queue_key,
            // The documentation at https://redis.io/docs/latest/commands/brpoplpush/ does not
            // state what unit the timeout is, but `BLPOP` and `BLMPOP` have similar timeout
            // parameters that are documented as being seconds.
            timeout.as_secs_f64(),
        )
        .await
        .map_err(QueueError::generic)?
    else {
        return Ok(None);
    };

    let internal = internal_from_list(&list_entry)?;
    let num_receives = internal.num_receives;
    let inner_payload = internal.payload.to_vec();

    // Refresh the timestamp of the item (i.e., reenqueue it with a new id)
    // in the processing queue if the original message is older than `cutoff`:
    let cutoff = (consumer.ack_deadline / 2).min(Duration::from_secs(5));
    let ack_key = if payload_is_older_than(&list_entry, cutoff) {
        let refreshed = internal_to_list_payload(InternalPayload {
            payload: &inner_payload,
            num_receives: num_receives - 1,
        });
        refresh_timestamp_script()
            .key(&consumer.processing_queue_key)
            .arg(&list_entry)
            .arg(&refreshed)
            .invoke_async::<()>(&mut *conn)
            .await
            .map_err(QueueError::generic)?;
        refreshed
    } else {
        list_entry
    };

    Some(internal_to_delivery(
        InternalPayloadOwned {
            payload: inner_payload,
            num_receives,
        },
        consumer,
        ack_key,
    ))
    .transpose()
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
    poll_interval: Duration,
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
            poll_interval,
        )
        .await
        {
            error!("{err}");
            tokio::time::sleep(poll_interval).await;
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
    poll_interval: Duration,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    const BATCH_SIZE: isize = 50;

    let mut conn = pool.get().await?;

    let keys: Vec<RawPayload> = conn.lrange(processing_queue_key, -1, -1).await?;

    let deadline = OffsetDateTime::now_utc() - ack_deadline;
    // If the key is older than now, it means we should be processing keys
    let validity_limit = KsuidMs::new(Some(deadline), None).to_string().into_bytes();

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
                        reenqueue_script()
                            .key(processing_queue_key)
                            .key(&dlq_config.queue_key)
                            .arg(&key)
                            .arg(internal.payload)
                            .invoke_async::<()>(&mut *conn)
                            .await?;
                    }
                    _ => {
                        trace!(
                            deadline = ?deadline,
                            num_receives = num_receives,
                            "Pushing back overdue task to queue"
                        );
                        reenqueue_script()
                            .key(processing_queue_key)
                            .key(queue_key)
                            .arg(&key)
                            .arg(internal_to_list_payload(internal))
                            .invoke_async::<()>(&mut *conn)
                            .await?;
                    }
                }
            }
        }
    } else {
        tokio::time::sleep(poll_interval).await;
    }

    Ok(())
}
