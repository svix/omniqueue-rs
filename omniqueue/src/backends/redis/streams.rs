//! Implementation of the main queue using redis streams.

use std::time::Duration;

use bb8::ManageConnection;
use redis::{
    streams::{
        StreamAutoClaimOptions, StreamClaimReply, StreamId, StreamRangeReply, StreamReadOptions,
        StreamReadReply,
    },
    AsyncCommands as _, FromRedisValue, RedisResult,
};
use tracing::{error, trace};

use super::{
    DeadLetterQueueConfig, InternalPayload, InternalPayloadOwned, RedisConnection, RedisConsumer,
    RedisProducer,
};
use crate::{queue::Acker, Delivery, QueueError, Result};

/// Special ID for XADD command's which generates a stream ID automatically
const GENERATE_STREAM_ID: &str = "*";
/// Special ID for XREADGROUP commands which reads any new messages
const LISTEN_STREAM_ID: &str = ">";

/// The maximum number of pending messages to reinsert into the queue after
/// becoming stale per loop
// FIXME(onelson): expose in config?
const PENDING_BATCH_SIZE: usize = 1000;

macro_rules! internal_to_stream_payload {
    ($internal_payload:expr, $payload_key:expr) => {
        &[
            ($payload_key, $internal_payload.payload),
            (
                NUM_RECEIVES,
                $internal_payload.num_receives.to_string().as_bytes(),
            ),
        ]
    };
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
        .xadd(
            &producer.queue_key,
            GENERATE_STREAM_ID,
            internal_to_stream_payload!(
                InternalPayload::new(payload),
                producer.payload_key.as_str()
            ),
        )
        .await
        .map_err(QueueError::generic)
}

pub(super) async fn receive<R: RedisConnection>(consumer: &RedisConsumer<R>) -> Result<Delivery> {
    // Ensure an empty vec is never returned
    let read_out: StreamReadReply = consumer
        .redis
        .get()
        .await
        .map_err(QueueError::generic)?
        .xread_options(
            &[&consumer.queue_key],
            &[LISTEN_STREAM_ID],
            &StreamReadOptions::default()
                .group(&consumer.consumer_group, &consumer.consumer_name)
                .block(100_000)
                .count(1),
        )
        .await
        .map_err(QueueError::generic)?;

    let queue = read_out.keys.into_iter().next().ok_or(QueueError::NoData)?;
    let entry = queue.ids.into_iter().next().ok_or(QueueError::NoData)?;

    let internal = internal_from_stream(&entry, &consumer.payload_key)?;
    Ok(internal_to_delivery(internal, consumer, entry.id))
}

pub(super) async fn receive_all<R: RedisConnection>(
    consumer: &RedisConsumer<R>,
    deadline: Duration,
    max_messages: usize,
) -> Result<Vec<Delivery>> {
    let read_out: StreamReadReply = consumer
        .redis
        .get()
        .await
        .map_err(QueueError::generic)?
        .xread_options(
            &[&consumer.queue_key],
            &[LISTEN_STREAM_ID],
            &StreamReadOptions::default()
                .group(&consumer.consumer_group, &consumer.consumer_name)
                .block(
                    deadline
                        .as_millis()
                        .try_into()
                        .map_err(QueueError::generic)?,
                )
                .count(max_messages),
        )
        .await
        .map_err(QueueError::generic)?;

    let mut out = Vec::with_capacity(max_messages);

    if let Some(queue) = read_out.keys.into_iter().next() {
        for entry in queue.ids {
            let internal = internal_from_stream(&entry, &consumer.payload_key)?;
            let delivery = internal_to_delivery(internal, consumer, entry.id);
            out.push(delivery);
        }
    }
    Ok(out)
}

const NUM_RECEIVES: &str = "num_receives";

fn internal_from_stream(stream_id: &StreamId, payload_key: &str) -> Result<InternalPayloadOwned> {
    let StreamId { map, .. } = stream_id;

    let num_receives = if let Some(redis::Value::BulkString(data)) = map.get(NUM_RECEIVES) {
        let count = std::str::from_utf8(data)
            .map_err(|_| QueueError::Generic("Improper key format".into()))?
            .parse::<usize>()
            .map_err(QueueError::generic)?;
        count + 1
    } else {
        1
    };

    let payload: Vec<u8> = map
        .get(payload_key)
        .ok_or(QueueError::NoData)
        .and_then(|x| redis::from_redis_value(x).map_err(QueueError::generic))?;

    Ok(InternalPayloadOwned {
        payload,
        num_receives,
    })
}

fn internal_to_delivery<R: RedisConnection>(
    InternalPayloadOwned {
        payload,
        num_receives,
    }: InternalPayloadOwned,
    consumer: &RedisConsumer<R>,
    entry_id: String,
) -> Delivery {
    Delivery::new(
        payload,
        RedisStreamsAcker {
            redis: consumer.redis.clone(),
            queue_key: consumer.queue_key.to_owned(),
            consumer_group: consumer.consumer_group.to_owned(),
            entry_id,
            already_acked_or_nacked: false,
            num_receives,
            dlq_config: consumer.dlq_config.clone(),
            payload_key: consumer.payload_key.clone(),
        },
    )
}

struct RedisStreamsAcker<M: ManageConnection> {
    redis: bb8::Pool<M>,
    queue_key: String,
    consumer_group: String,
    entry_id: String,
    payload_key: String,

    already_acked_or_nacked: bool,
    num_receives: usize,
    dlq_config: Option<DeadLetterQueueConfig>,
}

impl<R: RedisConnection> RedisStreamsAcker<R> {}

impl<R: RedisConnection> Acker for RedisStreamsAcker<R> {
    async fn ack(&mut self) -> Result<()> {
        if self.already_acked_or_nacked {
            return Err(QueueError::CannotAckOrNackTwice);
        }

        let mut pipeline = redis::pipe();
        pipeline.xack(&self.queue_key, &self.consumer_group, &[&self.entry_id]);
        pipeline.xdel(&self.queue_key, &[&self.entry_id]);

        let mut conn = self.redis.get().await.map_err(QueueError::generic)?;
        let _: () = pipeline
            .query_async(&mut *conn)
            .await
            .map_err(QueueError::generic)?;

        self.already_acked_or_nacked = true;

        Ok(())
    }

    async fn nack(&mut self) -> Result<()> {
        if let Some(dlq_config) = &self.dlq_config {
            if dlq_config.max_retries_reached(self.num_receives) {
                trace!(entry_id = self.entry_id, "Maximum attempts reached");
                send_to_dlq(
                    &self.redis,
                    &self.queue_key,
                    dlq_config,
                    &self.entry_id,
                    &self.payload_key,
                )
                .await?;
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
            "set_ack_deadline is not yet supported by redis streams backend",
        ))
    }
}

pub(super) async fn add_to_main_queue(
    keys: Vec<InternalPayload<'_>>,
    main_queue_name: &str,
    payload_key: &str,
    conn: &mut impl redis::aio::ConnectionLike,
) -> Result<()> {
    let mut pipe = redis::pipe();
    // We don't care about existing `num_receives`
    // since we're pushing onto a different queue.
    for InternalPayload { payload, .. } in keys {
        // So reset it to avoid carrying state over:
        let internal = InternalPayload::new(payload);
        let _ = pipe.xadd(
            main_queue_name,
            GENERATE_STREAM_ID,
            internal_to_stream_payload!(internal, payload_key),
        );
    }

    let _: () = pipe.query_async(conn).await.map_err(QueueError::generic)?;

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

/// Scoops up messages that have been claimed but not handled by a deadline,
/// then re-queues them.
pub(super) async fn background_task_pending<R: RedisConnection>(
    pool: bb8::Pool<R>,
    queue_key: String,
    consumer_group: String,
    consumer_name: String,
    ack_deadline_ms: i64,
    payload_key: String,
    dlq_config: Option<DeadLetterQueueConfig>,
) -> Result<()> {
    loop {
        if let Err(err) = reenqueue_timed_out_messages(
            &pool,
            &queue_key,
            &consumer_group,
            &consumer_name,
            ack_deadline_ms,
            &payload_key,
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

// In order to put it in the DLQ, we first have to get the payload
// from the original message, then push it onto the list. An
// alternative would be to store the full payload on the `Acker` as
// we do with the fallback implementation, but it seems good to
// avoid the additional memory utilization if possible.
async fn send_to_dlq<R: RedisConnection>(
    redis: &bb8::Pool<R>,
    main_queue_key: &str,
    dlq_config: &DeadLetterQueueConfig,
    entry_id: &str,
    payload_key: &str,
) -> Result<()> {
    let DeadLetterQueueConfig { queue_key: dlq, .. } = dlq_config;
    let StreamRangeReply { ids, .. } = redis
        .get()
        .await
        .map_err(QueueError::generic)?
        .xrange(main_queue_key, entry_id, entry_id)
        .await
        .map_err(QueueError::generic)?;

    let payload = ids.first().ok_or_else(|| QueueError::NoData)?;
    let payload: Vec<u8> = payload
        .map
        .get(payload_key)
        .ok_or(QueueError::NoData)
        .and_then(|x| redis::from_redis_value(x).map_err(QueueError::generic))?;

    let _: () = redis
        .get()
        .await
        .map_err(QueueError::generic)?
        .rpush(dlq, &payload)
        .await
        .map_err(QueueError::generic)?;

    Ok(())
}

async fn reenqueue_timed_out_messages<R: RedisConnection>(
    pool: &bb8::Pool<R>,
    main_queue_name: &str,
    consumer_group: &str,
    consumer_name: &str,
    ack_deadline_ms: i64,
    payload_key: &str,
    dlq_config: &Option<DeadLetterQueueConfig>,
) -> Result<()> {
    let mut conn = pool.get().await.map_err(QueueError::generic)?;

    // Every iteration checks whether the processing queue has items that should
    // be picked back up, claiming them in the process
    let StreamAutoclaimReply { ids } = conn
        .xautoclaim_options(
            main_queue_name,
            consumer_group,
            consumer_name,
            ack_deadline_ms,
            "-",
            StreamAutoClaimOptions::default().count(PENDING_BATCH_SIZE),
        )
        .await
        .map_err(QueueError::generic)?;

    if !ids.is_empty() {
        trace!("Moving {} unhandled messages back to the queue", ids.len());

        let mut pipe = redis::pipe();

        // And reinsert the map of KV pairs into the MAIN queue with a new stream ID
        for stream_id in &ids {
            let InternalPayloadOwned {
                payload,
                num_receives,
            } = internal_from_stream(stream_id, payload_key)?;

            if let Some(dlq_config) = &dlq_config {
                if num_receives >= dlq_config.max_receives {
                    trace!(
                        entry_id = stream_id.id,
                        "Maximum attempts reached for message, sending to DLQ",
                    );
                    send_to_dlq(
                        pool,
                        main_queue_name,
                        dlq_config,
                        &stream_id.id,
                        payload_key,
                    )
                    .await?;
                    continue;
                }
            }
            let _ = pipe.xadd(
                main_queue_name,
                GENERATE_STREAM_ID,
                internal_to_stream_payload!(
                    InternalPayload {
                        payload: payload.as_slice(),
                        num_receives
                    },
                    payload_key
                ),
            );
        }

        let _: () = pipe
            .query_async(&mut *conn)
            .await
            .map_err(QueueError::generic)?;

        // Acknowledge all the stale ones so the pending queue is cleared
        let ids: Vec<_> = ids.iter().map(|wrapped| &wrapped.id).collect();

        let mut pipe = redis::pipe();
        pipe.xack(main_queue_name, consumer_group, &ids);
        pipe.xdel(main_queue_name, &ids);

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
