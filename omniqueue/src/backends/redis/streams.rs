//! Implementation of the main queue using redis streams.

use std::time::Duration;

use bb8::ManageConnection;
use redis::{
    streams::{StreamClaimReply, StreamId, StreamReadOptions, StreamReadReply},
    AsyncCommands as _, FromRedisValue, RedisResult,
};
use tracing::{error, trace};

use super::{from_key, RedisConnection, RedisConsumer, RedisProducer};
use crate::{queue::Acker, Delivery, QueueError, Result};

/// Special ID for XADD command's which generates a stream ID automatically
const GENERATE_STREAM_ID: &str = "*";
/// Special ID for XREADGROUP commands which reads any new messages
const LISTEN_STREAM_ID: &str = ">";

/// The maximum number of pending messages to reinsert into the queue after
/// becoming stale per loop
// FIXME(onelson): expose in config?
const PENDING_BATCH_SIZE: i16 = 1000;

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
            &[(&producer.payload_key, payload)],
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

    wrap_entry(consumer, entry)
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
            let wrapped = wrap_entry(consumer, entry)?;
            out.push(wrapped);
        }
    }
    Ok(out)
}

fn wrap_entry<R: RedisConnection>(
    consumer: &RedisConsumer<R>,
    entry: StreamId,
) -> Result<Delivery> {
    let entry_id = entry.id.clone();
    let payload = entry
        .map
        .get(&consumer.payload_key)
        .ok_or(QueueError::NoData)?;
    let payload: Vec<u8> = redis::from_redis_value(payload).map_err(QueueError::generic)?;

    Ok(Delivery::new(
        payload,
        RedisStreamsAcker {
            redis: consumer.redis.clone(),
            queue_key: consumer.queue_key.to_owned(),
            consumer_group: consumer.consumer_group.to_owned(),
            entry_id,
            already_acked_or_nacked: false,
        },
    ))
}

struct RedisStreamsAcker<M: ManageConnection> {
    redis: bb8::Pool<M>,
    queue_key: String,
    consumer_group: String,
    entry_id: String,

    already_acked_or_nacked: bool,
}

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
    keys: &[Vec<u8>],
    main_queue_name: &str,
    payload_key: &str,
    conn: &mut impl redis::aio::ConnectionLike,
) -> Result<()> {
    let mut pipe = redis::pipe();
    for key in keys {
        let (_, payload) = from_key(key)?;
        let _ = pipe.xadd(
            main_queue_name,
            GENERATE_STREAM_ID,
            &[(payload_key, payload)],
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
) -> Result<()> {
    loop {
        if let Err(err) = reenqueue_timed_out_messages(
            &pool,
            &queue_key,
            &consumer_group,
            &consumer_name,
            ack_deadline_ms,
        )
        .await
        {
            error!("{err}");
            tokio::time::sleep(Duration::from_millis(500)).await;
            continue;
        }
    }
}

async fn reenqueue_timed_out_messages<R: RedisConnection>(
    pool: &bb8::Pool<R>,
    main_queue_name: &str,
    consumer_group: &str,
    consumer_name: &str,
    ack_deadline_ms: i64,
) -> Result<()> {
    let mut conn = pool.get().await.map_err(QueueError::generic)?;

    // Every iteration checks whether the processing queue has items that should
    // be picked back up, claiming them in the process
    let mut cmd = redis::cmd("XAUTOCLAIM");
    cmd.arg(main_queue_name)
        .arg(consumer_group)
        .arg(consumer_name)
        .arg(ack_deadline_ms)
        .arg("-")
        .arg("COUNT")
        .arg(PENDING_BATCH_SIZE);

    let StreamAutoclaimReply { ids } = cmd
        .query_async(&mut *conn)
        .await
        .map_err(QueueError::generic)?;

    if !ids.is_empty() {
        trace!("Moving {} unhandled messages back to the queue", ids.len());

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
