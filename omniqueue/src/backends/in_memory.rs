use std::time::{Duration, Instant};

use serde::Serialize;
use tokio::sync::mpsc;

#[allow(deprecated)]
use crate::{
    builder::{QueueBuilder, Static},
    queue::{Acker, Delivery, QueueBackend},
    QueueError, Result,
};

pub struct InMemoryBackend;

impl InMemoryBackend {
    /// Creates a new in-memory queue builder.
    pub fn builder() -> QueueBuilder<Self, Static> {
        #[allow(deprecated)]
        QueueBuilder::new(())
    }
}

#[allow(deprecated)]
impl QueueBackend for InMemoryBackend {
    type PayloadIn = Vec<u8>;
    type PayloadOut = Vec<u8>;

    type Producer = InMemoryProducer;
    type Consumer = InMemoryConsumer;

    type Config = ();

    async fn new_pair(_config: ()) -> Result<(InMemoryProducer, InMemoryConsumer)> {
        let (tx, rx) = mpsc::unbounded_channel();

        Ok((
            InMemoryProducer { tx: tx.clone() },
            InMemoryConsumer { tx, rx },
        ))
    }

    async fn producing_half(_config: ()) -> Result<InMemoryProducer> {
        Err(QueueError::CannotCreateHalf)
    }

    async fn consuming_half(_config: ()) -> Result<InMemoryConsumer> {
        Err(QueueError::CannotCreateHalf)
    }
}

pub struct InMemoryProducer {
    tx: mpsc::UnboundedSender<Vec<u8>>,
}

impl InMemoryProducer {
    pub async fn send_raw(&self, payload: &[u8]) -> Result<()> {
        self.tx.send(payload.to_vec()).map_err(QueueError::generic)
    }

    pub async fn send_serde_json<P: Serialize + Sync>(&self, payload: &P) -> Result<()> {
        let payload = serde_json::to_vec(payload)?;
        self.send_raw(&payload).await
    }

    pub async fn send_raw_scheduled(&self, payload: &[u8], delay: Duration) -> Result<()> {
        let tx = self.tx.clone();
        let payload = payload.to_vec();
        tokio::spawn(async move {
            tracing::trace!("MemoryQueue: event sent > (delay: {:?})", delay);
            tokio::time::sleep(delay).await;
            if tx.send(payload).is_err() {
                tracing::error!("Receiver dropped");
            }
        });
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

impl crate::QueueProducer for InMemoryProducer {
    type Payload = Vec<u8>;
    omni_delegate!(send_raw, send_serde_json);
}
impl crate::ScheduledQueueProducer for InMemoryProducer {
    omni_delegate!(send_raw_scheduled, send_serde_json_scheduled);
}

pub struct InMemoryConsumer {
    rx: mpsc::UnboundedReceiver<Vec<u8>>,
    tx: mpsc::UnboundedSender<Vec<u8>>,
}

impl InMemoryConsumer {
    fn wrap_payload(&self, payload: Vec<u8>) -> Delivery {
        Delivery::new(
            payload.clone(),
            InMemoryAcker {
                tx: self.tx.clone(),
                payload_copy: Some(payload),
                already_acked_or_nacked: false,
            },
        )
    }

    pub async fn receive(&mut self) -> Result<Delivery> {
        let payload = self
            .rx
            .recv()
            .await
            .ok_or_else(|| QueueError::Generic("recv failed".into()))?;
        Ok(self.wrap_payload(payload))
    }

    pub async fn receive_all(
        &mut self,
        max_messages: usize,
        deadline: Duration,
    ) -> Result<Vec<Delivery>> {
        let mut out = Vec::with_capacity(max_messages);
        let start = Instant::now();
        match tokio::time::timeout(deadline, self.rx.recv()).await {
            Ok(Some(x)) => out.push(self.wrap_payload(x)),
            // Timeouts and stream termination
            Err(_) | Ok(None) => return Ok(out),
        }

        if max_messages > 1 {
            // `try_recv` will break the loop if no ready items are already
            // buffered in the channel. This should allow us to
            // opportunistically fill up the buffer in the remaining time.
            while let Ok(x) = self.rx.try_recv() {
                out.push(self.wrap_payload(x));
                if out.len() >= max_messages || start.elapsed() >= deadline {
                    break;
                }
            }
        }
        Ok(out)
    }
}

impl crate::QueueConsumer for InMemoryConsumer {
    type Payload = Vec<u8>;
    omni_delegate!(receive, receive_all);
}

struct InMemoryAcker {
    tx: mpsc::UnboundedSender<Vec<u8>>,
    payload_copy: Option<Vec<u8>>,
    already_acked_or_nacked: bool,
}

impl Acker for InMemoryAcker {
    async fn ack(&mut self) -> Result<()> {
        if self.already_acked_or_nacked {
            Err(QueueError::CannotAckOrNackTwice)
        } else {
            self.already_acked_or_nacked = true;
            Ok(())
        }
    }

    async fn nack(&mut self) -> Result<()> {
        if self.already_acked_or_nacked {
            Err(QueueError::CannotAckOrNackTwice)
        } else {
            self.already_acked_or_nacked = true;
            self.tx
                .send(
                    self.payload_copy
                        .take()
                        .ok_or(QueueError::CannotAckOrNackTwice)?,
                )
                .map_err(QueueError::generic)
        }
    }

    async fn set_ack_deadline(&mut self, _duration: Duration) -> Result<()> {
        Err(QueueError::Unsupported(
            "set_ack_deadline is not yet supported by InMemoryBackend",
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use serde::{Deserialize, Serialize};

    use super::InMemoryBackend;
    use crate::QueueProducer;

    #[derive(Clone, Copy, Debug, Eq, Deserialize, PartialEq, Serialize)]
    struct TypeA {
        a: i32,
    }

    #[tokio::test]
    async fn simple_queue_test() {
        let (p, mut c) = InMemoryBackend::builder().build_pair().await.unwrap();

        p.send_serde_json(&TypeA { a: 13 }).await.unwrap();
        assert_eq!(
            c.receive()
                .await
                .unwrap()
                .payload_serde_json::<TypeA>()
                .unwrap()
                .unwrap(),
            TypeA { a: 13 },
        );

        p.send_bytes(&serde_json::to_vec(&TypeA { a: 14 }).unwrap())
            .await
            .unwrap();
        assert_eq!(
            serde_json::from_slice::<TypeA>(c.receive().await.unwrap().borrow_payload().unwrap())
                .unwrap(),
            TypeA { a: 14 },
        );
    }

    #[derive(Debug, Deserialize, Serialize, PartialEq)]
    struct ExType {
        a: u8,
    }

    /// Consumer will return immediately if there are fewer than max messages to
    /// start with.
    #[tokio::test]
    async fn test_send_recv_all_partial() {
        let payload = ExType { a: 2 };

        let (p, mut c) = InMemoryBackend::builder().build_pair().await.unwrap();

        p.send_serde_json(&payload).await.unwrap();
        let deadline = Duration::from_secs(1);

        let now = Instant::now();
        let mut xs = c.receive_all(2, deadline).await.unwrap();
        assert_eq!(xs.len(), 1);
        let d = xs.remove(0);
        assert_eq!(d.payload_serde_json::<ExType>().unwrap().unwrap(), payload);
        d.ack().await.unwrap();
        assert!(now.elapsed() <= deadline);
    }

    /// Consumer should yield items immediately if there's a full batch ready on
    /// the first poll.
    #[tokio::test]
    async fn test_send_recv_all_full() {
        let payload1 = ExType { a: 1 };
        let payload2 = ExType { a: 2 };

        let (p, mut c) = InMemoryBackend::builder().build_pair().await.unwrap();

        p.send_serde_json(&payload1).await.unwrap();
        p.send_serde_json(&payload2).await.unwrap();
        let deadline = Duration::from_secs(1);

        let now = Instant::now();
        let mut xs = c.receive_all(2, deadline).await.unwrap();
        assert_eq!(xs.len(), 2);
        let d1 = xs.remove(0);
        assert_eq!(
            d1.payload_serde_json::<ExType>().unwrap().unwrap(),
            payload1
        );
        d1.ack().await.unwrap();

        let d2 = xs.remove(0);
        assert_eq!(
            d2.payload_serde_json::<ExType>().unwrap().unwrap(),
            payload2
        );
        d2.ack().await.unwrap();
        // N.b. it's still possible this could turn up false if the test runs
        // too slow.
        assert!(now.elapsed() < deadline);
    }

    /// Consumer will return the full batch immediately, but also return
    /// immediately if a partial batch is ready.
    #[tokio::test]
    async fn test_send_recv_all_full_then_partial() {
        let payload1 = ExType { a: 1 };
        let payload2 = ExType { a: 2 };
        let payload3 = ExType { a: 3 };

        let (p, mut c) = InMemoryBackend::builder().build_pair().await.unwrap();

        p.send_serde_json(&payload1).await.unwrap();
        p.send_serde_json(&payload2).await.unwrap();
        p.send_serde_json(&payload3).await.unwrap();

        let deadline = Duration::from_secs(1);
        let now1 = Instant::now();
        let mut xs = c.receive_all(2, deadline).await.unwrap();
        assert_eq!(xs.len(), 2);
        let d1 = xs.remove(0);
        assert_eq!(
            d1.payload_serde_json::<ExType>().unwrap().unwrap(),
            payload1
        );
        d1.ack().await.unwrap();

        let d2 = xs.remove(0);
        assert_eq!(
            d2.payload_serde_json::<ExType>().unwrap().unwrap(),
            payload2
        );
        d2.ack().await.unwrap();
        assert!(now1.elapsed() < deadline);

        // 2nd call
        let now2 = Instant::now();
        let mut ys = c.receive_all(2, deadline).await.unwrap();
        assert_eq!(ys.len(), 1);
        let d3 = ys.remove(0);
        assert_eq!(
            d3.payload_serde_json::<ExType>().unwrap().unwrap(),
            payload3
        );
        d3.ack().await.unwrap();
        assert!(now2.elapsed() <= deadline);
    }

    /// Consumer will NOT wait indefinitely for at least one item.
    #[tokio::test]
    async fn test_send_recv_all_late_arriving_items() {
        let (_p, mut c) = InMemoryBackend::builder().build_pair().await.unwrap();

        let deadline = Duration::from_secs(1);
        let now = Instant::now();
        let xs = c.receive_all(2, deadline).await.unwrap();
        let elapsed = now.elapsed();

        assert_eq!(xs.len(), 0);
        // Elapsed should be around the deadline, ballpark
        assert!(elapsed >= deadline);
        assert!(elapsed <= deadline + Duration::from_millis(200));
    }

    #[tokio::test]
    async fn test_scheduled() {
        let payload1 = ExType { a: 1 };

        let (p, mut c) = InMemoryBackend::builder().build_pair().await.unwrap();

        let delay = Duration::from_millis(100);
        let now = Instant::now();
        p.send_serde_json_scheduled(&payload1, delay).await.unwrap();
        let delivery = c
            .receive_all(1, delay * 2)
            .await
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        assert!(now.elapsed() >= delay);
        assert!(now.elapsed() < delay * 2);
        assert_eq!(Some(payload1), delivery.payload_serde_json().unwrap());
    }
}
