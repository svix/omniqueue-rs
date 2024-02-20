use std::time::{Duration, Instant};
use std::{any::TypeId, collections::HashMap};

use async_trait::async_trait;
use serde::Serialize;
use tokio::sync::mpsc;

use crate::{
    builder::{QueueBuilder, Static},
    decoding::DecoderRegistry,
    encoding::{CustomEncoder, EncoderRegistry},
    queue::{consumer::QueueConsumer, producer::QueueProducer, Acker, Delivery, QueueBackend},
    scheduled::ScheduledProducer,
    QueueError,
};

pub struct InMemoryBackend;

impl InMemoryBackend {
    /// Creates a new in-memory queue builder.
    pub fn builder() -> QueueBuilder<Self, Static> {
        QueueBuilder::new(())
    }
}

impl QueueBackend for InMemoryBackend {
    type PayloadIn = Vec<u8>;
    type PayloadOut = Vec<u8>;

    type Producer = InMemoryProducer;
    type Consumer = InMemoryConsumer;

    type Config = ();

    async fn new_pair(
        _config: (),
        custom_encoders: EncoderRegistry<Vec<u8>>,
        custom_decoders: DecoderRegistry<Vec<u8>>,
    ) -> Result<(InMemoryProducer, InMemoryConsumer), QueueError> {
        let (tx, rx) = mpsc::unbounded_channel();

        Ok((
            InMemoryProducer {
                registry: custom_encoders,
                tx: tx.clone(),
            },
            InMemoryConsumer {
                registry: custom_decoders,
                tx,
                rx,
            },
        ))
    }

    async fn producing_half(
        _config: (),
        _custom_encoders: EncoderRegistry<Vec<u8>>,
    ) -> Result<InMemoryProducer, QueueError> {
        Err(QueueError::CannotCreateHalf)
    }

    async fn consuming_half(
        _config: (),
        _custom_decoders: DecoderRegistry<Vec<u8>>,
    ) -> Result<InMemoryConsumer, QueueError> {
        Err(QueueError::CannotCreateHalf)
    }
}

pub struct InMemoryProducer {
    registry: EncoderRegistry<Vec<u8>>,
    tx: mpsc::UnboundedSender<Vec<u8>>,
}

impl QueueProducer for InMemoryProducer {
    type Payload = Vec<u8>;

    fn get_custom_encoders(&self) -> &HashMap<TypeId, Box<dyn CustomEncoder<Self::Payload>>> {
        self.registry.as_ref()
    }

    async fn send_raw(&self, payload: &Self::Payload) -> Result<(), QueueError> {
        self.tx.send(payload.clone()).map_err(QueueError::generic)
    }

    async fn send_serde_json<P: Serialize + Sync>(&self, payload: &P) -> Result<(), QueueError> {
        let payload = serde_json::to_vec(payload)?;
        self.send_raw(&payload).await
    }
}

impl ScheduledProducer for InMemoryProducer {
    async fn send_raw_scheduled(
        &self,
        payload: &Self::Payload,
        delay: Duration,
    ) -> Result<(), QueueError> {
        let tx = self.tx.clone();
        let payload = payload.clone();
        tokio::spawn(async move {
            tracing::trace!("MemoryQueue: event sent > (delay: {:?})", delay);
            tokio::time::sleep(delay).await;
            if tx.send(payload).is_err() {
                tracing::error!("Receiver dropped");
            }
        });
        Ok(())
    }
}

pub struct InMemoryConsumer {
    registry: DecoderRegistry<Vec<u8>>,
    rx: mpsc::UnboundedReceiver<Vec<u8>>,
    tx: mpsc::UnboundedSender<Vec<u8>>,
}

impl InMemoryConsumer {
    fn wrap_payload(&self, payload: Vec<u8>) -> Delivery {
        Delivery {
            payload: Some(payload.clone()),
            decoders: self.registry.clone(),
            acker: Box::new(InMemoryAcker {
                tx: self.tx.clone(),
                payload_copy: Some(payload),
                already_acked_or_nacked: false,
            }),
        }
    }
}

impl QueueConsumer for InMemoryConsumer {
    type Payload = Vec<u8>;

    async fn receive(&mut self) -> Result<Delivery, QueueError> {
        let payload = self
            .rx
            .recv()
            .await
            .ok_or_else(|| QueueError::Generic("recv failed".into()))?;
        Ok(self.wrap_payload(payload))
    }

    async fn receive_all(
        &mut self,
        max_messages: usize,
        deadline: Duration,
    ) -> Result<Vec<Delivery>, QueueError> {
        let mut out = Vec::with_capacity(max_messages);
        let start = Instant::now();
        match tokio::time::timeout(deadline, self.rx.recv()).await {
            Ok(Some(x)) => out.push(self.wrap_payload(x)),
            // Timeouts and stream termination
            Err(_) | Ok(None) => return Ok(out),
        }

        if max_messages > 1 {
            // `try_recv` will break the loop if no ready items are already buffered in the channel.
            // This should allow us to opportunistically fill up the buffer in the remaining time.
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

struct InMemoryAcker {
    tx: mpsc::UnboundedSender<Vec<u8>>,
    payload_copy: Option<Vec<u8>>,
    already_acked_or_nacked: bool,
}

#[async_trait]
impl Acker for InMemoryAcker {
    async fn ack(&mut self) -> Result<(), QueueError> {
        if self.already_acked_or_nacked {
            Err(QueueError::CannotAckOrNackTwice)
        } else {
            self.already_acked_or_nacked = true;
            Ok(())
        }
    }

    async fn nack(&mut self) -> Result<(), QueueError> {
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
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};
    use std::time::{Duration, Instant};

    use crate::{
        queue::{consumer::QueueConsumer, producer::QueueProducer},
        scheduled::ScheduledProducer,
        QueueBuilder, QueueError,
    };

    use super::InMemoryBackend;

    #[derive(Clone, Copy, Debug, Eq, Deserialize, PartialEq, Serialize)]
    struct TypeA {
        a: i32,
    }

    fn type_a_to_json(a: &TypeA) -> Result<Vec<u8>, QueueError> {
        Ok(serde_json::to_vec(a)?)
    }

    /// Unfortunately type restrictions require this for now
    #[allow(clippy::ptr_arg)]
    fn json_to_type_a(json: &Vec<u8>) -> Result<TypeA, QueueError> {
        Ok(serde_json::from_slice(json)?)
    }

    #[tokio::test]
    async fn simple_queue_test() {
        let (p, mut c) = QueueBuilder::<InMemoryBackend, _>::new(())
            .with_encoder(type_a_to_json)
            .with_decoder(json_to_type_a)
            .build_pair()
            .await
            .unwrap();

        p.send_custom(&TypeA { a: 12 }).await.unwrap();
        assert_eq!(
            c.receive()
                .await
                .unwrap()
                .payload_custom::<TypeA>()
                .unwrap()
                .unwrap(),
            TypeA { a: 12 }
        );

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

    #[tokio::test]
    async fn dynamic_queue_test() {
        let (p, mut c) = QueueBuilder::<InMemoryBackend, _>::new(())
            .make_dynamic()
            .with_bytes_encoder(|a: &TypeA| Ok(serde_json::to_vec(a)?))
            .with_bytes_decoder(|b: &Vec<u8>| -> Result<TypeA, QueueError> {
                Ok(serde_json::from_slice(b)?)
            })
            .build_pair()
            .await
            .unwrap();

        p.send_custom(&TypeA { a: 12 }).await.unwrap();
        assert_eq!(
            c.receive()
                .await
                .unwrap()
                .payload_custom::<TypeA>()
                .unwrap()
                .unwrap(),
            TypeA { a: 12 }
        );
    }

    #[derive(Debug, Deserialize, Serialize, PartialEq)]
    struct ExType {
        a: u8,
    }

    /// Consumer will return immediately if there are fewer than max messages to start with.
    #[tokio::test]
    async fn test_send_recv_all_partial() {
        let payload = ExType { a: 2 };

        let (p, mut c) = QueueBuilder::<InMemoryBackend, _>::new(())
            .build_pair()
            .await
            .unwrap();

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

    /// Consumer should yield items immediately if there's a full batch ready on the first poll.
    #[tokio::test]
    async fn test_send_recv_all_full() {
        let payload1 = ExType { a: 1 };
        let payload2 = ExType { a: 2 };

        let (p, mut c) = QueueBuilder::<InMemoryBackend, _>::new(())
            .build_pair()
            .await
            .unwrap();

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
        // N.b. it's still possible this could turn up false if the test runs too slow.
        assert!(now.elapsed() < deadline);
    }

    /// Consumer will return the full batch immediately, but also return immediately if a partial batch is ready.
    #[tokio::test]
    async fn test_send_recv_all_full_then_partial() {
        let payload1 = ExType { a: 1 };
        let payload2 = ExType { a: 2 };
        let payload3 = ExType { a: 3 };

        let (p, mut c) = QueueBuilder::<InMemoryBackend, _>::new(())
            .build_pair()
            .await
            .unwrap();

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
        let (_p, mut c) = QueueBuilder::<InMemoryBackend, _>::new(())
            .build_pair()
            .await
            .unwrap();

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

        let (p, mut c) = QueueBuilder::<InMemoryBackend, _>::new(())
            .build_pair()
            .await
            .unwrap();

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
