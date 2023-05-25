use std::{any::TypeId, collections::HashMap};

use async_trait::async_trait;
use serde::Serialize;
use tokio::sync::broadcast;

use crate::{
    decoding::DecoderRegistry,
    encoding::{CustomEncoder, EncoderRegistry},
    queue::{consumer::QueueConsumer, producer::QueueProducer, Acker, Delivery, QueueBackend},
    QueueError,
};

pub struct MemoryQueueBackend;

#[async_trait]
impl QueueBackend for MemoryQueueBackend {
    type Config = usize;

    type PayloadIn = Vec<u8>;
    type PayloadOut = Vec<u8>;

    type Producer = MemoryQueueProducer;
    type Consumer = MemoryQueueConsumer;

    async fn new_pair(
        config: usize,
        custom_encoders: EncoderRegistry<Vec<u8>>,
        custom_decoders: DecoderRegistry<Vec<u8>>,
    ) -> Result<(MemoryQueueProducer, MemoryQueueConsumer), QueueError> {
        let (tx, rx) = broadcast::channel(config);

        Ok((
            MemoryQueueProducer {
                registry: custom_encoders,
                tx: tx.clone(),
            },
            MemoryQueueConsumer {
                registry: custom_decoders,
                tx,
                rx,
            },
        ))
    }

    async fn producing_half(
        _config: usize,
        _custom_encoders: EncoderRegistry<Vec<u8>>,
    ) -> Result<MemoryQueueProducer, QueueError> {
        Err(QueueError::CannotCreateHalf)
    }

    async fn consuming_half(
        _config: usize,
        _custom_decoders: DecoderRegistry<Vec<u8>>,
    ) -> Result<MemoryQueueConsumer, QueueError> {
        Err(QueueError::CannotCreateHalf)
    }
}

pub struct MemoryQueueProducer {
    registry: EncoderRegistry<Vec<u8>>,
    tx: broadcast::Sender<Vec<u8>>,
}

#[async_trait]
impl QueueProducer for MemoryQueueProducer {
    type Payload = Vec<u8>;

    fn get_custom_encoders(&self) -> &HashMap<TypeId, Box<dyn CustomEncoder<Self::Payload>>> {
        self.registry.as_ref()
    }

    async fn send_raw(&self, payload: &Vec<u8>) -> Result<(), QueueError> {
        self.tx
            .send(payload.clone())
            .map(|_| ())
            .map_err(QueueError::generic)
    }

    async fn send_serde_json<P: Serialize + Sync>(&self, payload: &P) -> Result<(), QueueError> {
        let payload = serde_json::to_vec(payload)?;
        self.send_raw(&payload).await
    }
}

pub struct MemoryQueueConsumer {
    registry: DecoderRegistry<Vec<u8>>,
    rx: broadcast::Receiver<Vec<u8>>,
    tx: broadcast::Sender<Vec<u8>>,
}

#[async_trait]
impl QueueConsumer for MemoryQueueConsumer {
    type Payload = Vec<u8>;

    async fn receive(&mut self) -> Result<Delivery, QueueError> {
        let payload = self.rx.recv().await.map_err(QueueError::generic)?;

        Ok(Delivery {
            payload: Some(payload.clone()),
            decoders: self.registry.clone(),
            acker: Box::new(MemoryQueueAcker {
                tx: self.tx.clone(),
                payload_copy: Some(payload),
                alredy_acked_or_nacked: false,
            }),
        })
    }
}

pub struct MemoryQueueAcker {
    tx: broadcast::Sender<Vec<u8>>,
    payload_copy: Option<Vec<u8>>,
    alredy_acked_or_nacked: bool,
}

#[async_trait]
impl Acker for MemoryQueueAcker {
    async fn ack(&mut self) -> Result<(), QueueError> {
        if self.alredy_acked_or_nacked {
            Err(QueueError::CannotAckOrNackTwice)
        } else {
            self.alredy_acked_or_nacked = true;
            Ok(())
        }
    }

    async fn nack(&mut self) -> Result<(), QueueError> {
        if self.alredy_acked_or_nacked {
            Err(QueueError::CannotAckOrNackTwice)
        } else {
            self.alredy_acked_or_nacked = true;
            self.tx
                .send(
                    self.payload_copy
                        .take()
                        .ok_or(QueueError::CannotAckOrNackTwice)?,
                )
                .map(|_| ())
                .map_err(QueueError::generic)
        }
    }
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use crate::{
        queue::{consumer::QueueConsumer, producer::QueueProducer, QueueBuilder},
        QueueError,
    };

    use super::MemoryQueueBackend;

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
        let (p, mut c) = QueueBuilder::<MemoryQueueBackend, _>::new(16)
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
        let (p, mut c) = QueueBuilder::<MemoryQueueBackend, _>::new(16)
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
}
