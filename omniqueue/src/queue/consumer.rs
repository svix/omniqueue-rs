use async_trait::async_trait;
use std::time::Duration;

use crate::{decoding::DecoderRegistry, QueueError, QueuePayload};

use super::Delivery;

#[async_trait]
pub trait QueueConsumer: Send + Sync {
    type Payload: QueuePayload;

    async fn receive(&mut self) -> Result<Delivery, QueueError>;

    async fn receive_all(
        &mut self,
        max_messages: usize,
        deadline: Duration,
    ) -> Result<Vec<Delivery>, QueueError>;

    fn into_dyn(self, custom_decoders: DecoderRegistry<Vec<u8>>) -> DynConsumer
    where
        Self: 'static + Sized,
    {
        let c = DynConsumerInner {
            inner: self,
            custom_decoders,
        };
        DynConsumer(Box::new(c))
    }
}

struct DynConsumerInner<T: QueuePayload, C: 'static + QueueConsumer<Payload = T>> {
    inner: C,
    custom_decoders: DecoderRegistry<Vec<u8>>,
}

#[async_trait]
impl<T: QueuePayload, C: 'static + QueueConsumer<Payload = T>> QueueConsumer
    for DynConsumerInner<T, C>
{
    type Payload = Vec<u8>;

    async fn receive(&mut self) -> Result<Delivery, QueueError> {
        let mut t_payload = self.inner.receive().await?;
        let bytes_payload: Option<Vec<u8>> = match t_payload.payload_custom() {
            Ok(b) => b,
            Err(QueueError::NoDecoderForThisType) => t_payload.take_payload(),
            Err(e) => return Err(e),
        };

        Ok(Delivery {
            payload: bytes_payload,
            decoders: self.custom_decoders.clone(),
            acker: t_payload.acker,
        })
    }

    async fn receive_all(
        &mut self,
        max_messages: usize,
        deadline: Duration,
    ) -> Result<Vec<Delivery>, QueueError> {
        let xs = self.inner.receive_all(max_messages, deadline).await?;
        let mut out = Vec::with_capacity(xs.len());
        for mut t_payload in xs {
            let bytes_payload: Option<Vec<u8>> = match t_payload.payload_custom() {
                Ok(b) => b,
                Err(QueueError::NoDecoderForThisType) => t_payload.take_payload(),
                Err(e) => return Err(e),
            };
            out.push(Delivery {
                payload: bytes_payload,
                decoders: self.custom_decoders.clone(),
                acker: t_payload.acker,
            });
        }
        Ok(out)
    }

    fn into_dyn(mut self, custom_decoders: DecoderRegistry<Vec<u8>>) -> DynConsumer
    where
        Self: Sized,
    {
        self.custom_decoders = custom_decoders;
        DynConsumer(Box::new(self))
    }
}

pub struct DynConsumer(Box<dyn QueueConsumer<Payload = Vec<u8>>>);

#[async_trait]
impl QueueConsumer for DynConsumer {
    type Payload = Vec<u8>;

    async fn receive(&mut self) -> Result<Delivery, QueueError> {
        self.0.receive().await
    }

    async fn receive_all(
        &mut self,
        max_messages: usize,
        deadline: Duration,
    ) -> Result<Vec<Delivery>, QueueError> {
        self.0.receive_all(max_messages, deadline).await
    }

    fn into_dyn(self, _custom_decoders: DecoderRegistry<Vec<u8>>) -> DynConsumer
    where
        Self: Sized,
    {
        self
    }
}
