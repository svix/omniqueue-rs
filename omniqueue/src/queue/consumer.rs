use std::{future::Future, pin::Pin, time::Duration};

use crate::{decoding::DecoderRegistry, QueueError, QueuePayload};

use super::Delivery;

pub trait QueueConsumer: Send {
    type Payload: QueuePayload;

    fn receive(&mut self) -> impl Future<Output = Result<Delivery, QueueError>> + Send;

    fn receive_all(
        &mut self,
        max_messages: usize,
        deadline: Duration,
    ) -> impl Future<Output = Result<Vec<Delivery>, QueueError>> + Send;

    fn into_dyn(self, custom_decoders: DecoderRegistry<Vec<u8>>) -> DynConsumer
    where
        Self: Sized + 'static,
    {
        DynConsumer::new(self, custom_decoders)
    }
}

pub struct DynConsumer(Box<dyn ErasedQueueConsumer>);

impl DynConsumer {
    fn new(inner: impl QueueConsumer + 'static, custom_decoders: DecoderRegistry<Vec<u8>>) -> Self {
        let c = DynConsumerInner {
            inner,
            custom_decoders,
        };
        Self(Box::new(c))
    }
}

trait ErasedQueueConsumer: Send {
    fn receive(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = Result<Delivery, QueueError>> + Send + '_>>;
    fn receive_all(
        &mut self,
        max_messages: usize,
        deadline: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Delivery>, QueueError>> + Send + '_>>;
}

struct DynConsumerInner<C> {
    inner: C,
    custom_decoders: DecoderRegistry<Vec<u8>>,
}

impl<C: QueueConsumer> ErasedQueueConsumer for DynConsumerInner<C> {
    fn receive(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = Result<Delivery, QueueError>> + Send + '_>> {
        Box::pin(async move {
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
        })
    }

    fn receive_all(
        &mut self,
        max_messages: usize,
        deadline: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Delivery>, QueueError>> + Send + '_>> {
        Box::pin(async move {
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
        })
    }
}

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

    fn into_dyn(self, _custom_decoders: DecoderRegistry<Vec<u8>>) -> DynConsumer {
        self
    }
}
