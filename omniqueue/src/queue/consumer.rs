use std::{future::Future, pin::Pin, time::Duration};

use crate::{decoding::DecoderRegistry, QueueError, Result};

use super::Delivery;

pub trait QueueConsumer: Send + Sized {
    fn receive(&mut self) -> impl Future<Output = Result<Delivery>> + Send;

    fn receive_all(
        &mut self,
        max_messages: usize,
        deadline: Duration,
    ) -> impl Future<Output = Result<Vec<Delivery>>> + Send;

    fn into_dyn(self, custom_decoders: DecoderRegistry) -> DynConsumer
    where
        Self: 'static,
    {
        DynConsumer::new(self, custom_decoders)
    }
}

pub struct DynConsumer(Box<dyn ErasedQueueConsumer>);

impl DynConsumer {
    fn new(inner: impl QueueConsumer + 'static, custom_decoders: DecoderRegistry) -> Self {
        let c = DynConsumerInner {
            inner,
            custom_decoders,
        };
        Self(Box::new(c))
    }
}

trait ErasedQueueConsumer: Send {
    fn receive(&mut self) -> Pin<Box<dyn Future<Output = Result<Delivery>> + Send + '_>>;
    fn receive_all(
        &mut self,
        max_messages: usize,
        deadline: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Delivery>>> + Send + '_>>;
}

struct DynConsumerInner<C> {
    inner: C,
    custom_decoders: DecoderRegistry,
}

impl<C: QueueConsumer> ErasedQueueConsumer for DynConsumerInner<C> {
    fn receive(&mut self) -> Pin<Box<dyn Future<Output = Result<Delivery>> + Send + '_>> {
        Box::pin(async move {
            let mut delivery = self.inner.receive().await?;
            let payload: Option<String> = match delivery.payload_custom() {
                Ok(b) => b,
                Err(QueueError::NoDecoderForThisType) => delivery.take_payload(),
                Err(e) => return Err(e),
            };

            Ok(Delivery {
                payload,
                decoders: self.custom_decoders.clone(),
                acker: delivery.acker,
            })
        })
    }

    fn receive_all(
        &mut self,
        max_messages: usize,
        deadline: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Delivery>>> + Send + '_>> {
        Box::pin(async move {
            let xs = self.inner.receive_all(max_messages, deadline).await?;
            let mut out = Vec::with_capacity(xs.len());
            for mut t_payload in xs {
                let payload: Option<String> = match t_payload.payload_custom() {
                    Ok(b) => b,
                    Err(QueueError::NoDecoderForThisType) => t_payload.take_payload(),
                    Err(e) => return Err(e),
                };
                out.push(Delivery {
                    payload,
                    decoders: self.custom_decoders.clone(),
                    acker: t_payload.acker,
                });
            }
            Ok(out)
        })
    }
}

impl QueueConsumer for DynConsumer {
    async fn receive(&mut self) -> Result<Delivery> {
        self.0.receive().await
    }

    async fn receive_all(
        &mut self,
        max_messages: usize,
        deadline: Duration,
    ) -> Result<Vec<Delivery>> {
        self.0.receive_all(max_messages, deadline).await
    }

    fn into_dyn(self, _custom_decoders: DecoderRegistry) -> DynConsumer {
        self
    }
}
