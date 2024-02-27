use std::{future::Future, pin::Pin, time::Duration};

use crate::{QueuePayload, Result};

use super::Delivery;

pub trait QueueConsumer: Send + Sized {
    type Payload: QueuePayload;

    fn receive(&mut self) -> impl Future<Output = Result<Delivery>> + Send;

    fn receive_all(
        &mut self,
        max_messages: usize,
        deadline: Duration,
    ) -> impl Future<Output = Result<Vec<Delivery>>> + Send;

    fn into_dyn(self) -> DynConsumer
    where
        Self: 'static,
    {
        DynConsumer::new(self)
    }
}

pub struct DynConsumer(Box<dyn ErasedQueueConsumer>);

impl DynConsumer {
    fn new(inner: impl QueueConsumer + 'static) -> Self {
        let c = DynConsumerInner { inner };
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
}

impl<C: QueueConsumer> ErasedQueueConsumer for DynConsumerInner<C> {
    fn receive(&mut self) -> Pin<Box<dyn Future<Output = Result<Delivery>> + Send + '_>> {
        Box::pin(async move {
            let mut t_payload = self.inner.receive().await?;
            Ok(Delivery {
                payload: t_payload.take_payload(),
                acker: t_payload.acker,
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
                out.push(Delivery {
                    payload: t_payload.take_payload(),
                    acker: t_payload.acker,
                });
            }
            Ok(out)
        })
    }
}

impl DynConsumer {
    pub async fn receive(&mut self) -> Result<Delivery> {
        self.0.receive().await
    }

    pub async fn receive_all(
        &mut self,
        max_messages: usize,
        deadline: Duration,
    ) -> Result<Vec<Delivery>> {
        self.0.receive_all(max_messages, deadline).await
    }
}

impl_queue_consumer!(DynConsumer, Vec<u8>);
