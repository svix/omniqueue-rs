use std::{future::Future, pin::Pin};

use serde::Serialize;

use crate::{QueuePayload, Result};

pub trait QueueProducer: Send + Sync + Sized {
    type Payload: QueuePayload;

    fn send_raw(&self, payload: &Self::Payload) -> impl Future<Output = Result<()>> + Send;

    fn send_bytes(&self, payload: &[u8]) -> impl Future<Output = Result<()>> + Send {
        async move {
            let payload = Self::Payload::from_bytes_naive(payload)?;
            self.send_raw(&payload).await
        }
    }

    fn send_serde_json<P: Serialize + Sync>(
        &self,
        payload: &P,
    ) -> impl Future<Output = Result<()>> + Send {
        async move {
            let payload = serde_json::to_vec(payload)?;
            self.send_bytes(&payload).await
        }
    }

    fn into_dyn(self) -> DynProducer
    where
        Self: 'static,
    {
        DynProducer::new(self)
    }
}

pub struct DynProducer(Box<dyn ErasedQueueProducer>);

impl DynProducer {
    fn new(inner: impl QueueProducer + 'static) -> Self {
        let dyn_inner = DynProducerInner { inner };
        Self(Box::new(dyn_inner))
    }
}

pub(crate) trait ErasedQueueProducer: Send + Sync {
    fn send_raw<'a>(
        &'a self,
        payload: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;
}

struct DynProducerInner<P> {
    inner: P,
}

impl<P: QueueProducer> ErasedQueueProducer for DynProducerInner<P> {
    fn send_raw<'a>(
        &'a self,
        payload: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move { self.inner.send_bytes(payload).await })
    }
}

impl DynProducer {
    pub async fn send_raw(&self, payload: &[u8]) -> Result<()> {
        self.0.send_raw(payload).await
    }

    pub async fn send_serde_json<P: Serialize + Sync>(&self, payload: &P) -> Result<()> {
        let payload = serde_json::to_vec(payload)?;
        self.send_raw(&payload).await
    }
}

impl_queue_producer!(DynProducer, Vec<u8>);
