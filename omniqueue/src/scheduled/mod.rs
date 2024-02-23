use std::{future::Future, pin::Pin, time::Duration};

use serde::Serialize;

use crate::{queue::ErasedQueueProducer, QueuePayload, QueueProducer, Result};

pub trait ScheduledQueueProducer: QueueProducer {
    fn send_raw_scheduled(
        &self,
        payload: &Self::Payload,
        delay: Duration,
    ) -> impl Future<Output = Result<()>> + Send;

    fn send_bytes_scheduled(
        &self,
        payload: &[u8],
        delay: Duration,
    ) -> impl Future<Output = Result<()>> + Send {
        async move {
            let payload = Self::Payload::from_bytes_naive(payload)?;
            self.send_raw_scheduled(&payload, delay).await
        }
    }

    fn send_serde_json_scheduled<P: Serialize + Sync>(
        &self,
        payload: &P,
        delay: Duration,
    ) -> impl Future<Output = Result<()>> + Send {
        async move {
            let payload = serde_json::to_vec(payload)?;
            self.send_bytes_scheduled(&payload, delay).await
        }
    }

    fn into_dyn_scheduled(self) -> DynScheduledQueueProducer
    where
        Self: 'static,
    {
        DynScheduledQueueProducer::new(self)
    }
}

pub struct DynScheduledQueueProducer(Box<dyn ErasedScheduledQueueProducer>);

impl DynScheduledQueueProducer {
    fn new(inner: impl ScheduledQueueProducer + 'static) -> Self {
        let dyn_inner = DynScheduledProducerInner { inner };
        Self(Box::new(dyn_inner))
    }
}

trait ErasedScheduledQueueProducer: ErasedQueueProducer {
    fn send_raw_scheduled<'a>(
        &'a self,
        payload: &'a [u8],
        delay: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;
}

struct DynScheduledProducerInner<P> {
    inner: P,
}

impl<P: ScheduledQueueProducer> ErasedQueueProducer for DynScheduledProducerInner<P> {
    fn send_raw<'a>(
        &'a self,
        payload: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move { self.inner.send_bytes(payload).await })
    }
}

impl<P: ScheduledQueueProducer> ErasedScheduledQueueProducer for DynScheduledProducerInner<P> {
    fn send_raw_scheduled<'a>(
        &'a self,
        payload: &'a [u8],
        delay: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move { self.inner.send_bytes_scheduled(payload, delay).await })
    }
}

impl DynScheduledQueueProducer {
    pub async fn send_raw(&self, payload: &[u8]) -> Result<()> {
        self.0.send_raw(payload).await
    }

    pub async fn send_serde_json<P: Serialize + Sync>(&self, payload: &P) -> Result<()> {
        let payload = serde_json::to_vec(payload)?;
        self.send_raw(&payload).await
    }

    pub async fn send_raw_scheduled(&self, payload: &[u8], delay: Duration) -> Result<()> {
        self.0.send_raw_scheduled(payload, delay).await
    }

    pub async fn send_serde_json_scheduled<P: Serialize + Sync>(
        &self,
        payload: &P,
        delay: Duration,
    ) -> Result<()> {
        let payload = serde_json::to_vec(payload)?;
        self.0.send_raw_scheduled(&payload, delay).await
    }
}

impl_queue_producer!(DynScheduledQueueProducer, Vec<u8>);
impl_scheduled_queue_producer!(DynScheduledQueueProducer, Vec<u8>);
