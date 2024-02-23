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
    ) -> impl Future<Output = Result<()>> + Send
    where
        Self: Sized,
    {
        async move {
            let payload = serde_json::to_vec(payload)?;
            self.send_bytes_scheduled(&payload, delay).await
        }
    }

    fn into_dyn_scheduled(self) -> DynScheduledQueueProducer
    where
        Self: Sized + 'static,
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
    #[allow(clippy::ptr_arg)] // for now
    fn send_raw_scheduled<'a>(
        &'a self,
        payload: &'a Vec<u8>,
        delay: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;
}

struct DynScheduledProducerInner<P> {
    inner: P,
}

impl<P: ScheduledQueueProducer> ErasedQueueProducer for DynScheduledProducerInner<P> {
    fn send_raw<'a>(
        &'a self,
        payload: &'a Vec<u8>,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move { self.inner.send_bytes(payload).await })
    }
}

impl<P: ScheduledQueueProducer> ErasedScheduledQueueProducer for DynScheduledProducerInner<P> {
    fn send_raw_scheduled<'a>(
        &'a self,
        payload: &'a Vec<u8>,
        delay: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move { self.inner.send_bytes_scheduled(payload, delay).await })
    }
}

impl QueueProducer for DynScheduledQueueProducer {
    type Payload = Vec<u8>;

    async fn send_raw(&self, payload: &Vec<u8>) -> Result<()> {
        self.0.send_raw(payload).await
    }
}

impl ScheduledQueueProducer for DynScheduledQueueProducer {
    async fn send_raw_scheduled(&self, payload: &Vec<u8>, delay: Duration) -> Result<()> {
        self.0.send_raw_scheduled(payload, delay).await
    }

    fn into_dyn_scheduled(self) -> DynScheduledQueueProducer {
        self
    }
}
