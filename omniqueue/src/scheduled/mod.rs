use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

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

    fn into_dyn_scheduled<'a>(self) -> DynScheduledProducer<'a>
    where
        Self: 'a,
    {
        DynScheduledProducer::new(self)
    }
}

macro_rules! ref_delegate {
    ($ty_param:ident, $ty:ty) => {
        #[deny(unconditional_recursion)]
        impl<$ty_param> ScheduledQueueProducer for $ty
        where
            $ty_param: ScheduledQueueProducer,
        {
            fn send_raw_scheduled(
                &self,
                payload: &Self::Payload,
                delay: Duration,
            ) -> impl Future<Output = Result<()>> + Send {
                (**self).send_raw_scheduled(payload, delay)
            }

            fn send_bytes_scheduled(
                &self,
                payload: &[u8],
                delay: Duration,
            ) -> impl Future<Output = Result<()>> + Send {
                (**self).send_bytes_scheduled(payload, delay)
            }

            fn send_serde_json_scheduled<P: Serialize + Sync>(
                &self,
                payload: &P,
                delay: Duration,
            ) -> impl Future<Output = Result<()>> + Send {
                (**self).send_serde_json_scheduled(payload, delay)
            }
        }
    };
}

ref_delegate!(T, &T);
ref_delegate!(T, Box<T>);
ref_delegate!(T, Arc<T>);

pub struct DynScheduledProducer<'a>(Box<dyn ErasedScheduledQueueProducer + 'a>);

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
    fn redrive_dlq<'a>(&'a self) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move { self.inner.redrive_dlq().await })
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

impl<'a> DynScheduledProducer<'a> {
    fn new(inner: impl ScheduledQueueProducer + 'a) -> Self {
        let dyn_inner = DynScheduledProducerInner { inner };
        Self(Box::new(dyn_inner))
    }

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

    pub async fn redrive_dlq(&self) -> Result<()> {
        self.0.redrive_dlq().await
    }
}

impl<'a> crate::QueueProducer for DynScheduledProducer<'a> {
    type Payload = Vec<u8>;
    omni_delegate!(send_raw, send_serde_json, redrive_dlq);
}
impl<'a> crate::ScheduledQueueProducer for DynScheduledProducer<'a> {
    omni_delegate!(send_raw_scheduled, send_serde_json_scheduled);
}
