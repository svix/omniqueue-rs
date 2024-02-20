use std::{
    any::{Any, TypeId},
    collections::HashMap,
    future::Future,
    pin::Pin,
    time::Duration,
};

use serde::Serialize;

use crate::{
    encoding::{CustomEncoder, EncoderRegistry},
    queue::ErasedQueueProducer,
    QueueError, QueuePayload, QueueProducer, Result,
};

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

    fn send_custom_scheduled<P: 'static + Send + Sync>(
        &self,
        payload: &P,
        delay: Duration,
    ) -> impl Future<Output = Result<()>> + Send
    where
        Self: Sized,
    {
        async move {
            let encoder = self
                .get_custom_encoders()
                .get(&TypeId::of::<P>())
                .ok_or(QueueError::NoEncoderForThisType)?;

            let payload = encoder.encode(payload)?;
            self.send_raw_scheduled(&payload, delay).await
        }
    }

    fn into_dyn_scheduled(
        self,
        custom_encoders: EncoderRegistry<Vec<u8>>,
    ) -> DynScheduledQueueProducer
    where
        Self: Sized + 'static,
    {
        DynScheduledQueueProducer::new(self, custom_encoders)
    }
}

pub struct DynScheduledQueueProducer(Box<dyn ErasedScheduledQueueProducer>);

impl DynScheduledQueueProducer {
    fn new(
        inner: impl ScheduledQueueProducer + 'static,
        custom_encoders: EncoderRegistry<Vec<u8>>,
    ) -> Self {
        let dyn_inner = DynScheduledProducerInner {
            inner,
            custom_encoders,
        };
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
    custom_encoders: EncoderRegistry<Vec<u8>>,
}

impl<P: ScheduledQueueProducer> ErasedQueueProducer for DynScheduledProducerInner<P> {
    fn send_raw<'a>(
        &'a self,
        payload: &'a Vec<u8>,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            // Prioritize a custom encoder that takes a &[u8].
            if let Some(encoder) = self
                .inner
                .get_custom_encoders()
                .get(&TypeId::of::<Vec<u8>>())
            {
                let payload = encoder.encode(payload as &(dyn Any + Send + Sync))?;
                self.inner.send_raw(&payload).await
            } else {
                self.inner.send_bytes(payload).await
            }
        })
    }

    fn get_custom_encoders(&self) -> &HashMap<TypeId, Box<dyn CustomEncoder<Vec<u8>>>> {
        self.custom_encoders.as_ref()
    }
}

impl<P: ScheduledQueueProducer> ErasedScheduledQueueProducer for DynScheduledProducerInner<P> {
    fn send_raw_scheduled<'a>(
        &'a self,
        payload: &'a Vec<u8>,
        delay: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            if let Some(encoder) = self
                .inner
                .get_custom_encoders()
                .get(&TypeId::of::<Vec<u8>>())
            {
                let payload = encoder.encode(payload as &(dyn Any + Send + Sync))?;
                self.inner.send_raw(&payload).await
            } else {
                self.inner.send_bytes_scheduled(payload, delay).await
            }
        })
    }
}

impl QueueProducer for DynScheduledQueueProducer {
    type Payload = Vec<u8>;

    async fn send_raw(&self, payload: &Vec<u8>) -> Result<()> {
        self.0.send_raw(payload).await
    }

    fn get_custom_encoders(&self) -> &HashMap<TypeId, Box<dyn CustomEncoder<Self::Payload>>> {
        self.0.get_custom_encoders()
    }
}

impl ScheduledQueueProducer for DynScheduledQueueProducer {
    async fn send_raw_scheduled(&self, payload: &Vec<u8>, delay: Duration) -> Result<()> {
        self.0.send_raw_scheduled(payload, delay).await
    }

    fn into_dyn_scheduled(
        self,
        _custom_encoders: EncoderRegistry<Vec<u8>>,
    ) -> DynScheduledQueueProducer {
        self
    }
}
