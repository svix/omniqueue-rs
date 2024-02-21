use std::{any::TypeId, collections::HashMap, future::Future, pin::Pin, time::Duration};

use serde::Serialize;

use crate::{
    encoding::{CustomEncoder, EncoderRegistry},
    queue::ErasedQueueProducer,
    QueueError, QueueProducer, Result,
};

pub trait ScheduledQueueProducer: QueueProducer {
    fn send_raw_scheduled(
        &self,
        payload: &str,
        delay: Duration,
    ) -> impl Future<Output = Result<()>> + Send;

    fn send_serde_json_scheduled<P: Serialize + Sync>(
        &self,
        payload: &P,
        delay: Duration,
    ) -> impl Future<Output = Result<()>> + Send {
        async move {
            let payload = serde_json::to_string(payload)?;
            self.send_raw_scheduled(&payload, delay).await
        }
    }

    fn send_custom_scheduled<P: Send + Sync + 'static>(
        &self,
        payload: &P,
        delay: Duration,
    ) -> impl Future<Output = Result<()>> + Send {
        async move {
            let encoder = self
                .get_custom_encoders()
                .get(&TypeId::of::<P>())
                .ok_or(QueueError::NoEncoderForThisType)?;

            let payload = encoder.encode(payload)?;
            self.send_raw_scheduled(&payload, delay).await
        }
    }

    fn into_dyn_scheduled(self, custom_encoders: EncoderRegistry) -> DynScheduledQueueProducer
    where
        Self: 'static,
    {
        DynScheduledQueueProducer::new(self, custom_encoders)
    }
}

pub struct DynScheduledQueueProducer(Box<dyn ErasedScheduledQueueProducer>);

impl DynScheduledQueueProducer {
    fn new(inner: impl ScheduledQueueProducer + 'static, custom_encoders: EncoderRegistry) -> Self {
        let dyn_inner = DynScheduledProducerInner {
            inner,
            custom_encoders,
        };
        Self(Box::new(dyn_inner))
    }
}

trait ErasedScheduledQueueProducer: ErasedQueueProducer {
    fn send_raw_scheduled<'a>(
        &'a self,
        payload: &'a str,
        delay: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;
}

struct DynScheduledProducerInner<P> {
    inner: P,
    custom_encoders: EncoderRegistry,
}

impl<P: ScheduledQueueProducer> ErasedQueueProducer for DynScheduledProducerInner<P> {
    fn send_raw<'a>(
        &'a self,
        payload: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move { self.inner.send_raw(payload).await })
    }

    fn get_custom_encoders(&self) -> &HashMap<TypeId, Box<dyn CustomEncoder>> {
        self.custom_encoders.as_ref()
    }
}

impl<P: ScheduledQueueProducer> ErasedScheduledQueueProducer for DynScheduledProducerInner<P> {
    fn send_raw_scheduled<'a>(
        &'a self,
        payload: &'a str,
        delay: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move { self.inner.send_raw_scheduled(payload, delay).await })
    }
}

impl QueueProducer for DynScheduledQueueProducer {
    async fn send_raw(&self, payload: &str) -> Result<()> {
        self.0.send_raw(payload).await
    }

    fn get_custom_encoders(&self) -> &HashMap<TypeId, Box<dyn CustomEncoder>> {
        self.0.get_custom_encoders()
    }
}

impl ScheduledQueueProducer for DynScheduledQueueProducer {
    async fn send_raw_scheduled(&self, payload: &str, delay: Duration) -> Result<()> {
        self.0.send_raw_scheduled(payload, delay).await
    }

    fn into_dyn_scheduled(self, _custom_encoders: EncoderRegistry) -> DynScheduledQueueProducer {
        self
    }
}
