use std::{any::TypeId, collections::HashMap, future::Future, pin::Pin};

use serde::Serialize;

use crate::{
    encoding::{CustomEncoder, EncoderRegistry},
    QueueError, Result,
};

pub trait QueueProducer: Send + Sync + Sized {
    fn get_custom_encoders(&self) -> &HashMap<TypeId, Box<dyn CustomEncoder>>;

    fn send_raw(&self, payload: &str) -> impl Future<Output = Result<()>> + Send;

    fn send_serde_json<P: Serialize + Sync>(
        &self,
        payload: &P,
    ) -> impl Future<Output = Result<()>> + Send {
        async move {
            let payload = serde_json::to_string(payload)?;
            self.send_raw(&payload).await
        }
    }

    fn send_custom<P: Send + Sync + 'static>(
        &self,
        payload: &P,
    ) -> impl Future<Output = Result<()>> + Send {
        async move {
            let encoder = self
                .get_custom_encoders()
                .get(&TypeId::of::<P>())
                .ok_or(QueueError::NoEncoderForThisType)?;
            let payload = encoder.encode(payload)?;
            self.send_raw(&payload).await
        }
    }

    fn into_dyn(self, custom_encoders: EncoderRegistry) -> DynProducer
    where
        Self: 'static,
    {
        DynProducer::new(self, custom_encoders)
    }
}

pub struct DynProducer(Box<dyn ErasedQueueProducer>);

impl DynProducer {
    fn new(inner: impl QueueProducer + 'static, custom_encoders: EncoderRegistry) -> Self {
        let dyn_inner = DynProducerInner {
            inner,
            custom_encoders,
        };
        Self(Box::new(dyn_inner))
    }
}

pub(crate) trait ErasedQueueProducer: Send + Sync {
    fn send_raw<'a>(
        &'a self,
        payload: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;
    fn get_custom_encoders(&self) -> &HashMap<TypeId, Box<dyn CustomEncoder>>;
}

struct DynProducerInner<P> {
    inner: P,
    custom_encoders: EncoderRegistry,
}

impl<P: QueueProducer> ErasedQueueProducer for DynProducerInner<P> {
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

impl QueueProducer for DynProducer {
    async fn send_raw(&self, payload: &str) -> Result<()> {
        self.0.send_raw(payload).await
    }

    fn get_custom_encoders(&self) -> &HashMap<TypeId, Box<dyn CustomEncoder>> {
        self.0.get_custom_encoders()
    }

    fn into_dyn(self, _custom_encoders: EncoderRegistry) -> DynProducer {
        self
    }
}
