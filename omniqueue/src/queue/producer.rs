use std::{
    any::{Any, TypeId},
    collections::HashMap,
    future::Future,
    pin::Pin,
};

use serde::Serialize;

use crate::{
    encoding::{CustomEncoder, EncoderRegistry},
    QueueError, QueuePayload,
};

pub trait QueueProducer: 'static + Send + Sync {
    type Payload: QueuePayload;

    fn get_custom_encoders(&self) -> &HashMap<TypeId, Box<dyn CustomEncoder<Self::Payload>>>;

    fn send_raw(
        &self,
        payload: &Self::Payload,
    ) -> impl Future<Output = Result<(), QueueError>> + Send;

    fn send_bytes(&self, payload: &[u8]) -> impl Future<Output = Result<(), QueueError>> + Send {
        async move {
            let payload = Self::Payload::from_bytes_naive(payload)?;
            self.send_raw(&payload).await
        }
    }

    fn send_serde_json<P: Serialize + Sync>(
        &self,
        payload: &P,
    ) -> impl Future<Output = Result<(), QueueError>> + Send
    where
        Self: Sized,
    {
        async move {
            let payload = serde_json::to_vec(payload)?;
            self.send_bytes(&payload).await
        }
    }

    fn send_custom<P: 'static + Send + Sync>(
        &self,
        payload: &P,
    ) -> impl Future<Output = Result<(), QueueError>> + Send
    where
        Self: Sized,
    {
        async move {
            let encoder = self
                .get_custom_encoders()
                .get(&TypeId::of::<P>())
                .ok_or(QueueError::NoEncoderForThisType)?;
            let payload = encoder.encode(payload)?;
            self.send_raw(&payload).await
        }
    }

    fn into_dyn(self, custom_encoders: EncoderRegistry<Vec<u8>>) -> DynProducer
    where
        Self: Sized,
    {
        DynProducer::new(self, custom_encoders)
    }
}

pub struct DynProducer(Box<dyn ErasedQueueProducer>);

impl DynProducer {
    fn new(inner: impl QueueProducer, custom_encoders: EncoderRegistry<Vec<u8>>) -> Self {
        let dyn_inner = DynProducerInner {
            inner,
            custom_encoders,
        };
        Self(Box::new(dyn_inner))
    }
}

pub(crate) trait ErasedQueueProducer: Send + Sync {
    #[allow(clippy::ptr_arg)] // for now
    fn send_raw<'a>(
        &'a self,
        payload: &'a Vec<u8>,
    ) -> Pin<Box<dyn Future<Output = Result<(), QueueError>> + Send + 'a>>;
    fn get_custom_encoders(&self) -> &HashMap<TypeId, Box<dyn CustomEncoder<Vec<u8>>>>;
}

struct DynProducerInner<P> {
    inner: P,
    custom_encoders: EncoderRegistry<Vec<u8>>,
}

impl<P: QueueProducer> ErasedQueueProducer for DynProducerInner<P> {
    fn send_raw<'a>(
        &'a self,
        payload: &'a Vec<u8>,
    ) -> Pin<Box<dyn Future<Output = Result<(), QueueError>> + Send + 'a>> {
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

impl QueueProducer for DynProducer {
    type Payload = Vec<u8>;

    async fn send_raw(&self, payload: &Vec<u8>) -> Result<(), QueueError> {
        self.0.send_raw(payload).await
    }

    fn get_custom_encoders(&self) -> &HashMap<TypeId, Box<dyn CustomEncoder<Self::Payload>>> {
        self.0.get_custom_encoders()
    }

    fn into_dyn(self, _custom_encoders: EncoderRegistry<Vec<u8>>) -> DynProducer {
        self
    }
}
