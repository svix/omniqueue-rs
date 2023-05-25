use std::{
    any::{Any, TypeId},
    collections::HashMap,
};

use async_trait::async_trait;
use serde::Serialize;

use crate::{
    encoding::{CustomEncoder, EncoderRegistry},
    QueueError, QueuePayload,
};

#[async_trait]
pub trait QueueProducer: 'static + Send + Sync {
    type Payload: QueuePayload;

    fn get_custom_encoders(&self) -> &HashMap<TypeId, Box<dyn CustomEncoder<Self::Payload>>>;

    async fn send_raw(&self, payload: &Self::Payload) -> Result<(), QueueError>;

    async fn send_bytes(&self, payload: &[u8]) -> Result<(), QueueError> {
        let payload = Self::Payload::from_bytes_naive(payload)?;
        self.send_raw(&payload).await
    }

    async fn send_serde_json<P: Serialize + Sync>(&self, payload: &P) -> Result<(), QueueError>
    where
        Self: Sized,
    {
        let payload = serde_json::to_vec(payload)?;
        self.send_bytes(&payload).await
    }

    async fn send_custom<P: 'static + Send + Sync>(&self, payload: &P) -> Result<(), QueueError>
    where
        Self: Sized,
    {
        let encoder = self
            .get_custom_encoders()
            .get(&TypeId::of::<P>())
            .ok_or(QueueError::NoEncoderForThisType)?;
        let payload = encoder.encode(payload)?;
        self.send_raw(&payload).await
    }

    fn into_dyn(self, custom_encoders: EncoderRegistry<Vec<u8>>) -> DynProducer
    where
        Self: Sized,
    {
        let dyn_inner = DynProducerInner {
            inner: self,
            custom_encoders,
        };
        DynProducer(Box::new(dyn_inner))
    }
}

struct DynProducerInner<T: QueuePayload, P: QueueProducer<Payload = T>> {
    inner: P,
    custom_encoders: EncoderRegistry<Vec<u8>>,
}

#[async_trait]
impl<T: QueuePayload, P: QueueProducer<Payload = T>> QueueProducer for DynProducerInner<T, P> {
    type Payload = Vec<u8>;

    async fn send_raw(&self, payload: &Vec<u8>) -> Result<(), QueueError> {
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
    }

    fn get_custom_encoders(&self) -> &HashMap<TypeId, Box<dyn CustomEncoder<Self::Payload>>> {
        self.custom_encoders.as_ref()
    }

    fn into_dyn(mut self, custom_encoders: EncoderRegistry<Vec<u8>>) -> DynProducer
    where
        Self: Sized,
    {
        self.custom_encoders = custom_encoders;
        DynProducer(Box::new(self))
    }
}

pub struct DynProducer(pub(crate) Box<dyn QueueProducer<Payload = Vec<u8>>>);

#[async_trait]
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
