use std::{
    any::{Any, TypeId},
    time::Duration,
};

use async_trait::async_trait;
use serde::Serialize;

use crate::{
    encoding::EncoderRegistry,
    queue::{
        producer::{DynProducer, QueueProducer},
        QueueBackend,
    },
    QueueError, QueuePayload,
};

#[async_trait]
pub trait SchedulerBackend: QueueBackend {
    async fn start_schedluer_background_task(
        &self,
    ) -> Option<tokio::task::JoinHandle<Result<(), QueueError>>>;
}

#[async_trait]
pub trait ScheduledProducer: QueueProducer {
    async fn send_raw_scheduled(
        &self,
        payload: &Self::Payload,
        delay: Duration,
    ) -> Result<(), QueueError>;

    async fn send_bytes_scheduled(
        &self,
        payload: &[u8],
        delay: Duration,
    ) -> Result<(), QueueError> {
        let payload = Self::Payload::from_bytes_naive(payload)?;
        self.send_raw_scheduled(&payload, delay).await
    }

    async fn send_serde_json_scheduled<P: Serialize + Sync>(
        &self,
        payload: &P,
        delay: Duration,
    ) -> Result<(), QueueError>
    where
        Self: Sized,
    {
        let payload = serde_json::to_vec(payload)?;
        self.send_bytes_scheduled(&payload, delay).await
    }

    async fn send_custom_scheduled<P: 'static + Send + Sync>(
        &self,
        payload: &P,
        delay: Duration,
    ) -> Result<(), QueueError>
    where
        Self: Sized,
    {
        let encoder = self
            .get_custom_encoders()
            .get(&TypeId::of::<P>())
            .ok_or(QueueError::NoEncoderForThisType)?;

        let payload = encoder.encode(payload)?;
        self.send_raw_scheduled(&payload, delay).await
    }

    fn into_dyn_scheduled(self, custom_encoders: EncoderRegistry<Vec<u8>>) -> DynScheduledProducer
    where
        Self: Sized,
    {
        let dyn_inner = DynScheduledProducerInner {
            inner: self,
            custom_encoders,
        };
        DynScheduledProducer(Box::new(dyn_inner))
    }
}

struct DynScheduledProducerInner<T: QueuePayload, P: ScheduledProducer<Payload = T>> {
    inner: P,
    custom_encoders: EncoderRegistry<Vec<u8>>,
}

#[async_trait]
impl<T: QueuePayload, P: ScheduledProducer<Payload = T>> QueueProducer
    for DynScheduledProducerInner<T, P>
{
    type Payload = Vec<u8>;

    async fn send_raw(&self, payload: &Vec<u8>) -> Result<(), QueueError> {
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

    fn get_custom_encoders(
        &self,
    ) -> &std::collections::HashMap<TypeId, Box<dyn crate::encoding::CustomEncoder<Self::Payload>>>
    {
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

#[async_trait]
impl<T: QueuePayload, P: ScheduledProducer<Payload = T>> ScheduledProducer
    for DynScheduledProducerInner<T, P>
{
    async fn send_raw_scheduled(
        &self,
        payload: &Vec<u8>,
        delay: Duration,
    ) -> Result<(), QueueError> {
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
    }

    fn into_dyn_scheduled(
        mut self,
        custom_encoders: EncoderRegistry<Vec<u8>>,
    ) -> DynScheduledProducer
    where
        Self: Sized,
    {
        self.custom_encoders = custom_encoders;
        DynScheduledProducer(Box::new(self))
    }
}

pub struct DynScheduledProducer(Box<dyn ScheduledProducer<Payload = Vec<u8>>>);

#[async_trait]
impl QueueProducer for DynScheduledProducer {
    type Payload = Vec<u8>;

    async fn send_raw(&self, payload: &Vec<u8>) -> Result<(), QueueError> {
        self.0.send_raw(payload).await
    }

    fn get_custom_encoders(
        &self,
    ) -> &std::collections::HashMap<TypeId, Box<dyn crate::encoding::CustomEncoder<Self::Payload>>>
    {
        self.0.get_custom_encoders()
    }
}

#[async_trait]
impl ScheduledProducer for DynScheduledProducer {
    async fn send_raw_scheduled(
        &self,
        payload: &Vec<u8>,
        delay: Duration,
    ) -> Result<(), QueueError> {
        self.0.send_raw_scheduled(payload, delay).await
    }

    fn into_dyn_scheduled(self, _custom_encoders: EncoderRegistry<Vec<u8>>) -> DynScheduledProducer
    where
        Self: Sized,
    {
        self
    }
}

pub struct SplitScheduledProducer<
    T: QueuePayload,
    P: QueueProducer<Payload = T>,
    S: ScheduledProducer<Payload = T>,
> {
    unscheduled: P,
    scheduled: S,
}

#[async_trait]
impl<T: QueuePayload, P: QueueProducer<Payload = T>, S: ScheduledProducer<Payload = T>>
    QueueProducer for SplitScheduledProducer<T, P, S>
{
    type Payload = T;

    fn get_custom_encoders(
        &self,
    ) -> &std::collections::HashMap<TypeId, Box<dyn crate::encoding::CustomEncoder<Self::Payload>>>
    {
        self.unscheduled.get_custom_encoders()
    }

    async fn send_raw(&self, payload: &Self::Payload) -> Result<(), QueueError> {
        self.unscheduled.send_raw(payload).await
    }
}

#[async_trait]
impl<T: QueuePayload, P: QueueProducer<Payload = T>, S: ScheduledProducer<Payload = T>>
    ScheduledProducer for SplitScheduledProducer<T, P, S>
{
    async fn send_raw_scheduled(
        &self,
        payload: &Self::Payload,
        delay: Duration,
    ) -> Result<(), QueueError> {
        self.scheduled.send_raw_scheduled(payload, delay).await
    }
}
