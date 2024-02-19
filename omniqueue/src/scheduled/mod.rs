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
    queue::{
        producer::{ErasedQueueProducer, QueueProducer},
        QueueBackend,
    },
    QueueError, QueuePayload,
};

// FIXME(onelson): only used by redis -- is this meant to be called internally or by the caller building the backend?
pub trait SchedulerBackend: QueueBackend {
    fn start_scheduler_background_task(
        &self,
    ) -> impl Future<Output = Option<tokio::task::JoinHandle<Result<(), QueueError>>>> + Send;
}

pub trait ScheduledProducer: QueueProducer {
    fn send_raw_scheduled(
        &self,
        payload: &Self::Payload,
        delay: Duration,
    ) -> impl Future<Output = Result<(), QueueError>> + Send;

    fn send_bytes_scheduled(
        &self,
        payload: &[u8],
        delay: Duration,
    ) -> impl Future<Output = Result<(), QueueError>> + Send {
        async move {
            let payload = Self::Payload::from_bytes_naive(payload)?;
            self.send_raw_scheduled(&payload, delay).await
        }
    }

    fn send_serde_json_scheduled<P: Serialize + Sync>(
        &self,
        payload: &P,
        delay: Duration,
    ) -> impl Future<Output = Result<(), QueueError>> + Send
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
            self.send_raw_scheduled(&payload, delay).await
        }
    }

    fn into_dyn_scheduled(self, custom_encoders: EncoderRegistry<Vec<u8>>) -> DynScheduledProducer
    where
        Self: Sized + 'static,
    {
        DynScheduledProducer::new(self, custom_encoders)
    }
}

pub struct DynScheduledProducer(Box<dyn ErasedScheduledProducer>);

impl DynScheduledProducer {
    fn new(
        inner: impl ScheduledProducer + 'static,
        custom_encoders: EncoderRegistry<Vec<u8>>,
    ) -> Self {
        let dyn_inner = DynScheduledProducerInner {
            inner,
            custom_encoders,
        };
        Self(Box::new(dyn_inner))
    }
}

trait ErasedScheduledProducer: ErasedQueueProducer {
    #[allow(clippy::ptr_arg)] // for now
    fn send_raw_scheduled<'a>(
        &'a self,
        payload: &'a Vec<u8>,
        delay: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<(), QueueError>> + Send + 'a>>;
}

struct DynScheduledProducerInner<P> {
    inner: P,
    custom_encoders: EncoderRegistry<Vec<u8>>,
}

impl<P: ScheduledProducer> ErasedQueueProducer for DynScheduledProducerInner<P> {
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

impl<P: ScheduledProducer> ErasedScheduledProducer for DynScheduledProducerInner<P> {
    fn send_raw_scheduled<'a>(
        &'a self,
        payload: &'a Vec<u8>,
        delay: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<(), QueueError>> + Send + 'a>> {
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

impl QueueProducer for DynScheduledProducer {
    type Payload = Vec<u8>;

    async fn send_raw(&self, payload: &Vec<u8>) -> Result<(), QueueError> {
        self.0.send_raw(payload).await
    }

    fn get_custom_encoders(&self) -> &HashMap<TypeId, Box<dyn CustomEncoder<Self::Payload>>> {
        self.0.get_custom_encoders()
    }
}

impl ScheduledProducer for DynScheduledProducer {
    async fn send_raw_scheduled(
        &self,
        payload: &Vec<u8>,
        delay: Duration,
    ) -> Result<(), QueueError> {
        self.0.send_raw_scheduled(payload, delay).await
    }

    fn into_dyn_scheduled(
        self,
        _custom_encoders: EncoderRegistry<Vec<u8>>,
    ) -> DynScheduledProducer {
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

impl<T: QueuePayload, P: QueueProducer<Payload = T>, S: ScheduledProducer<Payload = T>>
    QueueProducer for SplitScheduledProducer<T, P, S>
{
    type Payload = T;

    fn get_custom_encoders(&self) -> &HashMap<TypeId, Box<dyn CustomEncoder<Self::Payload>>> {
        self.unscheduled.get_custom_encoders()
    }

    async fn send_raw(&self, payload: &Self::Payload) -> Result<(), QueueError> {
        self.unscheduled.send_raw(payload).await
    }
}

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
