use std::{future::Future, pin::Pin};

use serde::Serialize;

use crate::{QueuePayload, Result};

pub trait QueueProducer: Send + Sync + Sized {
    type Payload: QueuePayload;

    fn send_raw(&self, payload: &Self::Payload) -> impl Future<Output = Result<()>> + Send;

    fn send_bytes(&self, payload: &[u8]) -> impl Future<Output = Result<()>> + Send {
        async move {
            let payload = Self::Payload::from_bytes_naive(payload)?;
            self.send_raw(&payload).await
        }
    }

    fn send_serde_json<P: Serialize + Sync>(
        &self,
        payload: &P,
    ) -> impl Future<Output = Result<()>> + Send {
        async move {
            let payload = serde_json::to_vec(payload)?;
            self.send_bytes(&payload).await
        }
    }

    fn into_dyn(self) -> DynProducer
    where
        Self: 'static,
    {
        DynProducer::new(self)
    }
}

pub struct DynProducer(Box<dyn ErasedQueueProducer>);

impl DynProducer {
    fn new(inner: impl QueueProducer + 'static) -> Self {
        let dyn_inner = DynProducerInner { inner };
        Self(Box::new(dyn_inner))
    }
}

pub(crate) trait ErasedQueueProducer: Send + Sync {
    #[allow(clippy::ptr_arg)] // for now
    fn send_raw<'a>(
        &'a self,
        payload: &'a Vec<u8>,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;
}

struct DynProducerInner<P> {
    inner: P,
}

impl<P: QueueProducer> ErasedQueueProducer for DynProducerInner<P> {
    fn send_raw<'a>(
        &'a self,
        payload: &'a Vec<u8>,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move { self.inner.send_bytes(payload).await })
    }
}

impl QueueProducer for DynProducer {
    type Payload = Vec<u8>;

    async fn send_raw(&self, payload: &Vec<u8>) -> Result<()> {
        self.0.send_raw(payload).await
    }

    fn into_dyn(self) -> DynProducer {
        self
    }
}
