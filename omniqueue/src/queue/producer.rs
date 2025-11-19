use std::{future::Future, pin::Pin, sync::Arc};

use serde::Serialize;

use crate::{QueuePayload, Result};

pub trait QueueProducer: Send + Sync + Sized {
    type Payload: QueuePayload;

    fn send_raw(&self, payload: &Self::Payload) -> impl Future<Output = Result<()>> + Send;

    fn redrive_dlq(&self) -> impl Future<Output = Result<()>> + Send;

    /// Send a batch of raw messages.
    ///
    /// The default implementation of this sends the payloads sequentially using
    /// [`send_raw`][QueueProducer::send_raw]. Specific backends use more
    /// efficient implementations where the underlying protocols support it.
    #[tracing::instrument(name = "send_batch", skip_all)]
    fn send_raw_batch(
        &self,
        payloads: impl IntoIterator<Item: AsRef<Self::Payload> + Send, IntoIter: Send> + Send,
    ) -> impl Future<Output = Result<()>> + Send {
        async move {
            for payload in payloads {
                self.send_raw(payload.as_ref()).await?;
            }
            Ok(())
        }
    }

    fn send_bytes(&self, payload: &[u8]) -> impl Future<Output = Result<()>> + Send {
        async move {
            let payload = Self::Payload::from_bytes_naive(payload)?;
            self.send_raw(&payload).await
        }
    }

    #[tracing::instrument(name = "send_batch", skip_all)]
    fn send_bytes_batch(
        &self,
        payloads: impl IntoIterator<Item: AsRef<[u8]> + Send, IntoIter: Send> + Send,
    ) -> impl Future<Output = Result<()>> + Send {
        async move {
            let payloads: Vec<_> = payloads
                .into_iter()
                .map(|p| Self::Payload::from_bytes_naive(p.as_ref()))
                .collect::<Result<_, _>>()?;
            self.send_raw_batch(payloads).await
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

    #[tracing::instrument(name = "send_batch", skip_all)]
    fn send_serde_json_batch(
        &self,
        payloads: impl IntoIterator<Item: Serialize + Send, IntoIter: Send> + Send,
    ) -> impl Future<Output = Result<()>> + Send {
        async move {
            let payloads: Vec<_> = payloads
                .into_iter()
                .map(|payload| {
                    let payload = serde_json::to_vec(&payload)?;
                    Self::Payload::from_bytes_naive(&payload)
                })
                .collect::<Result<_>>()?;
            self.send_raw_batch(payloads).await
        }
    }

    fn into_dyn(self) -> DynProducer
    where
        Self: 'static,
    {
        DynProducer::new(self)
    }
}

macro_rules! ref_delegate {
    ($ty_param:ident, $ty:ty) => {
        #[deny(unconditional_recursion)]
        impl<$ty_param> QueueProducer for $ty
        where
            $ty_param: QueueProducer,
        {
            type Payload = $ty_param::Payload;

            fn send_raw(&self, payload: &Self::Payload) -> impl Future<Output = Result<()>> + Send {
                (**self).send_raw(payload)
            }

            fn send_raw_batch(
                &self,
                payloads: impl IntoIterator<Item: AsRef<Self::Payload> + Send, IntoIter: Send> + Send,
            ) -> impl Future<Output = Result<()>> + Send {
                (**self).send_raw_batch(payloads)
            }

            fn send_bytes(&self, payload: &[u8]) -> impl Future<Output = Result<()>> + Send {
                (**self).send_bytes(payload)
            }

            fn send_bytes_batch(
                &self,
                payloads: impl IntoIterator<Item: AsRef<[u8]> + Send, IntoIter: Send> + Send,
            ) -> impl Future<Output = Result<()>> + Send {
                (**self).send_bytes_batch(payloads)
            }

            fn send_serde_json<P: Serialize + Sync>(
                &self,
                payload: &P,
            ) -> impl Future<Output = Result<()>> + Send {
                (**self).send_serde_json(payload)
            }

            fn send_serde_json_batch(
                &self,
                payloads: impl IntoIterator<Item: Serialize + Send, IntoIter: Send> + Send,
            ) -> impl Future<Output = Result<()>> + Send {
                (**self).send_serde_json_batch(payloads)
            }

            fn redrive_dlq(&self) -> impl Future<Output = Result<()>> + Send {
                (**self).redrive_dlq()
            }
        }
    };
}

ref_delegate!(T, &T);
ref_delegate!(T, Box<T>);
ref_delegate!(T, Arc<T>);

pub struct DynProducer(Box<dyn ErasedQueueProducer>);

impl DynProducer {
    fn new(inner: impl QueueProducer + 'static) -> Self {
        let dyn_inner = DynProducerInner { inner };
        Self(Box::new(dyn_inner))
    }
}

pub(crate) trait ErasedQueueProducer: Send + Sync {
    fn send_raw<'a>(
        &'a self,
        payload: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;

    fn redrive_dlq<'a>(&'a self) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;
}

struct DynProducerInner<P> {
    inner: P,
}

impl<P: QueueProducer> ErasedQueueProducer for DynProducerInner<P> {
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

impl DynProducer {
    pub async fn send_raw(&self, payload: &[u8]) -> Result<()> {
        self.0.send_raw(payload).await
    }

    pub async fn send_serde_json<P: Serialize + Sync>(&self, payload: &P) -> Result<()> {
        let payload = serde_json::to_vec(payload)?;
        self.send_raw(&payload).await
    }

    pub async fn redrive_dlq(&self) -> Result<()> {
        self.0.redrive_dlq().await
    }
}

impl crate::QueueProducer for DynProducer {
    type Payload = Vec<u8>;
    omni_delegate!(send_raw, send_serde_json, redrive_dlq);
}
