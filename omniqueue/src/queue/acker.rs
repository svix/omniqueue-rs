use std::{future::Future, pin::Pin, time::Duration};

use crate::Result;

pub(crate) trait Acker: Send {
    fn ack(&mut self) -> impl Future<Output = Result<()>> + Send;
    fn nack(&mut self) -> impl Future<Output = Result<()>> + Send;
    #[cfg_attr(not(feature = "beta"), allow(dead_code))]
    fn set_ack_deadline(&mut self, duration: Duration) -> impl Future<Output = Result<()>> + Send;
}

pub(crate) struct DynAcker(Box<dyn ErasedAcker>);

impl DynAcker {
    pub(super) fn new(inner: impl Acker + 'static) -> Self {
        let c = DynAckerInner { inner };
        Self(Box::new(c))
    }
}

impl Acker for DynAcker {
    async fn ack(&mut self) -> Result<()> {
        self.0.ack().await
    }

    async fn nack(&mut self) -> Result<()> {
        self.0.nack().await
    }

    async fn set_ack_deadline(&mut self, duration: Duration) -> Result<()> {
        self.0.set_ack_deadline(duration).await
    }
}

trait ErasedAcker: Send {
    fn ack(&mut self) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>>;
    fn nack(&mut self) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>>;
    #[cfg_attr(not(feature = "beta"), allow(dead_code))]
    fn set_ack_deadline(
        &mut self,
        duration: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>>;
}

struct DynAckerInner<C> {
    inner: C,
}

impl<C: Acker> ErasedAcker for DynAckerInner<C> {
    fn ack(&mut self) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        Box::pin(async move { self.inner.ack().await })
    }

    fn nack(&mut self) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        Box::pin(async move { self.inner.nack().await })
    }

    fn set_ack_deadline(
        &mut self,
        duration: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>> {
        Box::pin(async move { self.inner.set_ack_deadline(duration).await })
    }
}
