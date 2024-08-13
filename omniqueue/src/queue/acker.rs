use std::{future::Future, pin::Pin, time::Duration};

use sync_wrapper::SyncWrapper;

use crate::Result;

pub(crate) trait Acker: Send {
    fn ack(&mut self) -> impl Future<Output = Result<()>> + Send;
    fn nack(&mut self) -> impl Future<Output = Result<()>> + Send;
    #[cfg_attr(not(feature = "beta"), allow(dead_code))]
    fn set_ack_deadline(&mut self, duration: Duration) -> impl Future<Output = Result<()>> + Send;
}

pub(crate) struct DynAcker(SyncWrapper<Box<dyn ErasedAcker>>);

impl DynAcker {
    pub(super) fn new(inner: impl Acker + 'static) -> Self {
        let c = DynAckerInner { inner };
        Self(SyncWrapper::new(Box::new(c)))
    }
}

impl Acker for DynAcker {
    async fn ack(&mut self) -> Result<()> {
        self.0.get_mut().ack().await
    }

    async fn nack(&mut self) -> Result<()> {
        self.0.get_mut().nack().await
    }

    async fn set_ack_deadline(&mut self, duration: Duration) -> Result<()> {
        self.0.get_mut().set_ack_deadline(duration).await
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

#[cfg(test)]
mod tests {
    use super::DynAcker;

    fn assert_sync<T: Sync>() {}

    #[test]
    fn assert_acker_sync() {
        assert_sync::<DynAcker>();
    }
}
