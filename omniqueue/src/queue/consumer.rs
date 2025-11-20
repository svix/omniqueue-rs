use std::{cmp::min, future::Future, num::NonZeroUsize, pin::Pin, time::Duration};

use super::Delivery;
use crate::{QueuePayload, Result};

pub trait QueueConsumer: Send + Sized {
    type Payload: QueuePayload;

    fn receive(&mut self) -> impl Future<Output = Result<Delivery>> + Send;

    fn receive_all(
        &mut self,
        max_messages: usize,
        deadline: Duration,
    ) -> impl Future<Output = Result<Vec<Delivery>>> + Send;

    fn into_dyn<'a>(self) -> BaseDynConsumer<'a>
    where
        Self: 'a,
    {
        BaseDynConsumer::new(self)
    }

    /// Returns the largest number that may be passed as `max_messages` to
    /// `receive_all`.
    ///
    /// This is used by [`BaseDynConsumer`] to clamp the `max_messages` to
    /// what's permissible by the backend that ends up being used.
    fn max_messages(&self) -> Option<NonZeroUsize> {
        None
    }
}

macro_rules! ref_delegate {
    ($ty_param:ident, $ty:ty) => {
        #[deny(unconditional_recursion)]
        impl<$ty_param> QueueConsumer for $ty
        where
            $ty_param: QueueConsumer,
        {
            type Payload = $ty_param::Payload;

            fn receive(&mut self) -> impl Future<Output = Result<Delivery>> + Send {
                (**self).receive()
            }

            fn receive_all(
                &mut self,
                max_messages: usize,
                deadline: Duration,
            ) -> impl Future<Output = Result<Vec<Delivery>>> + Send {
                (**self).receive_all(max_messages, deadline)
            }
        }
    };
}

ref_delegate!(T, &mut T);
ref_delegate!(T, Box<T>);

pub struct BaseDynConsumer<'a>(Box<dyn ErasedQueueConsumer + 'a>);
pub type DynConsumer = BaseDynConsumer<'static>;

trait ErasedQueueConsumer: Send {
    fn receive(&mut self) -> Pin<Box<dyn Future<Output = Result<Delivery>> + Send + '_>>;
    fn receive_all(
        &mut self,
        max_messages: usize,
        deadline: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Delivery>>> + Send + '_>>;
    fn max_messages(&self) -> Option<NonZeroUsize>;
}

struct DynConsumerInner<C> {
    inner: C,
}

impl<C: QueueConsumer> ErasedQueueConsumer for DynConsumerInner<C> {
    fn receive(&mut self) -> Pin<Box<dyn Future<Output = Result<Delivery>> + Send + '_>> {
        Box::pin(async move {
            let mut t_payload = self.inner.receive().await?;
            Ok(Delivery {
                payload: t_payload.take_payload(),
                acker: t_payload.acker,
            })
        })
    }

    fn receive_all(
        &mut self,
        max_messages: usize,
        deadline: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Delivery>>> + Send + '_>> {
        Box::pin(async move {
            let xs = self.inner.receive_all(max_messages, deadline).await?;
            let mut out = Vec::with_capacity(xs.len());
            for mut t_payload in xs {
                out.push(Delivery {
                    payload: t_payload.take_payload(),
                    acker: t_payload.acker,
                });
            }
            Ok(out)
        })
    }

    fn max_messages(&self) -> Option<NonZeroUsize> {
        self.inner.max_messages()
    }
}

impl<'a> BaseDynConsumer<'a> {
    fn new(inner: impl QueueConsumer + 'a) -> Self {
        let c = DynConsumerInner { inner };
        Self(Box::new(c))
    }

    pub async fn receive(&mut self) -> Result<Delivery> {
        self.0.receive().await
    }

    /// Receive up to `max_messages` from the queue, waiting up to `deadline`
    /// for more messages to arrive.
    ///
    /// Unlike the `receive_all` methods on specific backends, this method
    /// clamps `max_messages` to what's permissible by the backend, so you don't
    /// have to know which backend is actually in use as a user of this type.
    pub async fn receive_all(
        &mut self,
        max_messages: usize,
        deadline: Duration,
    ) -> Result<Vec<Delivery>> {
        let max_messages = match self.max_messages() {
            Some(backend_max) => min(max_messages, backend_max.get()),
            None => max_messages,
        };
        self.0.receive_all(max_messages, deadline).await
    }
}

impl<'a> crate::QueueConsumer for BaseDynConsumer<'a> {
    type Payload = Vec<u8>;
    omni_delegate!(receive, receive_all);

    fn into_dyn<'b>(self) -> BaseDynConsumer<'b>
    where
        'a: 'b,
    {
        self
    }

    fn max_messages(&self) -> Option<NonZeroUsize> {
        self.0.max_messages()
    }
}
