use std::marker::PhantomData;

use crate::{
    DynConsumer, DynProducer, QueueBackend, QueueConsumer as _, QueueProducer as _, Result,
};

#[non_exhaustive]
pub struct Static;

#[non_exhaustive]
pub struct Dynamic;

/// Queue builder.
///
/// Created with
/// [`MemoryQueueBackend::builder`][crate::backends::InMemoryBackend::builder],
/// [`RedisQueueBackend::builder`][crate::backends::RedisBackend::builder] and
/// so on.
pub struct QueueBuilder<Q: QueueBackend, S = Static> {
    pub(crate) config: Q::Config,

    _pd: PhantomData<S>,
}

impl<Q: QueueBackend> QueueBuilder<Q> {
    /// Creates a new queue builder.
    ///
    /// This constructor exists primarily as an implementation detail of
    /// `SomeQueueBackend::builder` associated function, which are the more
    /// convenient way of creating a queue builder.
    pub fn new(config: Q::Config) -> Self {
        Self {
            config,
            _pd: PhantomData,
        }
    }

    pub async fn build_pair(self) -> Result<(Q::Producer, Q::Consumer)> {
        Q::new_pair(self.config).await
    }

    pub async fn build_producer(self) -> Result<Q::Producer> {
        Q::producing_half(self.config).await
    }

    pub async fn build_consumer(self) -> Result<Q::Consumer> {
        Q::consuming_half(self.config).await
    }

    pub fn make_dynamic(self) -> QueueBuilder<Q, Dynamic> {
        QueueBuilder {
            config: self.config,
            _pd: PhantomData,
        }
    }
}

impl<Q: QueueBackend + 'static> QueueBuilder<Q, Dynamic> {
    pub async fn build_pair(self) -> Result<(DynProducer, DynConsumer)> {
        let (p, c) = Q::new_pair(self.config).await?;
        Ok((p.into_dyn(), c.into_dyn()))
    }

    pub async fn build_producer(self) -> Result<DynProducer> {
        let p = Q::producing_half(self.config).await?;

        Ok(p.into_dyn())
    }

    pub async fn build_consumer(self) -> Result<DynConsumer> {
        let c = Q::consuming_half(self.config).await?;

        Ok(c.into_dyn())
    }
}
