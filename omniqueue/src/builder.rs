use std::{any::TypeId, collections::HashMap, marker::PhantomData, sync::Arc};

use crate::{
    decoding::{CustomDecoder, IntoCustomDecoder},
    encoding::{CustomEncoder, IntoCustomEncoder},
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
    config: Q::Config,

    encoders: HashMap<TypeId, Box<dyn CustomEncoder>>,
    decoders: HashMap<TypeId, Arc<dyn CustomDecoder>>,

    encoders_bytes: HashMap<TypeId, Box<dyn CustomEncoder>>,
    decoders_bytes: HashMap<TypeId, Arc<dyn CustomDecoder>>,

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
            encoders: HashMap::new(),
            decoders: HashMap::new(),
            encoders_bytes: HashMap::new(),
            decoders_bytes: HashMap::new(),
            _pd: PhantomData,
        }
    }

    pub fn with_encoder<I: 'static>(mut self, e: impl IntoCustomEncoder<I>) -> Self {
        let encoder = e.into();
        self.encoders.insert(TypeId::of::<I>(), encoder);

        self
    }

    pub fn with_decoder<O: 'static>(mut self, d: impl IntoCustomDecoder<O>) -> Self {
        let decoder = d.into();
        self.decoders.insert(TypeId::of::<O>(), decoder);

        self
    }

    pub async fn build_pair(self) -> Result<(Q::Producer, Q::Consumer)> {
        Q::new_pair(
            self.config,
            Arc::new(self.encoders),
            Arc::new(self.decoders),
        )
        .await
    }

    pub async fn build_producer(self) -> Result<Q::Producer> {
        Q::producing_half(self.config, Arc::new(self.encoders)).await
    }

    pub async fn build_consumer(self) -> Result<Q::Consumer> {
        Q::consuming_half(self.config, Arc::new(self.decoders)).await
    }

    pub fn make_dynamic(self) -> QueueBuilder<Q, Dynamic> {
        QueueBuilder {
            config: self.config,
            encoders: self.encoders,
            decoders: self.decoders,
            encoders_bytes: self.encoders_bytes,
            decoders_bytes: self.decoders_bytes,
            _pd: PhantomData,
        }
    }
}

impl<Q: QueueBackend + 'static> QueueBuilder<Q, Dynamic> {
    pub fn with_raw_encoder<I: 'static>(mut self, e: impl IntoCustomEncoder<I>) -> Self {
        let encoder = e.into();
        self.encoders_bytes.insert(TypeId::of::<I>(), encoder);

        self
    }

    pub fn with_raw_decoder<O: 'static>(mut self, d: impl IntoCustomDecoder<O>) -> Self {
        let decoder = d.into();
        self.decoders_bytes.insert(TypeId::of::<O>(), decoder);

        self
    }

    pub async fn build_pair(self) -> Result<(DynProducer, DynConsumer)> {
        let (p, c) = Q::new_pair(
            self.config,
            Arc::new(self.encoders),
            Arc::new(self.decoders),
        )
        .await?;
        Ok((
            p.into_dyn(Arc::new(self.encoders_bytes)),
            c.into_dyn(Arc::new(self.decoders_bytes)),
        ))
    }

    pub async fn build_producer(self) -> Result<DynProducer> {
        let p = Q::producing_half(self.config, Arc::new(self.encoders)).await?;

        Ok(p.into_dyn(Arc::new(self.encoders_bytes)))
    }

    pub async fn build_consumer(self) -> Result<DynConsumer> {
        let c = Q::consuming_half(self.config, Arc::new(self.decoders)).await?;

        Ok(c.into_dyn(Arc::new(self.decoders_bytes)))
    }
}
