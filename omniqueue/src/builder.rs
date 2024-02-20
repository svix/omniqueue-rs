use std::{any::TypeId, collections::HashMap, marker::PhantomData, sync::Arc};

use crate::{
    decoding::{CustomDecoder, IntoCustomDecoder},
    encoding::{CustomEncoder, IntoCustomEncoder},
    queue::{
        consumer::{DynConsumer, QueueConsumer as _},
        producer::{DynProducer, QueueProducer as _},
        QueueBackend,
    },
    QueueError,
};

#[non_exhaustive]
pub struct Static;

#[non_exhaustive]
pub struct Dynamic;

/// Queue builder.
///
/// Created with
/// [`MemoryQueueBackend::builder`][crate::backends::memory_queue::MemoryQueueBackend::builder],
/// [`RedisQueueBackend::builder`][crate::backends::redis::RedisQueueBackend::builder] and so on.
pub struct QueueBuilder<Q: QueueBackend, S = Static> {
    config: Q::Config,

    encoders: HashMap<TypeId, Box<dyn CustomEncoder<Q::PayloadIn>>>,
    decoders: HashMap<TypeId, Arc<dyn CustomDecoder<Q::PayloadOut>>>,

    encoders_bytes: HashMap<TypeId, Box<dyn CustomEncoder<Vec<u8>>>>,
    decoders_bytes: HashMap<TypeId, Arc<dyn CustomDecoder<Vec<u8>>>>,

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

    pub fn with_encoder<I: 'static>(mut self, e: impl IntoCustomEncoder<I, Q::PayloadIn>) -> Self {
        let encoder = e.into();
        self.encoders.insert(TypeId::of::<I>(), encoder);

        self
    }

    pub fn with_decoder<O: 'static>(mut self, d: impl IntoCustomDecoder<Q::PayloadOut, O>) -> Self {
        let decoder = d.into();
        self.decoders.insert(TypeId::of::<O>(), decoder);

        self
    }

    pub async fn build_pair(self) -> Result<(Q::Producer, Q::Consumer), QueueError> {
        Q::new_pair(
            self.config,
            Arc::new(self.encoders),
            Arc::new(self.decoders),
        )
        .await
    }

    pub async fn build_producer(self) -> Result<Q::Producer, QueueError> {
        Q::producing_half(self.config, Arc::new(self.encoders)).await
    }

    pub async fn build_consumer(self) -> Result<Q::Consumer, QueueError> {
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
    pub fn with_bytes_encoder<I: 'static>(mut self, e: impl IntoCustomEncoder<I, Vec<u8>>) -> Self {
        let encoder = e.into();
        self.encoders_bytes.insert(TypeId::of::<I>(), encoder);

        self
    }

    pub fn with_bytes_decoder<O: 'static>(mut self, d: impl IntoCustomDecoder<Vec<u8>, O>) -> Self {
        let decoder = d.into();
        self.decoders_bytes.insert(TypeId::of::<O>(), decoder);

        self
    }

    pub async fn build_pair(self) -> Result<(DynProducer, DynConsumer), QueueError> {
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

    pub async fn build_producer(self) -> Result<DynProducer, QueueError> {
        let p = Q::producing_half(self.config, Arc::new(self.encoders)).await?;

        Ok(p.into_dyn(Arc::new(self.encoders_bytes)))
    }

    pub async fn build_consumer(self) -> Result<DynConsumer, QueueError> {
        let c = Q::consuming_half(self.config, Arc::new(self.decoders)).await?;

        Ok(c.into_dyn(Arc::new(self.decoders_bytes)))
    }
}
