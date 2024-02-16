use std::{any::TypeId, collections::HashMap, fmt, future::Future, marker::PhantomData, sync::Arc};

use async_trait::async_trait;
use serde::de::DeserializeOwned;

use crate::{
    decoding::{CustomDecoder, DecoderRegistry, IntoCustomDecoder},
    encoding::{CustomEncoder, EncoderRegistry, IntoCustomEncoder},
    QueueError, QueuePayload,
};

use self::{
    consumer::{DynConsumer, QueueConsumer},
    producer::{DynProducer, QueueProducer},
};

pub mod consumer;
pub mod producer;

/// A marker trait with utility functions meant for the creation of new producers and/or consumers.
///
/// This trait is meant to be implemented on an empty struct representing the backend as a whole.
pub trait QueueBackend {
    type PayloadIn: QueuePayload;
    type PayloadOut: QueuePayload;

    type Producer: QueueProducer<Payload = Self::PayloadIn>;
    type Consumer: QueueConsumer<Payload = Self::PayloadOut>;

    type Config;

    fn new_pair(
        config: Self::Config,
        custom_encoders: EncoderRegistry<Self::PayloadIn>,
        custom_decoders: DecoderRegistry<Self::PayloadOut>,
    ) -> impl Future<Output = Result<(Self::Producer, Self::Consumer), QueueError>> + Send;

    fn producing_half(
        config: Self::Config,
        custom_encoders: EncoderRegistry<Self::PayloadIn>,
    ) -> impl Future<Output = Result<Self::Producer, QueueError>> + Send;

    fn consuming_half(
        config: Self::Config,
        custom_decoders: DecoderRegistry<Self::PayloadOut>,
    ) -> impl Future<Output = Result<Self::Consumer, QueueError>> + Send;

    fn builder(config: Self::Config) -> QueueBuilder<Self, Static>
    where
        Self: Sized,
    {
        QueueBuilder::new(config)
    }
}

/// The output of queue backends
pub struct Delivery {
    pub(crate) payload: Option<Vec<u8>>,

    pub(crate) decoders: DecoderRegistry<Vec<u8>>,
    pub(crate) acker: Box<dyn Acker>,
}

impl Delivery {
    /// Acknowledges the receipt and successful processing of this [`Delivery`].
    ///
    /// On failure, `self` is returned alongside the error to allow retrying.
    ///
    /// The exact nature of this will vary per backend, but usually it ensures that the same message
    /// is not reprocessed.
    pub async fn ack(mut self) -> Result<(), (QueueError, Self)> {
        self.acker.ack().await.map_err(|e| (e, self))
    }

    /// Explicitly does not Acknowledge the successful processing of this [`Delivery`].
    ///
    /// On failure, `self` is returned alongside the error to allow retrying.
    ///
    /// The exact nature of this will vary by backend, but usually it ensures that the same message
    /// is either reinserted into the same queue or is sent to a separate collection.
    pub async fn nack(mut self) -> Result<(), (QueueError, Self)> {
        self.acker.nack().await.map_err(|e| (e, self))
    }

    /// This method will deserialize the contained bytes using the configured decoder.
    ///
    /// If a decoder does not exist for the type parameter T, this function will return an error.
    ///
    /// This method does not consume the payload.
    pub fn payload_custom<T: 'static>(&self) -> Result<Option<T>, QueueError> {
        let Some(payload) = self.payload.as_ref() else {
            return Ok(None);
        };

        let decoder = self
            .decoders
            .get(&TypeId::of::<T>())
            .ok_or(QueueError::NoDecoderForThisType)?;
        decoder
            .decode(payload)?
            .downcast()
            .map(|boxed| Some(*boxed))
            .map_err(|_| QueueError::AnyError)
    }

    /// This method will take the contained bytes out of the delivery, doing no further processing.
    ///
    /// Once called, subsequent calls to any payload methods will fail.
    pub fn take_payload(&mut self) -> Option<Vec<u8>> {
        self.payload.take()
    }

    /// This method
    pub fn borrow_payload(&self) -> Option<&[u8]> {
        self.payload.as_deref()
    }

    pub fn payload_serde_json<T: DeserializeOwned>(&self) -> Result<Option<T>, QueueError> {
        let Some(bytes) = self.payload.as_ref() else {
            return Ok(None);
        };
        serde_json::from_slice(bytes).map_err(Into::into)
    }
}

impl fmt::Debug for Delivery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Delivery").finish()
    }
}

#[async_trait]
pub(crate) trait Acker: Send + Sync {
    async fn ack(&mut self) -> Result<(), QueueError>;
    async fn nack(&mut self) -> Result<(), QueueError>;
}

pub trait QueueBuilderState {}

pub struct Static;
impl QueueBuilderState for Static {}

pub struct Dynamic;
impl QueueBuilderState for Dynamic {}

pub struct QueueBuilder<Q: 'static + QueueBackend, S: QueueBuilderState> {
    config: Q::Config,

    encoders: HashMap<TypeId, Box<dyn CustomEncoder<Q::PayloadIn>>>,
    decoders: HashMap<TypeId, Arc<dyn CustomDecoder<Q::PayloadOut>>>,

    encoders_bytes: HashMap<TypeId, Box<dyn CustomEncoder<Vec<u8>>>>,
    decoders_bytes: HashMap<TypeId, Arc<dyn CustomDecoder<Vec<u8>>>>,

    _pd: PhantomData<S>,
}

impl<Q: 'static + QueueBackend> QueueBuilder<Q, Static> {
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

    pub fn with_encoder<I: 'static + Send + Sync>(
        mut self,
        e: impl IntoCustomEncoder<I, Q::PayloadIn>,
    ) -> Self {
        let encoder = e.into();
        self.encoders.insert(TypeId::of::<I>(), encoder);

        self
    }

    pub fn with_decoder<O: 'static + Send + Sync>(
        mut self,
        d: impl IntoCustomDecoder<Q::PayloadOut, O>,
    ) -> Self {
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

impl<Q: 'static + QueueBackend> QueueBuilder<Q, Dynamic> {
    pub fn with_bytes_encoder<I: 'static + Send + Sync>(
        mut self,
        e: impl IntoCustomEncoder<I, Vec<u8>>,
    ) -> Self {
        let encoder = e.into();
        self.encoders_bytes.insert(TypeId::of::<I>(), encoder);

        self
    }

    pub fn with_bytes_decoder<O: 'static + Send + Sync>(
        mut self,
        d: impl IntoCustomDecoder<Vec<u8>, O>,
    ) -> Self {
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
