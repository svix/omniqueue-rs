#[cfg_attr(not(feature = "beta"), allow(unused_imports))]
use std::{fmt, future::Future, time::Duration};

use serde::de::DeserializeOwned;

use crate::{QueueError, QueuePayload, Result};

mod acker;
mod consumer;
mod producer;

use self::acker::DynAcker;
pub(crate) use self::producer::ErasedQueueProducer;
pub use self::{
    acker::Acker,
    consumer::{BaseDynConsumer, DynConsumer, QueueConsumer},
    producer::{BaseDynProducer, DynProducer, QueueProducer},
};

/// A marker trait with utility functions meant for the creation of new
/// producers and/or consumers.
///
/// This trait is meant to be implemented on an empty struct representing the
/// backend as a whole.
#[deprecated = "This trait is likely to be removed in the future, please open an issue if you find it useful"]
#[allow(deprecated)]
pub trait QueueBackend {
    type PayloadIn: QueuePayload;
    type PayloadOut: QueuePayload;

    type Producer: QueueProducer<Payload = Self::PayloadIn>;
    type Consumer: QueueConsumer<Payload = Self::PayloadOut>;

    type Config;

    #[deprecated = "Use SomeBackend::builder(config).build_pair() instead"]
    fn new_pair(
        config: Self::Config,
    ) -> impl Future<Output = Result<(Self::Producer, Self::Consumer)>> + Send;

    #[deprecated = "Use SomeBackend::builder(config).build_producer() instead"]
    fn producing_half(config: Self::Config) -> impl Future<Output = Result<Self::Producer>> + Send;

    #[deprecated = "Use SomeBackend::builder(config).build_consumer() instead"]
    fn consuming_half(config: Self::Config) -> impl Future<Output = Result<Self::Consumer>> + Send;
}

/// The output of queue backends
pub struct Delivery {
    payload: Option<Vec<u8>>,
    acker: DynAcker,
}

impl Delivery {
    #[doc(hidden)]
    pub fn new(payload: Vec<u8>, acker: impl Acker + 'static) -> Self {
        Self {
            payload: Some(payload),
            acker: DynAcker::new(acker),
        }
    }

    /// Acknowledges the receipt and successful processing of this [`Delivery`].
    ///
    /// On failure, `self` is returned alongside the error to allow retrying.
    ///
    /// The exact nature of this will vary per backend, but usually it ensures
    /// that the same message is not reprocessed.
    pub async fn ack(mut self) -> Result<(), (QueueError, Self)> {
        self.acker.ack().await.map_err(|e| (e, self))
    }

    #[cfg(feature = "beta")]
    /// Sets the deadline for acknowledging this [`Delivery`] to `duration`,
    /// starting from the time this method is called.
    ///
    /// The exact nature of this will vary per backend, but usually ensures
    /// that the same message will not be reprocessed if `ack()` is called
    /// within an interval of `duration` from the time this method is
    /// called. For example, this corresponds to the 'visibility timeout' in
    /// SQS, and the 'ack deadline' in GCP
    pub async fn set_ack_deadline(&mut self, duration: Duration) -> Result<(), QueueError> {
        self.acker.set_ack_deadline(duration).await
    }

    /// Explicitly does not Acknowledge the successful processing of this
    /// [`Delivery`].
    ///
    /// On failure, `self` is returned alongside the error to allow retrying.
    ///
    /// The exact nature of this will vary by backend, but usually it ensures
    /// that the same message is either reinserted into the same queue or is
    /// sent to a separate collection.
    pub async fn nack(mut self) -> Result<(), (QueueError, Self)> {
        self.acker.nack().await.map_err(|e| (e, self))
    }

    /// This method will take the contained bytes out of the delivery, doing no
    /// further processing.
    ///
    /// Once called, subsequent calls to any payload methods will fail.
    pub fn take_payload(&mut self) -> Option<Vec<u8>> {
        self.payload.take()
    }

    /// This method
    pub fn borrow_payload(&self) -> Option<&[u8]> {
        self.payload.as_deref()
    }

    pub fn payload_serde_json<T: DeserializeOwned>(&self) -> Result<Option<T>> {
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
