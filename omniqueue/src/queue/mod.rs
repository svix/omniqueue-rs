use std::{fmt, future::Future};

use async_trait::async_trait;
use serde::de::DeserializeOwned;

use crate::{QueueError, QueuePayload, Result};

mod consumer;
mod producer;

pub(crate) use self::producer::ErasedQueueProducer;
pub use self::{
    consumer::{DynConsumer, QueueConsumer},
    producer::{DynProducer, QueueProducer},
};

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
    ) -> impl Future<Output = Result<(Self::Producer, Self::Consumer)>> + Send;

    fn producing_half(config: Self::Config) -> impl Future<Output = Result<Self::Producer>> + Send;
    fn consuming_half(config: Self::Config) -> impl Future<Output = Result<Self::Consumer>> + Send;
}

/// The output of queue backends
pub struct Delivery {
    pub(crate) payload: Option<Vec<u8>>,
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

#[async_trait]
pub(crate) trait Acker: Send + Sync {
    async fn ack(&mut self) -> Result<()>;
    async fn nack(&mut self) -> Result<()>;
}
