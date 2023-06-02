//! # Omniqueue
//!
//! Omniqueue provides a high-level interface for sending and receiving the following over a range
//! of queue backends:
//!
//!   * Raw byte arrays in the way most compatible with the queue backend
//!
//!   * JSON encoded byte arrays for types that implement [`serde::Deserialize`] and
//!     [`serde::Serialize`]
//!
//!   * Arbitrary types for which an encoder and/or decoder has been defined
//!
//! ## Cargo Features
//!
//! Each backend is enabled with its associated cargo feature. All backends are enabled by default.
//! As of present it supports:
//!
//!   * In-memory queues
//!
//!   * RabbitMQ
//!
//!   * Redis streams
//!
//!   * SQS
//!
//! ## How to Use Omniqueue
//!
//!   Each queue backend has a unique configuration type. One of these configurations is taken
//!   when constructing the [`queue::QueueBuilder`].
//!
//!   To create a simple producer and/or consumer:
//!
//!   ```compile_fail
//!   let cfg = SqsConfig {
//!       queue_dsn: "http://localhost:9234/queue/queue_name".to_owned(),
//!       override_endpoint: true,
//!   };
//!      
//!   // Either both producer and consumer
//!   let (p, mut c) = SqsQueueBackend::builder(cfg.clone()).build_pair().await?;
//!
//!   // Or one half
//!   let p = SqsQueueBackend::builder(cfg.clone()).build_producer().await?;
//!   let mut c = SqsQueueBackend::builder(cfg).build_consumer().await?;
//!
//!   (p, c)
//!   ```
//!
//!   Sending and receiving information from this queue is simple:
//!
//!   ```compile_fail
//!   p.send_serde_json(&ExampleType::default()).await?;
//!
//!   let delivery = c.receive().await?;
//!   let payload = delivery.payload_serde_json::<ExampleType>().await?;
//!   delivery.ack().await?;
//!   ```
//!
//! ## `DynProducer`s and `DynConsumer`s
//!
//! Dynamic-dispatch can be used easily for when you're not sure which backend to use at
//! compile-time.
//!
//! Making a `DynProducer` or `DynConsumer` is as simple as adding one line to the builder:
//!
//! ```compile_fail
//! let (p, mut c) = RabbitMqBackend::builder(cfg)
//!     .make_dynamic()
//!     .build_pair()
//!     .await?;
//! ```
//!
//! ## Encoders/Decoders
//!
//! The [`encoding::CustomEncoder`]s and [`decoding::CustomDecoder`]s given to the builder upon
//! producer/consumer creation will be used to convert from/to the queue's native representation
//! into/from a given type. This helps enforce a separation of responsibilities where only the
//! application setting up a concrete queue instance should ever have to think about the internal
//! data-representation of items within the queue while abstract uses of queues should be able to
//! work with simple Rust types.
//!
//! Any function or closure with the right signature may be used as an encoder or decoder.
//!
//! ```compile_fail
//! #[derive(Debug, PartialEq)]
//! struct ExampleType {
//!     field: u8,
//! }
//!
//! let (p, mut c) = RabbitMqBackend::builder(cfg)
//!     .with_encoder(|et: &ExampleType| -> Result<Vec<u8>, QueueError> {
//!         Ok(vec![et.field])
//!     })
//!     .with_decoder(|v: &Vec<u8>| -> Result<ExampleType, QueueError> {
//!         Ok(ExampleType {
//!             field: *v.first().unwrap_or(&0),
//!         })
//!     })
//! ```
use std::fmt::Debug;

use thiserror::Error;

pub mod backends;
pub mod decoding;
pub mod encoding;
pub mod queue;
pub mod scheduled;

#[derive(Debug, Error)]
pub enum QueueError {
    #[error("only `new_pair` may be used with this type")]
    CannotCreateHalf,

    #[error("a single delivery may only be ACKed or NACKed once")]
    CannotAckOrNackTwice,

    #[error("no data was received from the queue")]
    NoData,
    #[error("(de)serialization error")]
    Serde(#[from] serde_json::Error),

    #[error("cannot decode into custom type without registered decoder")]
    NoDecoderForThisType,
    #[error("cannot encode into custom type without registered encoder")]
    NoEncoderForThisType,
    #[error("error downcasting to custom type")]
    AnyError,

    #[error("{0}")]
    Generic(Box<dyn std::error::Error + Send + Sync>),
}

impl QueueError {
    pub fn generic<E: 'static + std::error::Error + Send + Sync>(e: E) -> Self {
        Self::Generic(Box::new(e))
    }
}

pub trait QueuePayload: 'static + Send + Sync {
    fn to_bytes_naive(&self) -> Result<Vec<u8>, QueueError>;
    fn from_bytes_naive(bytes: &[u8]) -> Result<Box<Self>, QueueError>;
}

impl QueuePayload for Vec<u8> {
    fn to_bytes_naive(&self) -> Result<Vec<u8>, QueueError> {
        Ok(self.clone())
    }

    fn from_bytes_naive(bytes: &[u8]) -> Result<Box<Self>, QueueError> {
        Ok(Box::new(bytes.to_owned()))
    }
}

impl QueuePayload for String {
    fn to_bytes_naive(&self) -> Result<Vec<u8>, QueueError> {
        Ok(self.as_bytes().to_owned())
    }

    fn from_bytes_naive(bytes: &[u8]) -> Result<Box<Self>, QueueError> {
        Ok(Box::new(
            String::from_utf8(bytes.to_owned()).map_err(QueueError::generic)?,
        ))
    }
}

impl QueuePayload for serde_json::Value {
    fn to_bytes_naive(&self) -> Result<Vec<u8>, QueueError> {
        serde_json::to_vec(self).map_err(Into::into)
    }

    fn from_bytes_naive(bytes: &[u8]) -> Result<Box<Self>, QueueError> {
        serde_json::from_slice(bytes).map_err(Into::into)
    }
}
