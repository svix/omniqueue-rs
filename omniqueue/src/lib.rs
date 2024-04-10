//! # Omniqueue
//!
//! Omniqueue provides a high-level interface for sending and receiving the
//! following over a range of queue backends:
//!
//!   * Raw byte arrays in the way most compatible with the queue backend
//!   * JSON encoded byte arrays for types that implement [`serde::Deserialize`]
//!     and [`serde::Serialize`]
//!
//! ## Cargo Features
//!
//! Each backend is enabled with its associated cargo feature. All backends are
//! enabled by default. As of present it supports:
//!
//! * In-memory queue
//! * Google Cloud Pub/Sub
//! * RabbitMQ
//! * Redis
//! * Amazon SQS
//! * Azure Queue Storage
//!
//! ## How to Use Omniqueue
//!
//! Each queue backend has a unique configuration type. One of these
//! configurations is taken when constructing the [`QueueBuilder`].
//!
//! To create a simple producer and/or consumer:
//!
//! ```no_run
//! # async {
//! use omniqueue::backends::{SqsConfig, SqsBackend};
//!
//! let cfg = SqsConfig {
//!     queue_dsn: "http://localhost:9234/queue/queue_name".to_owned(),
//!     override_endpoint: true,
//! };
//!
//! // Either both producer and consumer
//! let (p, mut c) = SqsBackend::builder(cfg.clone()).build_pair().await?;
//!
//! // Or one half
//! let p = SqsBackend::builder(cfg.clone()).build_producer().await?;
//! let mut c = SqsBackend::builder(cfg).build_consumer().await?;
//! # anyhow::Ok(())
//! # };
//! ```
//!
//! Sending and receiving information from this queue is simple:
//!
//! ```no_run
//! # use omniqueue::backends::SqsBackend;
//! # async {
//! # #[derive(Default, serde::Deserialize, serde::Serialize)]
//! # struct ExampleType;
//! #
//! # let (p, mut c) = SqsBackend::builder("<dsn>").build_pair().await?;
//! p.send_serde_json(&ExampleType::default()).await?;
//!
//! let delivery = c.receive().await?;
//! let payload = delivery.payload_serde_json::<ExampleType>()?;
//! delivery.ack().await.map_err(|(e, _)| e)?;
//! # anyhow::Ok(())
//! # };
//! ```
//!
//! ## `DynProducer`s and `DynConsumer`s
//!
//! Dynamic-dispatch can be used easily for when you're not sure which backend
//! to use at compile-time.
//!
//! Making a `DynProducer` or `DynConsumer` is as simple as adding one line to
//! the builder:
//!
//! ```no_run
//! # async {
//! # let cfg = todo!();
//! use omniqueue::backends::RabbitMqBackend;
//!
//! let (p, mut c) = RabbitMqBackend::builder(cfg)
//!     .make_dynamic()
//!     .build_pair()
//!     .await?;
//! # anyhow::Ok(())
//! # };
//! ```
#![warn(unreachable_pub)]

use std::fmt::Debug;

use thiserror::Error;

#[macro_use]
mod macros;

pub mod backends;
pub mod builder;
mod queue;
mod scheduled;

pub use self::{
    builder::QueueBuilder,
    queue::{Delivery, DynConsumer, DynProducer, QueueBackend, QueueConsumer, QueueProducer},
    scheduled::{DynScheduledProducer, ScheduledQueueProducer},
};

/// Type alias for std's `Result` with the error type defaulting to omniqueue's
/// `QueueError`.
pub type Result<T, E = QueueError> = std::result::Result<T, E>;

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

    #[error("{0}")]
    Generic(Box<dyn std::error::Error + Send + Sync>),

    #[error("{0}")]
    #[deprecated = "This variant is never created inside omniqueue"]
    Unsupported(&'static str),
}

impl QueueError {
    pub fn generic<E: std::error::Error + Send + Sync + 'static>(e: E) -> Self {
        Self::Generic(Box::new(e))
    }
}

pub trait QueuePayload: Send + Sync + 'static {
    fn to_bytes_naive(&self) -> Result<Vec<u8>>;
    fn from_bytes_naive(bytes: &[u8]) -> Result<Box<Self>>;
}

impl QueuePayload for Vec<u8> {
    fn to_bytes_naive(&self) -> Result<Vec<u8>> {
        Ok(self.clone())
    }

    fn from_bytes_naive(bytes: &[u8]) -> Result<Box<Self>> {
        Ok(Box::new(bytes.to_owned()))
    }
}

impl QueuePayload for String {
    fn to_bytes_naive(&self) -> Result<Vec<u8>> {
        Ok(self.as_bytes().to_owned())
    }

    fn from_bytes_naive(bytes: &[u8]) -> Result<Box<Self>> {
        Ok(Box::new(
            String::from_utf8(bytes.to_owned()).map_err(QueueError::generic)?,
        ))
    }
}

impl QueuePayload for serde_json::Value {
    fn to_bytes_naive(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(self).map_err(Into::into)
    }

    fn from_bytes_naive(bytes: &[u8]) -> Result<Box<Self>> {
        serde_json::from_slice(bytes).map_err(Into::into)
    }
}
