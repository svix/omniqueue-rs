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
