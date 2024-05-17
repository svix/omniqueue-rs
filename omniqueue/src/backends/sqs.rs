use std::{
    fmt::{self, Write},
    time::Duration,
};

use async_trait::async_trait;
use aws_sdk_sqs::{
    operation::delete_message::DeleteMessageError,
    types::{error::ReceiptHandleIsInvalid, Message},
    Client,
};
use serde::Serialize;

#[allow(deprecated)]
use crate::{
    builder::{QueueBuilder, Static},
    queue::{Acker, Delivery, QueueBackend},
    QueueError, Result,
};

/// https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/quotas-messages.html
const MAX_PAYLOAD_SIZE: usize = 262_144;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqsConfig {
    /// The queue's [DSN](https://aws.amazon.com/route53/what-is-dns/).
    pub queue_dsn: String,

    /// Whether to override the AWS endpoint URL with the queue DSN.
    pub override_endpoint: bool,
}

#[derive(Clone, Debug)]
pub struct SqsConfigFull {
    queue_dsn: String,
    override_endpoint: bool,
    aws_config: Option<aws_config::SdkConfig>,
}

impl SqsConfigFull {
    async fn take_aws_config(&mut self) -> aws_config::SdkConfig {
        if let Some(cfg) = self.aws_config.take() {
            cfg
        } else if self.override_endpoint {
            aws_config::from_env()
                .endpoint_url(&self.queue_dsn)
                .load()
                .await
        } else {
            aws_config::load_from_env().await
        }
    }
}

#[allow(deprecated)]
impl From<SqsConfig> for SqsConfigFull {
    fn from(cfg: SqsConfig) -> Self {
        let SqsConfig {
            queue_dsn,
            override_endpoint,
        } = cfg;
        Self {
            queue_dsn,
            override_endpoint,
            aws_config: None,
        }
    }
}

impl From<&str> for SqsConfigFull {
    fn from(dsn: &str) -> Self {
        Self::from(dsn.to_owned())
    }
}

impl From<String> for SqsConfigFull {
    fn from(dsn: String) -> Self {
        Self {
            queue_dsn: dsn,
            override_endpoint: false,
            aws_config: None,
        }
    }
}

pub struct SqsBackend;

#[allow(deprecated)]
impl SqsBackend {
    /// Creates a new Amazon SQS queue builder with the given configuration.
    ///
    /// You can pass either a queue DSN, or a [`SqsConfig`] instance here.
    pub fn builder(cfg: impl Into<SqsConfigFull>) -> QueueBuilder<Self, Static> {
        QueueBuilder::new(cfg.into())
    }

    #[deprecated = "Use SqsBackend::builder(cfg).build_pair() instead"]
    pub async fn new_pair(cfg: impl Into<SqsConfigFull>) -> Result<(SqsProducer, SqsConsumer)> {
        <Self as QueueBackend>::new_pair(cfg.into()).await
    }

    #[deprecated = "Use SqsBackend::builder(cfg).build_producer() instead"]
    pub async fn producing_half(cfg: impl Into<SqsConfigFull>) -> Result<SqsProducer> {
        <Self as QueueBackend>::producing_half(cfg.into()).await
    }

    #[deprecated = "Use SqsBackend::builder(cfg).build_consumer() instead"]
    pub async fn consuming_half(cfg: impl Into<SqsConfigFull>) -> Result<SqsConsumer> {
        <Self as QueueBackend>::consuming_half(cfg.into()).await
    }
}

#[allow(deprecated)]
impl QueueBackend for SqsBackend {
    type PayloadIn = String;
    type PayloadOut = String;

    type Producer = SqsProducer;
    type Consumer = SqsConsumer;

    type Config = SqsConfigFull;

    async fn new_pair(mut cfg: SqsConfigFull) -> Result<(SqsProducer, SqsConsumer)> {
        let aws_cfg = cfg.take_aws_config().await;
        let client = Client::new(&aws_cfg);

        let producer = SqsProducer {
            client: client.clone(),
            queue_dsn: cfg.queue_dsn.clone(),
        };

        let consumer = SqsConsumer {
            client,
            queue_dsn: cfg.queue_dsn,
        };

        Ok((producer, consumer))
    }

    async fn producing_half(mut cfg: SqsConfigFull) -> Result<SqsProducer> {
        let aws_cfg = cfg.take_aws_config().await;
        let client = Client::new(&aws_cfg);

        let producer = SqsProducer {
            client,
            queue_dsn: cfg.queue_dsn,
        };

        Ok(producer)
    }

    async fn consuming_half(mut cfg: SqsConfigFull) -> Result<SqsConsumer> {
        let aws_cfg = cfg.take_aws_config().await;
        let client = Client::new(&aws_cfg);

        let consumer = SqsConsumer {
            client,
            queue_dsn: cfg.queue_dsn,
        };

        Ok(consumer)
    }
}

impl QueueBuilder<SqsBackend> {
    /// Set the AWS configuration to use.
    ///
    /// If you _don't_ call this method, the AWS configuration will be loaded
    /// from the process environment, via [`aws_config::load_from_env`].
    pub fn aws_config(mut self, value: aws_config::SdkConfig) -> Self {
        self.config.aws_config = Some(value);
        self
    }

    /// Configure whether to override the AWS endpoint URL with the queue DSN.
    pub fn override_endpoint(mut self, value: bool) -> Self {
        self.config.override_endpoint = value;
        self
    }
}

struct SqsAcker {
    ack_client: Client,
    // FIXME: Cow/Arc this stuff?
    queue_dsn: String,
    receipt_handle: Option<String>,

    has_been_acked_or_nacked: bool,
}

#[async_trait]
impl Acker for SqsAcker {
    async fn ack(&mut self) -> Result<()> {
        if self.has_been_acked_or_nacked {
            return Err(QueueError::CannotAckOrNackTwice);
        }

        if let Some(receipt_handle) = &self.receipt_handle {
            self.ack_client
                .delete_message()
                .queue_url(&self.queue_dsn)
                .receipt_handle(receipt_handle)
                .send()
                .await
                .map_err(aws_to_queue_error)?;

            self.has_been_acked_or_nacked = true;

            Ok(())
        } else {
            self.has_been_acked_or_nacked = true;

            Err(QueueError::generic(
                DeleteMessageError::ReceiptHandleIsInvalid(
                    ReceiptHandleIsInvalid::builder()
                        .message("receipt handle must be Some to be acked")
                        .build(),
                ),
            ))
        }
    }

    async fn nack(&mut self) -> Result<()> {
        Ok(())
    }
}

pub struct SqsProducer {
    client: Client,
    queue_dsn: String,
}

impl SqsProducer {
    pub async fn send_raw(&self, payload: &str) -> Result<()> {
        self.send_raw_scheduled(payload, Duration::ZERO).await
    }

    pub async fn send_serde_json<P: Serialize + Sync>(&self, payload: &P) -> Result<()> {
        let payload = serde_json::to_string(payload)?;
        self.send_raw(&payload).await
    }

    #[tracing::instrument(
        name = "send",
        skip_all,
        fields(
            payload_size = payload.len(),
            delay = (delay > Duration::ZERO).then(|| tracing::field::debug(delay))
        )
    )]
    pub async fn send_raw_scheduled(&self, payload: &str, delay: Duration) -> Result<()> {
        if payload.len() > MAX_PAYLOAD_SIZE {
            return Err(QueueError::PayloadTooLarge {
                limit: MAX_PAYLOAD_SIZE,
                actual: payload.len(),
            });
        }

        self.client
            .send_message()
            .queue_url(&self.queue_dsn)
            .message_body(payload)
            .delay_seconds(delay.as_secs().try_into().map_err(QueueError::generic)?)
            .send()
            .await
            .map_err(aws_to_queue_error)?;

        Ok(())
    }

    pub async fn send_serde_json_scheduled<P: Serialize + Sync>(
        &self,
        payload: &P,
        delay: Duration,
    ) -> Result<()> {
        let payload = serde_json::to_string(payload)?;
        self.send_raw_scheduled(&payload, delay).await
    }
}

impl_queue_producer!(SqsProducer, String);
impl_scheduled_queue_producer!(SqsProducer, String);

pub struct SqsConsumer {
    client: Client,
    queue_dsn: String,
}

impl SqsConsumer {
    fn wrap_message(&self, message: &Message) -> Delivery {
        Delivery {
            acker: Box::new(SqsAcker {
                ack_client: self.client.clone(),
                queue_dsn: self.queue_dsn.clone(),
                receipt_handle: message.receipt_handle().map(ToOwned::to_owned),
                has_been_acked_or_nacked: false,
            }),
            payload: Some(message.body().unwrap_or_default().as_bytes().to_owned()),
        }
    }

    pub async fn receive(&self) -> Result<Delivery> {
        let out = self
            .client
            .receive_message()
            .set_max_number_of_messages(Some(1))
            .queue_url(&self.queue_dsn)
            .send()
            .await
            .map_err(aws_to_queue_error)?;

        out.messages()
            .iter()
            .map(|message| -> Result<Delivery> { Ok(self.wrap_message(message)) })
            .next()
            .ok_or(QueueError::NoData)?
    }

    pub async fn receive_all(
        &self,
        max_messages: usize,
        deadline: Duration,
    ) -> Result<Vec<Delivery>> {
        let out = self
            .client
            .receive_message()
            .set_wait_time_seconds(Some(
                deadline.as_secs().try_into().map_err(QueueError::generic)?,
            ))
            .set_max_number_of_messages(Some(max_messages.try_into().map_err(QueueError::generic)?))
            .queue_url(&self.queue_dsn)
            .send()
            .await
            .map_err(aws_to_queue_error)?;

        out.messages()
            .iter()
            .map(|message| -> Result<Delivery> { Ok(self.wrap_message(message)) })
            .collect::<Result<Vec<_>, _>>()
    }
}

impl_queue_consumer!(SqsConsumer, String);

fn aws_to_queue_error<E>(err: aws_sdk_sqs::error::SdkError<E>) -> QueueError
where
    E: std::error::Error + 'static,
{
    let mut message = String::new();
    write_err(&mut message, &err).expect("Write to string never fails");
    QueueError::Generic(message.into())
}

fn write_err(s: &mut String, err: &dyn std::error::Error) -> fmt::Result {
    write!(s, "{err}")?;
    if let Some(source) = err.source() {
        write!(s, ": ")?;
        write_err(s, source)?;
    }

    Ok(())
}
