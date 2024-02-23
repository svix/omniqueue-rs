use std::time::Duration;

use async_trait::async_trait;
use aws_sdk_sqs::types::Message;
use aws_sdk_sqs::{
    operation::delete_message::DeleteMessageError, types::error::ReceiptHandleIsInvalid, Client,
};
use serde::Serialize;

use crate::{
    builder::{QueueBuilder, Static},
    queue::{Acker, Delivery, QueueBackend},
    QueueError, Result,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqsConfig {
    pub queue_dsn: String,
    pub override_endpoint: bool,
}

pub struct SqsBackend;

impl SqsBackend {
    /// Creates a new Amazon SQS queue builder with the given configuration.
    pub fn builder(config: SqsConfig) -> QueueBuilder<Self, Static> {
        QueueBuilder::new(config)
    }
}

impl QueueBackend for SqsBackend {
    type PayloadIn = String;
    type PayloadOut = String;

    type Producer = SqsProducer;
    type Consumer = SqsConsumer;

    type Config = SqsConfig;

    async fn new_pair(cfg: SqsConfig) -> Result<(SqsProducer, SqsConsumer)> {
        let aws_cfg = if cfg.override_endpoint {
            aws_config::from_env()
                .endpoint_url(&cfg.queue_dsn)
                .load()
                .await
        } else {
            aws_config::load_from_env().await
        };

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

    async fn producing_half(cfg: SqsConfig) -> Result<SqsProducer> {
        let aws_cfg = if cfg.override_endpoint {
            aws_config::from_env()
                .endpoint_url(&cfg.queue_dsn)
                .load()
                .await
        } else {
            aws_config::load_from_env().await
        };

        let client = Client::new(&aws_cfg);

        let producer = SqsProducer {
            client,
            queue_dsn: cfg.queue_dsn,
        };

        Ok(producer)
    }

    async fn consuming_half(cfg: SqsConfig) -> Result<SqsConsumer> {
        let aws_cfg = if cfg.override_endpoint {
            aws_config::from_env()
                .endpoint_url(&cfg.queue_dsn)
                .load()
                .await
        } else {
            aws_config::load_from_env().await
        };

        let client = Client::new(&aws_cfg);

        let consumer = SqsConsumer {
            client,
            queue_dsn: cfg.queue_dsn,
        };

        Ok(consumer)
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
                .map_err(QueueError::generic)?;

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
        self.client
            .send_message()
            .queue_url(&self.queue_dsn)
            .message_body(payload)
            .send()
            .await
            .map_err(QueueError::generic)?;

        Ok(())
    }

    pub async fn send_serde_json<P: Serialize + Sync>(&self, payload: &P) -> Result<()> {
        let payload = serde_json::to_string(payload)?;
        self.send_raw(&payload).await
    }

    pub async fn send_raw_scheduled(&self, payload: &str, delay: Duration) -> Result<()> {
        self.client
            .send_message()
            .queue_url(&self.queue_dsn)
            .message_body(payload)
            .delay_seconds(delay.as_secs().try_into().map_err(QueueError::generic)?)
            .send()
            .await
            .map_err(QueueError::generic)?;

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

    pub async fn receive(&mut self) -> Result<Delivery> {
        let out = self
            .client
            .receive_message()
            .set_max_number_of_messages(Some(1))
            .queue_url(&self.queue_dsn)
            .send()
            .await
            .map_err(QueueError::generic)?;

        out.messages()
            .iter()
            .map(|message| -> Result<Delivery> { Ok(self.wrap_message(message)) })
            .next()
            .ok_or(QueueError::NoData)?
    }

    pub async fn receive_all(
        &mut self,
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
            .map_err(QueueError::generic)?;

        out.messages()
            .iter()
            .map(|message| -> Result<Delivery> { Ok(self.wrap_message(message)) })
            .collect::<Result<Vec<_>, _>>()
    }
}

impl_queue_consumer!(SqsConsumer, String);
