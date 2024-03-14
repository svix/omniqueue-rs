use std::time::Duration;

use async_trait::async_trait;
use azure_storage::StorageCredentials;
use azure_storage_queues::{
    operations::Message, PopReceipt, QueueClient, QueueServiceClientBuilder,
};
use serde::Serialize;

use crate::{queue::Acker, Delivery, QueueBackend, QueueError, Result};

fn get_client(cfg: &AqsQueueConfig) -> QueueClient {
    let storage_credentials =
        StorageCredentials::access_key(cfg.storage_account.clone(), cfg.access_key.clone());

    let mut builder =
        QueueServiceClientBuilder::new(cfg.storage_account.clone(), storage_credentials);
    if let Some(cloud_uri) = cfg.cloud_uri.clone() {
        builder = builder.cloud_location(azure_storage::CloudLocation::Custom {
            account: cfg.storage_account.clone(),
            uri: cloud_uri,
        });
    }
    builder.build().queue_client(cfg.queue_name.clone())
}

pub struct AqsQueueBackend;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AqsQueueConfig {
    pub queue_name: String,
    pub empty_receive_delay: std::time::Duration,
    pub message_ttl: std::time::Duration,
    pub storage_account: String,
    pub access_key: String,
    pub cloud_uri: Option<String>,
}

impl QueueBackend for AqsQueueBackend {
    type Config = AqsQueueConfig;

    type PayloadIn = String;
    type PayloadOut = String;

    type Producer = AqsQueueProducer;
    type Consumer = AqsQueueConsumer;

    async fn new_pair(config: Self::Config) -> Result<(AqsQueueProducer, AqsQueueConsumer)> {
        let client = get_client(&config);
        Ok((
            AqsQueueProducer {
                client: client.clone(),
                config: config.clone(),
            },
            AqsQueueConsumer {
                client: client.clone(),
                config: config.clone(),
            },
        ))
    }

    async fn producing_half(config: Self::Config) -> Result<AqsQueueProducer> {
        let client = get_client(&config);
        Ok(AqsQueueProducer { client, config })
    }

    async fn consuming_half(config: Self::Config) -> Result<AqsQueueConsumer> {
        let client = get_client(&config);
        Ok(AqsQueueConsumer { client, config })
    }
}

pub struct AqsQueueProducer {
    client: QueueClient,
    config: AqsQueueConfig,
}

impl AqsQueueProducer {
    pub async fn send_raw(&self, payload: &str) -> Result<()> {
        self.send_raw_scheduled(payload, Duration::ZERO).await
    }

    pub async fn send_raw_scheduled(&self, payload: &str, delay: Duration) -> Result<()> {
        self.client
            .put_message(payload)
            .visibility_timeout(delay)
            .ttl(self.config.message_ttl)
            .await
            .map_err(QueueError::generic)
            .map(|_| ())
    }

    pub async fn send_serde_json<P: Serialize + Sync>(&self, payload: &P) -> Result<()> {
        let payload = serde_json::to_string(payload)?;
        self.send_raw(&payload).await
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

impl_queue_producer!(AqsQueueProducer, String);
impl_scheduled_queue_producer!(AqsQueueProducer, String);

pub struct AqsQueueConsumer {
    client: QueueClient,
    config: AqsQueueConfig,
}

struct AqsAcker {
    client: QueueClient,
    already_acked_or_nacked: bool,
    pop_receipt: PopReceipt,
}

#[async_trait]
impl Acker for AqsAcker {
    async fn ack(&mut self) -> Result<()> {
        if self.already_acked_or_nacked {
            return Err(QueueError::CannotAckOrNackTwice);
        }
        self.already_acked_or_nacked = true;
        self.client
            .pop_receipt_client(self.pop_receipt.clone())
            .delete()
            .await
            .map_err(QueueError::generic)
            .map(|_| ())
    }

    async fn nack(&mut self) -> Result<()> {
        Ok(())
    }
}

impl AqsQueueConsumer {
    fn wrap_message(&self, message: &Message) -> Delivery {
        Delivery {
            acker: Box::new(AqsAcker {
                client: self.client.clone(),
                pop_receipt: message.pop_receipt(),
                already_acked_or_nacked: false,
            }),
            payload: Some(message.message_text.as_bytes().to_owned()),
        }
    }

    /// Note that blocking receives are not supported by Azure Queue Storage
    /// and will return immediately if no messages are available for delivery
    /// in the queue.
    pub async fn receive(&mut self) -> Result<Delivery> {
        self.client
            .get_messages()
            .await
            .map_err(QueueError::generic)
            .and_then(|m| m.messages.into_iter().next().ok_or(QueueError::NoData))
            .map(|m| self.wrap_message(&m))
    }

    pub async fn receive_all(
        &mut self,
        max_messages: usize,
        deadline: Duration,
    ) -> Result<Vec<Delivery>> {
        let end = std::time::Instant::now() + deadline;
        let mut interval = tokio::time::interval(self.config.empty_receive_delay);
        loop {
            interval.tick().await;
            let msgs = self
                .client
                .get_messages()
                .number_of_messages(max_messages.try_into().unwrap_or(u8::MAX))
                .await
                .map_err(QueueError::generic)
                .map(|m| {
                    m.messages
                        .iter()
                        .map(|m| self.wrap_message(m))
                        .collect::<Vec<_>>()
                })?;
            if !msgs.is_empty() {
                return Ok(msgs);
            }
            if std::time::Instant::now() > end {
                return Ok(vec![]);
            }
        }
    }
}

impl_queue_consumer!(AqsQueueConsumer, String);
