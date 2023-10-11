use std::time::Duration;
use std::{any::TypeId, collections::HashMap, sync::Arc};

use async_trait::async_trait;
use aws_sdk_sqs::types::Message;
use aws_sdk_sqs::{
    operation::delete_message::DeleteMessageError, types::error::ReceiptHandleIsInvalid, Client,
};

use crate::{
    decoding::{CustomDecoder, CustomDecoderStandardized, DecoderRegistry},
    encoding::{CustomEncoder, EncoderRegistry},
    queue::{consumer::QueueConsumer, producer::QueueProducer, Acker, Delivery, QueueBackend},
    scheduled::ScheduledProducer,
    QueueError,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqsConfig {
    pub queue_dsn: String,
    pub override_endpoint: bool,
}

pub struct SqsQueueBackend;

#[async_trait]
impl QueueBackend for SqsQueueBackend {
    type Config = SqsConfig;

    type PayloadIn = String;
    type PayloadOut = String;

    type Producer = SqsQueueProducer;
    type Consumer = SqsQueueConsumer;

    async fn new_pair(
        cfg: SqsConfig,
        custom_encoders: EncoderRegistry<String>,
        custom_decoders: DecoderRegistry<String>,
    ) -> Result<(SqsQueueProducer, SqsQueueConsumer), QueueError> {
        let aws_cfg = if cfg.override_endpoint {
            aws_config::from_env()
                .endpoint_url(&cfg.queue_dsn)
                .load()
                .await
        } else {
            aws_config::load_from_env().await
        };

        let client = Client::new(&aws_cfg);

        let producer = SqsQueueProducer {
            registry: custom_encoders,
            client: client.clone(),
            queue_dsn: cfg.queue_dsn.clone(),
        };

        let byte_decoders = Arc::new(
            custom_decoders
                .iter()
                .map(|(k, v)| {
                    (
                        *k,
                        Arc::new(CustomDecoderStandardized::from_decoder(
                            v.clone(),
                            |b: &Vec<u8>| String::from_utf8(b.clone()).map_err(QueueError::generic),
                        )) as Arc<dyn CustomDecoder<Vec<u8>>>,
                    )
                })
                .collect(),
        );

        let consumer = SqsQueueConsumer {
            bytes_registry: byte_decoders,
            client,
            queue_dsn: cfg.queue_dsn,
        };

        Ok((producer, consumer))
    }

    async fn producing_half(
        cfg: SqsConfig,
        custom_encoders: EncoderRegistry<String>,
    ) -> Result<SqsQueueProducer, QueueError> {
        let aws_cfg = if cfg.override_endpoint {
            aws_config::from_env()
                .endpoint_url(&cfg.queue_dsn)
                .load()
                .await
        } else {
            aws_config::load_from_env().await
        };

        let client = Client::new(&aws_cfg);

        let producer = SqsQueueProducer {
            registry: custom_encoders,
            client,
            queue_dsn: cfg.queue_dsn,
        };

        Ok(producer)
    }

    async fn consuming_half(
        cfg: SqsConfig,
        custom_decoders: DecoderRegistry<String>,
    ) -> Result<SqsQueueConsumer, QueueError> {
        let aws_cfg = if cfg.override_endpoint {
            aws_config::from_env()
                .endpoint_url(&cfg.queue_dsn)
                .load()
                .await
        } else {
            aws_config::load_from_env().await
        };

        let client = Client::new(&aws_cfg);

        let byte_decoders = Arc::new(
            custom_decoders
                .iter()
                .map(|(k, v)| {
                    (
                        *k,
                        Arc::new(CustomDecoderStandardized::from_decoder(
                            v.clone(),
                            |b: &Vec<u8>| String::from_utf8(b.clone()).map_err(QueueError::generic),
                        )) as Arc<dyn CustomDecoder<Vec<u8>>>,
                    )
                })
                .collect(),
        );

        let consumer = SqsQueueConsumer {
            bytes_registry: byte_decoders,
            client,
            queue_dsn: cfg.queue_dsn,
        };

        Ok(consumer)
    }
}

pub struct SqsAcker {
    ack_client: Client,
    // FIXME: Cow/Arc this stuff?
    queue_dsn: String,
    receipt_handle: Option<String>,

    has_been_acked_or_nacked: bool,
}

#[async_trait]
impl Acker for SqsAcker {
    async fn ack(&mut self) -> Result<(), QueueError> {
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

    async fn nack(&mut self) -> Result<(), QueueError> {
        Ok(())
    }
}

pub struct SqsQueueProducer {
    registry: Arc<HashMap<TypeId, Box<dyn CustomEncoder<String>>>>,
    client: Client,
    queue_dsn: String,
}

#[async_trait]
impl QueueProducer for SqsQueueProducer {
    type Payload = String;

    fn get_custom_encoders(&self) -> &HashMap<TypeId, Box<dyn CustomEncoder<Self::Payload>>> {
        self.registry.as_ref()
    }

    async fn send_raw(&self, payload: &String) -> Result<(), QueueError> {
        self.client
            .send_message()
            .queue_url(&self.queue_dsn)
            .message_body(payload)
            .send()
            .await
            .map_err(QueueError::generic)?;

        Ok(())
    }
}

#[async_trait]
impl ScheduledProducer for SqsQueueProducer {
    async fn send_raw_scheduled(
        &self,
        payload: &Self::Payload,
        delay: std::time::Duration,
    ) -> Result<(), QueueError> {
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
}

pub struct SqsQueueConsumer {
    bytes_registry: DecoderRegistry<Vec<u8>>,
    client: Client,
    queue_dsn: String,
}

impl SqsQueueConsumer {
    fn wrap_message(&self, message: &Message) -> Delivery {
        Delivery {
            decoders: self.bytes_registry.clone(),
            acker: Box::new(SqsAcker {
                ack_client: self.client.clone(),
                queue_dsn: self.queue_dsn.clone(),
                receipt_handle: message.receipt_handle().map(ToOwned::to_owned),
                has_been_acked_or_nacked: false,
            }),
            payload: Some(message.body().unwrap_or_default().as_bytes().to_owned()),
        }
    }
}

#[async_trait]
impl QueueConsumer for SqsQueueConsumer {
    type Payload = String;

    async fn receive(&mut self) -> Result<Delivery, QueueError> {
        let out = self
            .client
            .receive_message()
            .set_max_number_of_messages(Some(1))
            .queue_url(&self.queue_dsn)
            .send()
            .await
            .map_err(QueueError::generic)?;

        out.messages()
            .unwrap_or_default()
            .iter()
            .map(|message| -> Result<Delivery, QueueError> { Ok(self.wrap_message(message)) })
            .next()
            .ok_or(QueueError::NoData)?
    }

    async fn receive_all(
        &mut self,
        max_messages: usize,
        deadline: Duration,
    ) -> Result<Vec<Delivery>, QueueError> {
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

        Ok(out
            .messages()
            .unwrap_or_default()
            .iter()
            .map(|message| -> Result<Delivery, QueueError> { Ok(self.wrap_message(message)) })
            .collect::<Result<Vec<_>, _>>()?)
    }
}
