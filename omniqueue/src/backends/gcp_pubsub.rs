use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use futures_util::{future::try_join_all, StreamExt};
use google_cloud_googleapis::pubsub::v1::PubsubMessage;
use google_cloud_pubsub::{
    client::{google_cloud_auth::credentials::CredentialsFile, Client, ClientConfig},
    publisher::Publisher,
    subscriber::ReceivedMessage,
    subscription::Subscription,
};
use serde::Serialize;

#[allow(deprecated)]
use crate::{
    builder::{QueueBuilder, Static},
    queue::{Acker, Delivery, QueueBackend},
    QueueError, Result,
};

pub struct GcpPubSubBackend;

impl GcpPubSubBackend {
    /// Creates a new Google Cloud Pub/Sub queue builder with the given
    /// configuration.
    pub fn builder(config: GcpPubSubConfig) -> QueueBuilder<Self, Static> {
        #[allow(deprecated)]
        QueueBuilder::new(config)
    }
}

type Payload = Vec<u8>;

// FIXME: topic/subscription are each for read/write. Split config up?
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GcpPubSubConfig {
    pub topic_id: String,
    pub subscription_id: String,
    pub credentials_file: Option<PathBuf>,
}

/// Make a `ClientConfig` from a `CredentialsFile` on disk.
async fn configure_client_from_file<P: AsRef<Path>>(cred_file_path: P) -> Result<ClientConfig> {
    let bytes = std::fs::read(cred_file_path).map_err(QueueError::generic)?;
    let creds: CredentialsFile = serde_json::from_slice(&bytes).map_err(QueueError::generic)?;
    ClientConfig::default()
        .with_credentials(creds)
        .await
        .map_err(QueueError::generic)
}

/// Making a `ClientConfig` via env vars is possible in two ways:
/// - setting `GOOGLE_APPLICATION_CREDENTIALS` to the file path to have it
///   loaded automatically
/// - setting `GOOGLE_APPLICATION_CREDENTIALS_JSON` to the file contents
///   (avoiding the need for a file on disk).
async fn configure_client_from_env() -> Result<ClientConfig> {
    ClientConfig::default()
        .with_auth()
        .await
        .map_err(QueueError::generic)
}

async fn get_client(cfg: &GcpPubSubConfig) -> Result<Client> {
    let config = {
        if let Some(fp) = &cfg.credentials_file {
            tracing::trace!("reading gcp creds from file: {}", fp.display());
            configure_client_from_file(&fp).await?
        } else {
            tracing::trace!("reading gcp creds from env");
            configure_client_from_env().await?
        }
    };
    Client::new(config).await.map_err(QueueError::generic)
}

#[allow(deprecated)]
impl QueueBackend for GcpPubSubBackend {
    type Config = GcpPubSubConfig;

    type PayloadIn = Payload;
    type PayloadOut = Payload;

    type Producer = GcpPubSubProducer;
    type Consumer = GcpPubSubConsumer;

    async fn new_pair(config: Self::Config) -> Result<(GcpPubSubProducer, GcpPubSubConsumer)> {
        let client = get_client(&config).await?;
        Ok((
            GcpPubSubProducer::new(client.clone(), config.topic_id).await?,
            GcpPubSubConsumer::new(client, config.subscription_id).await?,
        ))
    }

    async fn producing_half(config: Self::Config) -> Result<GcpPubSubProducer> {
        let client = get_client(&config).await?;
        GcpPubSubProducer::new(client, config.topic_id).await
    }

    async fn consuming_half(config: Self::Config) -> Result<GcpPubSubConsumer> {
        let client = get_client(&config).await?;
        GcpPubSubConsumer::new(client, config.subscription_id).await
    }
}

pub struct GcpPubSubProducer {
    client: Client,
    topic_id: Arc<String>,
}

impl GcpPubSubProducer {
    async fn new(client: Client, topic_id: String) -> Result<Self> {
        let topic = client.topic(&topic_id);
        // Only warn if the topic doesn't exist at this point.
        // If it gets created after the fact, we should be able to still use it
        // when available, otherwise if it's still missing at that time, error.
        if !topic.exists(None).await.map_err(QueueError::generic)? {
            tracing::warn!("topic {} does not exist", &topic_id);
        }
        Ok(Self {
            client,
            topic_id: Arc::new(topic_id),
        })
    }

    async fn publisher(&self) -> Result<Publisher> {
        // N.b. defer the creation of a publisher/topic until needed. Helps recover when
        // the topic does not yet exist, but will soon.
        // Might be more expensive to recreate each time, but overall more reliable.
        let topic = self.client.topic(&self.topic_id);

        // Publishing to a non-existent topic will cause the publisher to wait
        // (forever?) Giving this error will allow dependents to handle the
        // error case immediately when this happens, instead of holding the
        // connection open indefinitely.
        if !topic.exists(None).await.map_err(QueueError::generic)? {
            return Err(QueueError::Generic(
                format!("topic {} does not exist", &self.topic_id).into(),
            ));
        }

        // FIXME: may need to expose `PublisherConfig` to caller so they can tweak this
        Ok(topic.new_publisher(None))
    }

    #[tracing::instrument(
        name = "send",
        skip_all,
        fields(payload_size = payload.len())
    )]
    pub async fn send_raw(&self, payload: &[u8]) -> Result<()> {
        let msg = PubsubMessage {
            data: payload.to_vec(),
            ..Default::default()
        };

        let publisher = self.publisher().await?;
        let awaiter = publisher.publish(msg).await;
        awaiter.get().await.map_err(QueueError::generic)?;
        Ok(())
    }

    pub async fn send_serde_json<P: Serialize + Sync>(&self, payload: &P) -> Result<()> {
        self.send_raw(&serde_json::to_vec(&payload)?).await
    }
}

impl std::fmt::Debug for GcpPubSubProducer {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("GcpPubSubProducer")
            .field("topic_id", &self.topic_id)
            .finish()
    }
}

impl crate::QueueProducer for GcpPubSubProducer {
    type Payload = Payload;
    omni_delegate!(send_raw, send_serde_json);

    /// This method is overwritten for the Google Cloud Pub/Sub backend to be
    /// more efficient than the default of sequentially publishing `payloads`.
    #[tracing::instrument(name = "send_batch", skip_all)]
    async fn send_raw_batch(
        &self,
        payloads: impl IntoIterator<Item: AsRef<Self::Payload> + Send, IntoIter: Send> + Send,
    ) -> Result<()> {
        let msgs = payloads
            .into_iter()
            .map(|payload| PubsubMessage {
                data: payload.as_ref().to_vec(),
                ..Default::default()
            })
            .collect();

        let publisher = self.publisher().await?;
        let awaiters = publisher.publish_bulk(msgs).await;
        try_join_all(awaiters.into_iter().map(|a| a.get()))
            .await
            .map_err(QueueError::generic)?;
        Ok(())
    }

    /// This method is overwritten for the Google Cloud Pub/Sub backend to be
    /// more efficient than the default of sequentially publishing `payloads`.
    #[tracing::instrument(name = "send_batch", skip_all)]
    async fn send_serde_json_batch(
        &self,
        payloads: impl IntoIterator<Item: Serialize + Send, IntoIter: Send> + Send,
    ) -> Result<()> {
        let msgs = payloads
            .into_iter()
            .map(|payload| {
                Ok(PubsubMessage {
                    data: serde_json::to_vec(&payload)?,
                    ..Default::default()
                })
            })
            .collect::<Result<_>>()?;

        let publisher = self.publisher().await?;
        let awaiters = publisher.publish_bulk(msgs).await;
        try_join_all(awaiters.into_iter().map(|a| a.get()))
            .await
            .map_err(QueueError::generic)?;
        Ok(())
    }
}

pub struct GcpPubSubConsumer {
    client: Client,
    subscription_id: Arc<String>,
}

impl GcpPubSubConsumer {
    async fn new(client: Client, subscription_id: String) -> Result<Self> {
        Ok(Self {
            client,
            subscription_id: Arc::new(subscription_id),
        })
    }

    pub async fn receive(&mut self) -> Result<Delivery> {
        let subscription = subscription(&self.client, &self.subscription_id).await?;
        let mut stream = subscription
            .subscribe(None)
            .await
            .map_err(QueueError::generic)?;

        let recv_msg = stream.next().await.ok_or_else(|| QueueError::NoData)?;

        Ok(self.wrap_recv_msg(recv_msg))
    }

    pub async fn receive_all(
        &mut self,
        max_messages: usize,
        deadline: Duration,
    ) -> Result<Vec<Delivery>> {
        let subscription = subscription(&self.client, &self.subscription_id).await?;
        match tokio::time::timeout(deadline, subscription.pull(max_messages as _, None)).await {
            Ok(messages) => Ok(messages
                .map_err(QueueError::generic)?
                .into_iter()
                .map(|m| self.wrap_recv_msg(m))
                .collect()),
            // Timeout
            Err(_) => Ok(vec![]),
        }
    }

    fn wrap_recv_msg(&self, mut recv_msg: ReceivedMessage) -> Delivery {
        // FIXME: would be nice to avoid having to move the data out here.
        // While it's possible to ack via a subscription and an ack_id, nack
        // is only possible via a `ReceiveMessage`. This means we either need
        // to hold 2 copies of the payload, or move the bytes out so they can be
        // returned _outside of the Acker_.
        let payload = recv_msg.message.data.drain(..).collect();

        Delivery::new(
            payload,
            GcpPubSubAcker {
                recv_msg,
                subscription_id: self.subscription_id.clone(),
            },
        )
    }
}

impl std::fmt::Debug for GcpPubSubConsumer {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("GcpPubSubConsumer")
            .field("subscription_id", &self.subscription_id)
            .finish()
    }
}

async fn subscription(client: &Client, subscription_id: &str) -> Result<Subscription> {
    let subscription = client.subscription(subscription_id);
    if !subscription
        .exists(None)
        .await
        .map_err(QueueError::generic)?
    {
        return Err(QueueError::Generic(
            format!("subscription {} does not exist", &subscription_id).into(),
        ));
    }
    Ok(subscription)
}

impl crate::QueueConsumer for GcpPubSubConsumer {
    type Payload = Payload;
    omni_delegate!(receive, receive_all);
}

struct GcpPubSubAcker {
    recv_msg: ReceivedMessage,
    subscription_id: Arc<String>,
}

impl std::fmt::Debug for GcpPubSubAcker {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("GcpPubSubAcker")
            .field("ack_id", &self.recv_msg.ack_id())
            .field("message_id", &self.recv_msg.message.message_id)
            .field("subscription_id", &self.subscription_id)
            .finish()
    }
}

impl Acker for GcpPubSubAcker {
    async fn ack(&mut self) -> Result<()> {
        self.recv_msg.ack().await.map_err(QueueError::generic)
    }

    async fn nack(&mut self) -> Result<()> {
        self.recv_msg.nack().await.map_err(QueueError::generic)
    }

    async fn set_ack_deadline(&mut self, duration: Duration) -> Result<()> {
        let duration_secs = duration.as_secs().try_into().map_err(|e| {
            QueueError::Generic(Box::<dyn std::error::Error + Send + Sync>::from(format!(
                "set_ack_deadline duration {duration:?} is too large: {e:?}"
            )))
        })?;

        self.recv_msg
            .modify_ack_deadline(duration_secs)
            .await
            .map_err(QueueError::generic)
    }
}
