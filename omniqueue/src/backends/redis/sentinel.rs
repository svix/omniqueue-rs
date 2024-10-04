use async_trait::async_trait;
use redis::{
    sentinel::{SentinelClient, SentinelNodeConnectionInfo, SentinelServerType},
    ErrorKind, IntoConnectionInfo, RedisError,
};
use tokio::sync::Mutex;

// The mutex here is needed b/c there's currently
// no way to get connections in the redis sentinel client
// without a mutable reference to the underlying client.
struct LockedSentinelClient(pub(crate) Mutex<SentinelClient>);

/// ConnectionManager that implements `bb8::ManageConnection` and supports
/// asynchronous Sentinel connections via `redis::sentinel::SentinelClient`
pub struct RedisSentinelConnectionManager {
    client: LockedSentinelClient,
}

impl RedisSentinelConnectionManager {
    pub fn new<T: IntoConnectionInfo>(
        info: Vec<T>,
        service_name: String,
        node_connection_info: Option<SentinelNodeConnectionInfo>,
    ) -> Result<RedisSentinelConnectionManager, RedisError> {
        Ok(RedisSentinelConnectionManager {
            client: LockedSentinelClient(Mutex::new(SentinelClient::build(
                info,
                service_name,
                node_connection_info,
                SentinelServerType::Master,
            )?)),
        })
    }
}

#[async_trait]
impl bb8::ManageConnection for RedisSentinelConnectionManager {
    type Connection = redis::aio::MultiplexedConnection;
    type Error = RedisError;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        self.client.0.lock().await.get_async_connection().await
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        let pong: String = redis::cmd("PING").query_async(conn).await?;
        match pong.as_str() {
            "PONG" => Ok(()),
            _ => Err((ErrorKind::ResponseError, "ping request").into()),
        }
    }

    fn has_broken(&self, _: &mut Self::Connection) -> bool {
        false
    }
}
