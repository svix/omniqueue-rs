use async_trait::async_trait;

use redis::{
    cluster::{ClusterClient, ClusterClientBuilder},
    ErrorKind, IntoConnectionInfo, RedisError,
};

/// ConnectionManager that implements `bb8::ManageConnection` and supports asynchronous clustered
/// connections via `redis::cluster::ClusterClient`
#[derive(Clone)]
pub struct RedisClusterConnectionManager {
    client: ClusterClient,
}

impl RedisClusterConnectionManager {
    pub fn new<T: IntoConnectionInfo>(
        info: T,
    ) -> Result<RedisClusterConnectionManager, RedisError> {
        Ok(RedisClusterConnectionManager {
            client: ClusterClientBuilder::new(vec![info]).build()?,
        })
    }
}

#[async_trait]
impl bb8::ManageConnection for RedisClusterConnectionManager {
    type Connection = redis::cluster_async::ClusterConnection;
    type Error = RedisError;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        self.client.get_async_connection().await
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        let pong: String = redis::cmd("PING").query_async(&mut *conn).await?;
        match pong.as_str() {
            "PONG" => Ok(()),
            _ => Err((ErrorKind::ResponseError, "ping request").into()),
        }
    }

    fn has_broken(&self, _: &mut Self::Connection) -> bool {
        false
    }
}
