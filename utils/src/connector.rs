use sea_orm::{Database, DatabaseConnection};
use once_cell::sync::Lazy;
use dashmap::DashMap;

// Global cache for connection pools
static CONNECTION_POOLS: Lazy<DashMap<String, DatabaseConnection>> = Lazy::new(|| DashMap::new());

pub fn create_redis_pool(
    host: &str,
    port: u16,
    db: u16,
    username: &Option<String>,
    password: &Option<String>,
    pool_size: Option<usize>,
    tls: bool,
) -> Option<deadpool_redis::Pool> {
    let addr = if tls {
        deadpool_redis::ConnectionAddr::TcpTls {
            host: host.to_string(),
            port,
            insecure: false, // Enforce certificate validation for industrial grade security
        }
    } else {
        deadpool_redis::ConnectionAddr::Tcp(host.to_string(), port)
    };

    let cfg = deadpool_redis::Config {
        connection: Some(deadpool_redis::ConnectionInfo {
            addr,
            redis: deadpool_redis::RedisConnectionInfo {
                db: db as i64,
                username: username.clone(),
                password: password.clone(),
                protocol: deadpool_redis::ProtocolVersion::RESP3,
            },
        }),
        pool: Some(deadpool_redis::PoolConfig {
            max_size: pool_size.unwrap_or(100),
            ..Default::default()
        }),
        ..Default::default()
    };
    cfg.create_pool(Some(deadpool_redis::Runtime::Tokio1)).ok()
}

pub async fn db_connection(
    url: Option<String>,
    schema: Option<String>,
    pool_size: Option<u32>,
    tls: Option<bool>,
) -> Option<DatabaseConnection> {
    let mut final_url = url?;
    if tls.unwrap_or(false) && !final_url.contains("sslmode=") {
        let joiner = if final_url.contains('?') { "&" } else { "?" };
        final_url = format!("{}{}sslmode=require", final_url, joiner);
    }

    let key = final_url.clone();

    if let Some(conn) = CONNECTION_POOLS.get(&key) {
        return Some(conn.clone());
    }

    let mut db_options = sea_orm::ConnectOptions::new(final_url);
    if let Some(s) = schema {
        db_options.set_schema_search_path(s);
    }
    db_options.max_connections(pool_size.unwrap_or(10))
        .sqlx_logging(true)
        .sqlx_logging_level(log::LevelFilter::Trace);

    match Database::connect(db_options).await{
        Ok(db) => {
            CONNECTION_POOLS.insert(key, db.clone());
            Some(db)
        },
        Err(e) => {
            log::error!("Failed to connect to database: {}", e);
            None
        }
    }
}
