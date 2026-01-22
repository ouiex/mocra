use sea_orm::{Database, DatabaseConnection};

pub fn create_redis_pool(
    host: &String,
    port: u16,
    db: u16,
    username: &Option<String>,
    password: &Option<String>,
) -> Option<deadpool_redis::Pool> {
    let cfg = deadpool_redis::Config {
        connection: Some(deadpool_redis::ConnectionInfo {
            addr: deadpool_redis::ConnectionAddr::Tcp(host.clone(), port),
            redis: deadpool_redis::RedisConnectionInfo {
                db: db as i64,
                username: username.clone(),
                password: password.clone(),
                protocol: deadpool_redis::ProtocolVersion::RESP3,
            },
        }),
        pool: Some(deadpool_redis::PoolConfig {
            max_size: 100,
            ..Default::default()
        }),
        ..Default::default()
    };
    cfg.create_pool(Some(deadpool_redis::Runtime::Tokio1)).ok()
}
pub async fn postgres_connection(
    host: &String,
    port: u16,
    db: &String,
    schema: &String,
    user: &String,
    password: &String,
) -> Option<DatabaseConnection> {
    let pg_url = format!("postgres://{}:{}@{}:{}/{}", user, password, host, port, db);

    let mut db_options = sea_orm::ConnectOptions::new(pg_url);
    db_options
        .set_schema_search_path(schema)
        .sqlx_logging(true)
        // Show SQL statements and bound parameters. TRACE includes bind args.
        .sqlx_logging_level(log::LevelFilter::Trace);

    match Database::connect(db_options).await{
        Ok(db) => Some(db),
        Err(e) => {
            println!("Failed to connect to postgres database: {}", e);
            None
        }
    }
}
