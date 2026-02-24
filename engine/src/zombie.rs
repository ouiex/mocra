use std::sync::Arc;
use tokio::time::{Duration, sleep};
use log::{info, warn, error};
use common::state::State;
use sea_orm::{ConnectionTrait, Statement};
use chrono::{Utc, DateTime};

/// Starts a periodic cleaner for stale `Running` tasks.
///
/// Every minute, the cleaner marks rows as `Failed` when `updated_at` is older
/// than `now - zombie_threshold_secs`.
pub async fn start_zombie_cleaner(state: Arc<State>, zombie_threshold_secs: i64) {
    info!("ZombieTaskCleaner started with threshold {}s", zombie_threshold_secs);
    
    loop {
        sleep(Duration::from_secs(60)).await;
        
        let threshold_time = Utc::now() - chrono::Duration::seconds(zombie_threshold_secs);
        
        match clean_zombies(&state, threshold_time).await {
            Ok(count) => {
                if count > 0 {
                    warn!("Cleaned up {} zombie tasks", count);
                }
            }
            Err(e) => {
                error!("Failed to clean zombie tasks: {}", e);
            }
        }
    }
}

async fn clean_zombies(state: &State, threshold_time: DateTime<Utc>) -> Result<u64, sea_orm::DbErr> {
    // Uses a raw SQL update for schema-compatible bulk transitions.
    let sql = format!(
        "UPDATE base.task_result SET status = 'Failed', error = 'Zombie Task Detected (Timeout)' WHERE status = 'Running' AND updated_at < '{}'",
        threshold_time.format("%Y-%m-%d %H:%M:%S")
    );
    
    let backend = state.db.get_database_backend();
    let res = state.db.execute(Statement::from_string(backend, sql)).await?;
    
    Ok(res.rows_affected())
}
