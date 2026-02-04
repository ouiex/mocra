use std::sync::Arc;
use tokio::time::{Duration, sleep};
use log::{info, warn, error};
use common::state::State;
use sea_orm::{ConnectionTrait, Statement};
use chrono::{Utc, DateTime};

/// Zombie Task Cleaner
///
/// Scans the database/cache for tasks that have been in 'Running' state for too long.
/// This typically happens when a worker crashes before updating the task status.
///
/// Strategy:
/// 1. Scan `task_result` table for tasks with status `Running` and `updated_at` < (now - threshold).
/// 2. Mark them as `Failed` with error "Zombie Task Detected".
/// 3. Optionally re-queue them (if retry policy allows).
pub async fn start_zombie_cleaner(state: Arc<State>, zombie_threshold_secs: i64) {
    info!("ZombieTaskCleaner started with threshold {}s", zombie_threshold_secs);
    
    loop {
        // Run every minute
        sleep(Duration::from_secs(60)).await;
        
        let threshold_time = Utc::now() - chrono::Duration::seconds(zombie_threshold_secs);
        
        // This query depends on the schema of `task_result` which is SeaORM entity.
        // Assuming there is a `status` and `updated_at` column.
        // We will use raw SQL for flexibility or Entity if available.
        // Let's use SeaORM Entity update for safety.
        
        // We need to fetch the IDs first to log them
        // Note: This logic assumes `task_result` table tracks active tasks.
        // If active tasks are only in Redis, we would scan Redis.
        // But `StatusTracker` writes to DB.
        
        // Actually, `StatusTracker` updates DB on status change.
        // So a crashed worker leaves a 'Running' row in DB.
        
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
    // Update task_result SET status = 'Failed', error = 'Zombie Task' WHERE status = 'Running' AND updated_at < threshold
    // SeaORM update_many
    
    // Convert DateTime<Utc> to NaiveDateTime for SeaORM if needed, usually it handles it.
    // Assuming `task_result` entity is available and has `status` column.
    
    // Since we don't have the full Entity definition visible here (it's in common), 
    // we'll use a raw SQL execution for robust compatibility across schema versions 
    // (assuming standard column names).
    
    let sql = format!(
        "UPDATE base.task_result SET status = 'Failed', error = 'Zombie Task Detected (Timeout)' WHERE status = 'Running' AND updated_at < '{}'",
        threshold_time.format("%Y-%m-%d %H:%M:%S")
    );
    
    let backend = state.db.get_database_backend();
    let res = state.db.execute(Statement::from_string(backend, sql)).await?;
    
    Ok(res.rows_affected())
}
