use std::sync::Arc;
use tokio::time::{Duration, sleep};
use log::{info, warn, error};
use crate::common::state::State;
use crate::queue::compensation::Compensator;
#[cfg(feature = "store")]
use sea_orm::{ConnectionTrait, Statement, TransactionTrait};
#[cfg(feature = "store")]
use chrono::{Utc, DateTime};

/// Starts a periodic cleaner for stale `Running` tasks.
///
/// Every minute, the cleaner marks rows as `Failed` when `updated_at` is older
/// than `now - zombie_threshold_secs`.
pub async fn start_zombie_cleaner(
    state: Arc<State>,
    zombie_threshold_secs: i64,
    compensator: Option<Arc<dyn Compensator>>,
) {
    #[cfg(not(feature = "store"))]
    {
        let _ = (&state, zombie_threshold_secs, &compensator);
        info!("ZombieTaskCleaner disabled (no `store` feature); nothing to clean");
    }
    #[cfg(feature = "store")]
    {
        info!("ZombieTaskCleaner started with threshold {}s", zombie_threshold_secs);

        loop {
            sleep(Duration::from_secs(60)).await;

            let threshold_time = Utc::now() - chrono::Duration::seconds(zombie_threshold_secs);

            match clean_zombies(&state, threshold_time, &compensator).await {
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
}

#[cfg(feature = "store")]
async fn clean_zombies(
    state: &State,
    threshold_time: DateTime<Utc>,
    compensator: &Option<Arc<dyn Compensator>>,
) -> Result<u64, sea_orm::DbErr> {
    let formatted_time = threshold_time.format("%Y-%m-%d %H:%M:%S").to_string();
    // 无 DB(standalone)模式:无僵尸任务表可清理。
    let Some(db) = state.db.as_ref() else {
        return Ok(0);
    };
    let backend = db.get_database_backend();

    let txn = db.begin().await?;

    // Mark zombie tasks as Failed inside the transaction.
    let update_sql = format!(
        "UPDATE base.task_result SET status = 'Failed', error = 'Zombie Task Detected (Timeout)' WHERE status = 'Running' AND updated_at < '{}'",
        formatted_time
    );
    let res = txn.execute(Statement::from_string(backend, update_sql)).await?;
    let affected = res.rows_affected();

    // Remove compensation records only for rows actually transitioned.
    if affected > 0 {
        if let Some(comp) = compensator {
            let select_sql = format!(
                "SELECT id FROM base.task_result WHERE status = 'Failed' AND error = 'Zombie Task Detected (Timeout)' AND updated_at < '{}'",
                formatted_time
            );
            if let Ok(rows) = txn.query_all(Statement::from_string(backend, select_sql)).await {
                for row in &rows {
                    if let Ok(id) = row.try_get::<String>("", "id") {
                        let _ = comp.remove_task("task", &id).await;
                    }
                }
            }
        }
    }

    txn.commit().await?;

    Ok(affected)
}
