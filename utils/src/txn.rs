use sea_orm::{
    AccessMode, DatabaseConnection, DatabaseTransaction, IsolationLevel, TransactionTrait,
};
use std::sync::Arc;

#[allow(unused)]
pub async fn begin_write(db: &Arc<DatabaseConnection>) -> Result<DatabaseTransaction,Box<dyn std::error::Error>> {
    Ok(db
        .begin_with_config(
            Some(IsolationLevel::ReadCommitted),
            Some(AccessMode::ReadWrite),
        )
        .await?)
}
pub async fn begin_read(db: &Arc<DatabaseConnection>) -> Result<DatabaseTransaction,Box<dyn std::error::Error>> {
    Ok(db
        .begin_with_config(
            Some(IsolationLevel::ReadCommitted),
            Some(AccessMode::ReadOnly),
        )
        .await?)
}
