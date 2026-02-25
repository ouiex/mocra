use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq, Serialize, Deserialize)]
#[sea_orm(schema_name = "base", table_name = "log")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: i64,
    pub task_id: String,
    pub request_id: Option<Uuid>,
    pub status: String,
    pub level: String,
    #[sea_orm(column_type = "Text")]
    pub message: String,
    pub timestamp: DateTime,
    #[sea_orm(column_type = "Text", nullable)]
    pub traceback: Option<String>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
