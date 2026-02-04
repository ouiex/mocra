use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq, Serialize, Deserialize)]
#[sea_orm(schema_name = "base", table_name = "task_result")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: i64,
    pub task_id: String,
    pub status: String,
    pub start_time: DateTime,
    pub end_time: Option<DateTime>,
    #[sea_orm(column_type = "Text", nullable)]
    pub result: Option<String>,
    #[sea_orm(column_type = "Text", nullable)]
    pub error: Option<String>,
    pub updated_at: DateTime,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
