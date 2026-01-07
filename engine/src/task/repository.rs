#![allow(unused)]
use common::model::entity::*;
use errors::{ Result};
use utils::txn::begin_read;
use sea_orm::{
    ColumnTrait, DatabaseConnection, DbBackend, EntityTrait, QueryFilter, Statement, Value,
};
use std::collections::HashMap;
use std::sync::Arc;
use errors::OrmError;

/// 数据库访问层，负责从数据库加载实体数据
pub struct TaskRepository {
    db: Arc<DatabaseConnection>,
}

impl TaskRepository {
    pub fn new(db: DatabaseConnection) -> Self {
        Self { db: Arc::new(db) }
    }

    /// 加载账号信息
    pub async fn load_account(&self, account_name: &str) -> Result<AccountModel> {
        let txn = begin_read(&self.db)
            .await
            .map_err(|e| OrmError::ConnectionError(e.to_string().into()))?;

        let account = AccountEntity::find()
            .filter(AccountColumn::Name.eq(account_name))
            .filter(AccountColumn::Enabled.eq(true))
            .one(&txn)
            .await
            .map_err(|e| OrmError::QueryExecutionError(e.to_string().into()))?
            .ok_or_else(|| OrmError::NotFound)?;

        Ok(account)
    }

    /// 加载平台信息
    pub async fn load_platform(&self, platform_name: &str) -> Result<PlatformModel> {
        let txn = begin_read(&self.db)
            .await
            .map_err(|e| OrmError::ConnectionError(e.to_string().into()))?;

        let platform = PlatformEntity::find()
            .filter(PlatformColumn::Name.eq(platform_name))
            .filter(PlatformColumn::Enabled.eq(true))
            .one(&txn)
            .await
            .map_err(|e| OrmError::QueryExecutionError(e.to_string().into()))?
            .ok_or_else(|| OrmError::NotFound)?;

        Ok(platform)
    }

    /// 验证账号-平台关联
    pub async fn load_account_platform_relation(
        &self,
        account_id: i32,
        platform_id: i32,
    ) -> Result<RelAccountPlatformModel> {
        let txn = begin_read(&self.db)
            .await
            .map_err(|e| OrmError::ConnectionError(e.to_string().into()))?;

        let relation = RelAccountPlatformEntity::find()
            .filter(RelAccountPlatformColumn::AccountId.eq(account_id))
            .filter(RelAccountPlatformColumn::PlatformId.eq(platform_id))
            .filter(RelAccountPlatformColumn::Enabled.eq(true))
            .one(&txn)
            .await
            .map_err(|e| OrmError::QueryExecutionError(e.to_string().into()))?
            .ok_or_else(|| OrmError::NotFound)?;

        Ok(relation)
    }

    /// 根据账号和平台加载所有启用的模块
    pub async fn load_modules_by_account_platform(
        &self,
        platform_name: &str,
        account_name: &str,
    ) -> Result<Vec<ModuleModel>> {
        let txn = begin_read(&self.db)
            .await
            .map_err(|e| OrmError::ConnectionError(e.to_string().into()))?;

        let module_sql = r#"
        select a.* from base.module as a
        left join base.rel_module_platform rmp on a.id = rmp.module_id
        left join base.rel_module_account rma on a.id = rma.module_id
        left join base.rel_account_platform rap on rma.account_id = rap.account_id and rmp.platform_id = rap.platform_id
        left join base.platform as p on rmp.platform_id = p.id
        left join base.account as acc on rma.account_id = acc.id
        where a.enabled = true
        and rmp.enabled = true
        and rma.enabled = true
        and rap.enabled = true
        and p.enabled = true
        and acc.enabled = true
        and p.name = $1
        and acc.name = $2"#;

        let modules = ModuleEntity::find()
            .from_raw_sql(Statement::from_sql_and_values(
                DbBackend::Postgres,
                module_sql,
                vec![
                    Value::from(platform_name.to_string()),
                    Value::from(account_name.to_string()),
                ],
            ))
            .all(&txn)
            .await
            .map_err(|e| OrmError::QueryExecutionError(e.to_string().into()))?;

        Ok(modules)
    }

    /// 根据账号、平台和模块名加载特定模块
    pub async fn load_module_by_account_platform_module(
        &self,
        platform_name: &str,
        account_name: &str,
        module_name: &[String], // 保持Vec<String>参数
    ) -> Result<Vec<ModuleModel>> {
        let txn = begin_read(&self.db)
            .await
            .map_err(|e| OrmError::ConnectionError(e.to_string().into()))?;

        // 动态生成IN子句的占位符
        let placeholders: Vec<String> =
            (1..=module_name.len()).map(|i| format!("${i}")).collect();
        let in_clause = placeholders.join(", ");

        let module_sql = format!(
            r#"  
        select a.* from base.module as a  
        left join base.rel_module_platform rmp on a.id = rmp.module_id  
        left join base.rel_module_account rma on a.id = rma.module_id  
        left join base.rel_account_platform rap on rma.account_id = rap.account_id and rmp.platform_id = rap.platform_id  
        left join base.platform as p on rmp.platform_id = p.id  
        left join base.account as acc on rma.account_id = acc.id  
        where a.enabled = true  
        and rmp.enabled = true  
        and rma.enabled = true  
        and rap.enabled = true  
        and p.enabled = true  
        and acc.enabled = true  
        and a.name IN ({})  
        and p.name = ${}  
        and acc.name = ${}"#,
            in_clause,
            module_name.len() + 1,
            module_name.len() + 2
        );

        // 构建参数值向量
        let mut values: Vec<Value> = module_name
            .iter()
            .map(|name| Value::from(name.clone()))
            .collect();
        values.push(Value::from(platform_name.to_string()));
        values.push(Value::from(account_name.to_string()));

        let module = ModuleEntity::find()
            .from_raw_sql(Statement::from_sql_and_values(
                DbBackend::Postgres,
                module_sql,
                values,
            ))
            .all(&txn)
            .await
            .map_err(|e| OrmError::QueryExecutionError(e.to_string().into()))?;

        Ok(module)
    }

    /// 加载模块与平台的关联关系
    pub async fn load_module_platform_relation(
        &self,
        module_id: i32,
        platform_id: i32,
    ) -> Result<RelModulePlatformModel> {
        let txn = begin_read(&self.db)
            .await
            .map_err(|e| OrmError::ConnectionError(e.to_string().into()))?;

        let relation = RelModulePlatformEntity::find()
            .filter(RelModulePlatformColumn::ModuleId.eq(module_id))
            .filter(RelModulePlatformColumn::PlatformId.eq(platform_id))
            .filter(RelModulePlatformColumn::Enabled.eq(true))
            .one(&txn)
            .await
            .map_err(|e| OrmError::QueryExecutionError(e.to_string().into()))?
            .ok_or_else(|| OrmError::NotFound)?;

        Ok(relation)
    }

    /// 加载模块与账号的关联关系
    pub async fn load_module_account_relation(
        &self,
        module_id: i32,
        account_id: i32,
    ) -> Result<RelModuleAccountModel> {
        let txn = begin_read(&self.db)
            .await
            .map_err(|e| OrmError::ConnectionError(e.to_string().into()))?;

        let relation = RelModuleAccountEntity::find()
            .filter(RelModuleAccountColumn::ModuleId.eq(module_id))
            .filter(RelModuleAccountColumn::AccountId.eq(account_id))
            .filter(RelModuleAccountColumn::Enabled.eq(true))
            .one(&txn)
            .await
            .map_err(|e| OrmError::QueryExecutionError(e.to_string().into()))?
            .ok_or_else(|| OrmError::NotFound)?;

        Ok(relation)
    }

    /// 批量加载模块的数据中间件关联
    pub async fn load_module_data_middleware_relations(
        &self,
        module_ids: &[i32],
    ) -> Result<HashMap<i32, Vec<RelModuleDataMiddlewareModel>>> {
        let txn = begin_read(&self.db)
            .await
            .map_err(|e| OrmError::ConnectionError(e.to_string().into()))?;

        let relations = RelModuleDataMiddlewareEntity::find()
            .filter(RelModuleDataMiddlewareColumn::ModuleId.is_in(module_ids.iter().copied()))
            .filter(RelModuleDataMiddlewareColumn::Enabled.eq(true))
            .all(&txn)
            .await
            .map_err(|e| OrmError::QueryExecutionError(e.to_string().into()))?;

        let mut grouped = HashMap::new();
        for relation in relations {
            grouped
                .entry(relation.module_id)
                .or_insert_with(Vec::new)
                .push(relation);
        }

        Ok(grouped)
    }

    /// 批量加载模块的下载中间件关联
    pub async fn load_module_download_middleware_relations(
        &self,
        module_ids: &[i32],
    ) -> Result<HashMap<i32, Vec<RelModuleDownloadMiddlewareModel>>> {
        let txn = begin_read(&self.db)
            .await
            .map_err(|e| OrmError::ConnectionError(e.to_string().into()))?;

        let relations = RelModuleDownloadMiddlewareEntity::find()
            .filter(RelModuleDownloadMiddlewareColumn::ModuleId.is_in(module_ids.iter().copied()))
            .filter(RelModuleDownloadMiddlewareColumn::Enabled.eq(true))
            .all(&txn)
            .await
            .map_err(|e| OrmError::QueryExecutionError(e.to_string().into()))?;

        let mut grouped = HashMap::new();
        for relation in relations {
            grouped
                .entry(relation.module_id)
                .or_insert_with(Vec::new)
                .push(relation);
        }

        Ok(grouped)
    }

    /// 批量加载数据中间件
    pub async fn load_data_middlewares(
        &self,
        middleware_ids: &[i32],
    ) -> Result<Vec<DataMiddlewareModel>> {
        let txn = begin_read(&self.db)
            .await
            .map_err(|e| OrmError::ConnectionError(e.to_string().into()))?;

        let middlewares = DataMiddlewareEntity::find()
            .filter(DataMiddlewareColumn::Id.is_in(middleware_ids.iter().copied()))
            .filter(DataMiddlewareColumn::Enabled.eq(true))
            .all(&txn)
            .await
            .map_err(|e| OrmError::QueryExecutionError(e.to_string().into()))?;

        Ok(middlewares)
    }

    /// 批量加载下载中间件
    pub async fn load_download_middlewares(
        &self,
        middleware_ids: &[i32],
    ) -> Result<Vec<DownloadMiddlewareModel>> {
        let txn = begin_read(&self.db)
            .await
            .map_err(|e| OrmError::ConnectionError(e.to_string().into()))?;

        let middlewares = DownloadMiddlewareEntity::find()
            .filter(DownloadMiddlewareColumn::Id.is_in(middleware_ids.iter().copied()))
            .filter(DownloadMiddlewareColumn::Enabled.eq(true))
            .all(&txn)
            .await
            .map_err(|e| OrmError::QueryExecutionError(e.to_string().into()))?;

        Ok(middlewares)
    }
}
