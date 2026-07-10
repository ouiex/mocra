use axum::{extract::State, Json};
use crate::engine::api::state::ApiState;
use serde::Serialize;
// use sea_orm::ConnectionTrait; // for ping() - DatabaseConnection implements it inherently or via Deref?

#[derive(Serialize)]
pub struct Components {
    redis: ComponentStatus,
    db: ComponentStatus,
}

#[derive(Serialize)]
pub struct ComponentStatus {
    status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Serialize)]
pub struct HealthResponse {
    status: String,
    components: Components,
}

impl ComponentStatus {
    fn up() -> Self {
        Self { status: "up".to_string(), error: None }
    }
    fn down(e: impl ToString) -> Self {
        Self { status: "down".to_string(), error: Some(e.to_string()) }
    }
}

pub async fn health_check(
    State(state): State<ApiState>,
) -> Json<HealthResponse> {
    // Check Redis
    let redis_status = match state.state.cache_service.ping().await {
        Ok(_) => ComponentStatus::up(),
        Err(e) => ComponentStatus::down(e),
    };

    // Check DB(无 `store` 特性 / 无 DB 模式视为 up)。
    #[cfg(feature = "store")]
    let db_status = match state.state.db.as_ref() {
        Some(db) => match db.ping().await {
            Ok(_) => ComponentStatus::up(),
            Err(e) => ComponentStatus::down(e),
        },
        None => ComponentStatus::up(),
    };
    #[cfg(not(feature = "store"))]
    let db_status = ComponentStatus::up();

    let global_status = if redis_status.status == "up" && db_status.status == "up" {
        "up"
    } else {
        "degraded"
    };

    Json(HealthResponse {
        status: global_status.to_string(),
        components: Components {
            redis: redis_status,
            db: db_status,
        },
    })
}
