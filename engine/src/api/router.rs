
use axum::extract::State;
use axum::{Json, Router};
use axum::routing::post;
use common::model::message::TaskModel;
use crate::api::state::ApiState;


pub fn router()->Router<ApiState>{
    Router::new()
        .route("/start_work", post(start_work))
}


pub async fn start_work(
    State(app_state): State<ApiState>,
    Json(task): Json<TaskModel>,
){
    let task_pop_chain = app_state.queue_manager.get_task_push_channel().clone();
    if let Err(e) = task_pop_chain.send(task).await {
        log::error!("Failed to send task to processing channel: {}", e);
    }
}