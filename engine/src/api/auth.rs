use axum::{
    extract::{Request, State},
    http::{StatusCode, HeaderMap},
    middleware::Next,
    response::Response,
};
use crate::api::state::ApiState;

/// API Authentication Middleware
///
/// Validates the request headers against the configured API key.
/// Supports `Authorization: Bearer <key>` and `x-api-key: <key>`.
/// If no API key is configured in `config.toml`, requests are rejected.
pub async fn auth_middleware(
    State(state): State<ApiState>,
    headers: HeaderMap,
    request: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    // Get config from state (read lock)
    let config = state.state.config.read().await;
    
    let api_config = config.api.as_ref().ok_or(StatusCode::FORBIDDEN)?;
    let configured_key = api_config.api_key.as_ref().ok_or(StatusCode::FORBIDDEN)?;

    // Check headers
    let auth_header = headers
        .get("Authorization")
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "));
        
    let api_key_header = headers
        .get("x-api-key")
        .and_then(|h| h.to_str().ok());

    match (auth_header, api_key_header) {
        (Some(token), _) if token == configured_key => {}
        (_, Some(key)) if key == configured_key => {}
        _ => return Err(StatusCode::UNAUTHORIZED),
    }
    
    // If no API key configured, or key matched, proceed
    Ok(next.run(request).await)
}
