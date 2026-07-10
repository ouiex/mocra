use axum::{
    extract::{Request, State},
    http::StatusCode,
    middleware::Next,
    response::Response,
};
use crate::engine::api::state::ApiState;

/// Rate Limit Middleware
///
/// Limits requests based on API Key or IP (simplified to "anonymous" if no key).
/// Uses the `api_limiter` configured in `State`.
pub async fn rate_limit_middleware(
    State(state): State<ApiState>,
    req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    if let Some(limiter) = &state.state.api_limiter {
        // Try to identify user by API key
        let key = req.headers()
            .get("x-api-key")
            .and_then(|v| v.to_str().ok())
            .or_else(|| {
                 req.headers()
                    .get("Authorization")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.strip_prefix("Bearer "))
            })
            .unwrap_or("anonymous");

        // Acquire 1 permit
        // If it fails (limit exceeded), return 429
        if limiter.as_ref().acquire(key, 1.0).await.is_err() {
             return Err(StatusCode::TOO_MANY_REQUESTS);
        }
    }
    
    Ok(next.run(req).await)
}
