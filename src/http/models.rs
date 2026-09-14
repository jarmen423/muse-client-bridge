//! `GET /v1/models`: cached `model/list` as an OpenAI model list.

use axum::Json;
use axum::extract::State;
use axum::response::{IntoResponse, Response};

use crate::http::AppState;
use crate::http::error::ApiError;

/// `GET /v1/models`.
pub async fn handler(State(state): State<AppState>) -> Response {
    let tail = state.dispatcher.supervisor().current().await.stderr_tail();
    match state.dispatcher.models().await {
        Ok(catalog) => {
            let data: Vec<serde_json::Value> = catalog
                .models
                .iter()
                .map(|model| {
                    serde_json::json!({
                        "id": model.id,
                        "object": "model",
                        "created": model.created,
                        "owned_by": model.owned_by,
                    })
                })
                .collect();
            (
                axum::http::StatusCode::OK,
                Json(serde_json::json!({"object": "list", "data": data})),
            )
                .into_response()
        }
        Err(error) => ApiError::from_dispatch(&error, &tail.text()).into_response(),
    }
}
