//! `POST /v1/chat/completions`: JSON or SSE (SPEC §3.1).
//!
//! - Content deltas → `choices[0].delta.content`; reasoning summaries →
//!   `delta.reasoning_content`; tool/status lines → appended assistant
//!   text (visibly marked, never silent).
//! - Success streams end `stop-chunk → [usage-chunk] → [DONE]`; failures
//!   emit one `{"error": …}` frame and close — a stream that just closes,
//!   or claims `stop` after a failure, is a bug.

use std::sync::Arc;

use axum::Json;
use axum::extract::State;
use axum::response::{IntoResponse, Response};
use serde_json::Value;

use crate::dispatch::TurnHandle;
use crate::http::AppState;
use crate::http::error::ApiError;
use crate::http::sse::{self, DONE_FRAME, SsePoll, SseRender, data_frame, sse_headers};
use crate::msp::fold::{OutputEvent, TurnOutcome, Usage};
use crate::msp::spawn::StderrTail;
use crate::translate::{ChatRequest, translate_chat};

/// `POST /v1/chat/completions`.
pub async fn handler(
    State(state): State<AppState>,
    body: Result<Json<ChatRequest>, axum::extract::rejection::JsonRejection>,
) -> Response {
    let request = match body {
        Ok(Json(request)) => request,
        Err(rejection) => {
            return ApiError::new(
                axum::http::StatusCode::BAD_REQUEST,
                "invalid_request_error",
                "invalid_json",
                format!("invalid JSON body: {rejection}"),
            )
            .into_response();
        }
    };
    let model = request
        .model
        .as_deref()
        .map(str::trim)
        .filter(|m| !m.is_empty());
    let stream = request.stream.unwrap_or(false);
    let include_usage = request
        .stream_options
        .as_ref()
        .and_then(|o| o.include_usage)
        .unwrap_or(false);

    let input = match translate_chat(&request).await {
        Ok(input) => input,
        Err(error) => return ApiError::from_translate(&error).into_response(),
    };
    // Fresh stderr tail per failure frame (sync read from the live ring).
    let tail = state.dispatcher.supervisor().current().await.stderr_tail();
    let handle = match state
        .dispatcher
        .run_turn(model.map(str::to_string), input)
        .await
    {
        Ok(handle) => handle,
        Err(error) => return ApiError::from_dispatch(&error, &tail.text()).into_response(),
    };
    let response_model = model.unwrap_or("default").to_string();
    if stream {
        stream_response(handle, response_model, include_usage, tail).into_response()
    } else {
        collect_response(handle, response_model, &tail).await
    }
}

/// Render a collected (non-stream) turn.
async fn collect_response(mut handle: TurnHandle, model: String, tail: &StderrTail) -> Response {
    let collected = handle.collect().await;
    let id = sse::new_id("chatcmpl");
    let created = sse::now_secs();
    match collected.outcome {
        Some(TurnOutcome::Completed { usage }) => {
            let content = join_content(&collected.text, &collected.status_lines);
            (
                axum::http::StatusCode::OK,
                Json(completion_response(
                    &id,
                    &model,
                    created,
                    &content,
                    &collected.reasoning,
                    &usage,
                )),
            )
                .into_response()
        }
        Some(TurnOutcome::Cancelled { .. }) => ApiError::new(
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            "server_error",
            "turn_cancelled",
            "turn was cancelled (fail-closed policy with nothing safe to do)",
        )
        .into_response(),
        Some(TurnOutcome::Failed { kind, message, .. }) => {
            ApiError::from_turn_failure(&kind, &message, &tail.text()).into_response()
        }
        None => ApiError::new(
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            "server_error",
            "bridge_bug",
            "turn stream ended without a terminal",
        )
        .into_response(),
    }
}

/// Render a streaming turn (SSE).
fn stream_response(
    handle: TurnHandle,
    model: String,
    include_usage: bool,
    tail: Arc<StderrTail>,
) -> Response {
    let id = sse::new_id("chatcmpl");
    let created = sse::now_secs();
    let preface = vec![data_frame(&delta_chunk(
        &id,
        &model,
        created,
        serde_json::json!({"role": "assistant"}),
    ))];
    let mut render = ChatRender {
        id,
        model,
        created,
        include_usage,
        tail,
    };
    let stream = sse::sse_stream(handle, preface, sse::KEEPALIVE_INTERVAL, move |poll| {
        render.poll(poll)
    });
    let body = axum::body::Body::from_stream(stream);
    let mut response = (axum::http::StatusCode::OK, body).into_response();
    sse_headers(response.headers_mut());
    response
}

/// SSE render state for one chat stream.
struct ChatRender {
    id: String,
    model: String,
    created: u64,
    include_usage: bool,
    tail: Arc<StderrTail>,
}

impl ChatRender {
    fn poll(&mut self, poll: SsePoll) -> SseRender {
        match poll {
            SsePoll::Event(OutputEvent::ContentDelta(text))
            | SsePoll::Event(OutputEvent::StatusLine(text)) => {
                SseRender::frames(vec![data_frame(&delta_chunk(
                    &self.id,
                    &self.model,
                    self.created,
                    serde_json::json!({"content": text}),
                ))])
            }
            SsePoll::Event(OutputEvent::ReasoningDelta { text, .. }) => {
                SseRender::frames(vec![data_frame(&delta_chunk(
                    &self.id,
                    &self.model,
                    self.created,
                    serde_json::json!({"reasoning_content": text}),
                ))])
            }
            // Never surfaces from dispatch (paged + refolded inside); defensive.
            SsePoll::Event(OutputEvent::Gap { .. }) => SseRender::frames(vec![]),
            SsePoll::Event(OutputEvent::Terminal(outcome)) => self.terminal(outcome),
            SsePoll::KeepAlive => SseRender::keepalive(),
            SsePoll::Closed => SseRender::terminal(vec![data_frame(
                &ApiError::new(
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    "server_error",
                    "bridge_bug",
                    "turn stream ended without a terminal",
                )
                .body(),
            )]),
        }
    }

    fn terminal(&self, outcome: TurnOutcome) -> SseRender {
        match outcome {
            TurnOutcome::Completed { usage } => {
                let mut frames = vec![data_frame(&stop_chunk(&self.id, &self.model, self.created))];
                if self.include_usage {
                    frames.push(data_frame(&usage_chunk(
                        &self.id,
                        &self.model,
                        self.created,
                        &usage,
                    )));
                }
                frames.push(DONE_FRAME.to_string());
                SseRender::terminal(frames)
            }
            // MUST NOT claim `stop`: one error frame, then close.
            TurnOutcome::Cancelled { .. } => SseRender::terminal(vec![data_frame(
                &ApiError::new(
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    "server_error",
                    "turn_cancelled",
                    "turn was cancelled (fail-closed policy with nothing safe to do)",
                )
                .body(),
            )]),
            TurnOutcome::Failed { kind, message, .. } => SseRender::terminal(vec![data_frame(
                &ApiError::from_turn_failure(&kind, &message, &self.tail.text()).body(),
            )]),
        }
    }
}

/// One delta chunk (`finish_reason: null`).
pub fn delta_chunk(id: &str, model: &str, created: u64, delta: Value) -> Value {
    serde_json::json!({
        "id": id,
        "object": "chat.completion.chunk",
        "created": created,
        "model": model,
        "choices": [{"index": 0, "delta": delta, "finish_reason": null}],
    })
}

/// The final `stop` chunk.
pub fn stop_chunk(id: &str, model: &str, created: u64) -> Value {
    serde_json::json!({
        "id": id,
        "object": "chat.completion.chunk",
        "created": created,
        "model": model,
        "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
    })
}

/// The `include_usage` trailer chunk.
pub fn usage_chunk(id: &str, model: &str, created: u64, usage: &Usage) -> Value {
    serde_json::json!({
        "id": id,
        "object": "chat.completion.chunk",
        "created": created,
        "model": model,
        "choices": [{"index": 0, "delta": {}, "finish_reason": null}],
        "usage": usage_value(usage),
    })
}

/// Chat Completions usage object (responses has its own field names; see
/// `responses_usage_value`).
pub fn usage_value(usage: &Usage) -> Value {
    serde_json::json!({
        "prompt_tokens": usage.prompt_tokens,
        "completion_tokens": usage.completion_tokens,
        "total_tokens": usage.total_tokens,
    })
}

/// Non-stream `chat.completion` object.
pub fn completion_response(
    id: &str,
    model: &str,
    created: u64,
    content: &str,
    reasoning: &str,
    usage: &Usage,
) -> Value {
    let mut message = serde_json::json!({"role": "assistant", "content": content});
    if !reasoning.is_empty() {
        message["reasoning_content"] = Value::String(reasoning.to_string());
    }
    serde_json::json!({
        "id": id,
        "object": "chat.completion",
        "created": created,
        "model": model,
        "choices": [{"index": 0, "message": message, "finish_reason": "stop"}],
        "usage": usage_value(usage),
    })
}

/// Non-stream content: answer text plus appended status lines.
fn join_content(text: &str, status_lines: &[String]) -> String {
    if status_lines.is_empty() {
        return text.to_string();
    }
    if text.is_empty() {
        return status_lines.join("\n");
    }
    format!("{text}\n{}", status_lines.join("\n"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_goldens() {
        let usage = Usage {
            prompt_tokens: 100,
            completion_tokens: 10,
            total_tokens: 110,
        };
        assert_eq!(
            delta_chunk("chatcmpl-1", "m", 7, serde_json::json!({"content": "Hi"})),
            serde_json::json!({
                "id": "chatcmpl-1", "object": "chat.completion.chunk",
                "created": 7, "model": "m",
                "choices": [{"index": 0, "delta": {"content": "Hi"}, "finish_reason": null}],
            })
        );
        assert_eq!(
            stop_chunk("chatcmpl-1", "m", 7)["choices"][0]["finish_reason"],
            "stop"
        );
        let trailer = usage_chunk("chatcmpl-1", "m", 7, &usage);
        assert_eq!(trailer["usage"]["total_tokens"], 110);
        assert_eq!(trailer["choices"][0]["finish_reason"], Value::Null);
    }

    #[test]
    fn completion_golden() {
        let usage = Usage {
            prompt_tokens: 1,
            completion_tokens: 2,
            total_tokens: 3,
        };
        let response = completion_response("chatcmpl-9", "m", 7, "answer", "think", &usage);
        assert_eq!(response["object"], "chat.completion");
        assert_eq!(response["choices"][0]["message"]["content"], "answer");
        assert_eq!(
            response["choices"][0]["message"]["reasoning_content"],
            "think"
        );
        assert_eq!(response["choices"][0]["finish_reason"], "stop");
        // Empty reasoning omits the key.
        let response = completion_response("chatcmpl-9", "m", 7, "answer", "", &usage);
        assert!(
            response["choices"][0]["message"]
                .get("reasoning_content")
                .is_none()
        );
    }

    #[test]
    fn status_lines_append_without_leading_blank() {
        assert_eq!(join_content("t", &[]), "t");
        assert_eq!(join_content("", &["a".into()]), "a");
        assert_eq!(join_content("t", &["a".into(), "b".into()]), "t\na\nb");
    }
}
