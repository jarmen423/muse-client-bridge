//! `POST /v1/responses`: JSON or SSE (SPEC §3.2).
//!
//! Success streams MUST be `response.created → output_item.added →
//! output_text.delta×N → output_item.done → response.completed` (Codex
//! errors without the terminal `completed`). Reasoning summaries ride the
//! standard `reasoning_summary_part` vocabulary (best-effort; unknown to a
//! client they are ignored, never fatal). Failures emit `response.failed`
//! with OpenAI-shaped `error.code`s (`context_length_exceeded`,
//! `rate_limit_exceeded`) so Codex classifies them.

use std::sync::Arc;

use axum::Json;
use axum::extract::State;
use axum::response::{IntoResponse, Response};
use serde_json::Value;

use crate::dispatch::TurnHandle;
use crate::http::AppState;
use crate::http::error::ApiError;
use crate::http::sse::{self, SsePoll, SseRender, data_frame, event_frame, sse_headers};
use crate::msp::fold::{OutputEvent, TurnOutcome, Usage};
use crate::msp::spawn::StderrTail;
use crate::translate::{ResponsesRequest, translate_responses};

/// `POST /v1/responses`.
pub async fn handler(
    State(state): State<AppState>,
    body: Result<Json<ResponsesRequest>, axum::extract::rejection::JsonRejection>,
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
    // OpenAI default is non-streaming; Codex always sends `stream: true`.
    let stream = request.stream.unwrap_or(false);

    let input = match translate_responses(&request).await {
        Ok(input) => input,
        Err(error) => return ApiError::from_translate(&error).into_response(),
    };
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
        stream_response(handle, response_model, tail).into_response()
    } else {
        collect_response(handle, response_model, &tail).await
    }
}

/// Render a collected (non-stream) turn.
async fn collect_response(mut handle: TurnHandle, model: String, tail: &StderrTail) -> Response {
    let collected = handle.collect().await;
    let id = sse::new_id("resp");
    let created = sse::now_secs();
    match collected.outcome {
        Some(TurnOutcome::Completed { usage }) => {
            let text = join_text(&collected.text, &collected.status_lines);
            // Reasoning arrives flattened from collect(); emit one summary.
            let summaries = if collected.reasoning.is_empty() {
                Vec::new()
            } else {
                vec![collected.reasoning.clone()]
            };
            let msg_id = sse::new_id("msg");
            let rs_id = sse::new_id("rs");
            (
                axum::http::StatusCode::OK,
                Json(response_object(ResponseObjectArgs {
                    resp_id: &id,
                    model: &model,
                    created,
                    status: "completed",
                    msg_id: &msg_id,
                    text: &text,
                    rs_id: Some(&rs_id),
                    summaries: &summaries,
                    usage: Some(&usage),
                })),
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
fn stream_response(handle: TurnHandle, model: String, tail: Arc<StderrTail>) -> Response {
    let resp_id = sse::new_id("resp");
    let msg_id = sse::new_id("msg");
    let rs_id = sse::new_id("rs");
    let created = sse::now_secs();
    let preface = vec![
        event_frame(
            "response.created",
            &created_event(&resp_id, &model, created),
        ),
        event_frame(
            "response.output_item.added",
            &output_item_added(0, &message_item_shell(&msg_id)),
        ),
    ];
    let mut render = ResponsesRender {
        resp_id,
        msg_id,
        rs_id,
        model,
        created,
        tail,
        text: String::new(),
        reasoning_parts: Vec::new(),
        reasoning_added: false,
        current_part: None,
    };
    let stream = sse::sse_stream(handle, preface, sse::KEEPALIVE_INTERVAL, move |poll| {
        render.poll(poll)
    });
    let body = axum::body::Body::from_stream(stream);
    let mut response = (axum::http::StatusCode::OK, body).into_response();
    sse_headers(response.headers_mut());
    response
}

/// SSE render state for one responses stream.
struct ResponsesRender {
    resp_id: String,
    msg_id: String,
    rs_id: String,
    model: String,
    created: u64,
    tail: Arc<StderrTail>,
    text: String,
    reasoning_parts: Vec<String>,
    reasoning_added: bool,
    current_part: Option<u32>,
}

impl ResponsesRender {
    fn poll(&mut self, poll: SsePoll) -> SseRender {
        match poll {
            SsePoll::Event(OutputEvent::ContentDelta(text))
            | SsePoll::Event(OutputEvent::StatusLine(text)) => {
                self.text.push_str(&text);
                SseRender::frames(vec![event_frame(
                    "response.output_text.delta",
                    &output_text_delta(&self.msg_id, &text),
                )])
            }
            SsePoll::Event(OutputEvent::ReasoningDelta { part, text }) => {
                let frames = self.reasoning_delta(part, &text);
                SseRender::frames(frames)
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

    /// Lazily add the reasoning item/part, then emit the summary delta.
    fn reasoning_delta(&mut self, part: u32, text: &str) -> Vec<String> {
        let mut frames = Vec::new();
        if !self.reasoning_added {
            self.reasoning_added = true;
            frames.push(event_frame(
                "response.output_item.added",
                &output_item_added(1, &reasoning_item(&self.rs_id, &[])),
            ));
        }
        if self.current_part != Some(part) {
            if let Some(previous) = self.current_part {
                frames.push(event_frame(
                    "response.reasoning_summary_part.done",
                    &reasoning_part_done(&self.rs_id, previous),
                ));
            }
            self.current_part = Some(part);
            frames.push(event_frame(
                "response.reasoning_summary_part.added",
                &reasoning_part_added(&self.rs_id, part),
            ));
        }
        while self.reasoning_parts.len() <= part as usize {
            self.reasoning_parts.push(String::new());
        }
        self.reasoning_parts[part as usize].push_str(text);
        frames.push(event_frame(
            "response.reasoning_summary_text.delta",
            &reasoning_text_delta(&self.rs_id, part, text),
        ));
        frames
    }

    fn terminal(&mut self, outcome: TurnOutcome) -> SseRender {
        match outcome {
            TurnOutcome::Completed { usage } => {
                let mut frames = Vec::new();
                let summaries: Vec<String> = std::mem::take(&mut self.reasoning_parts)
                    .into_iter()
                    .filter(|part| !part.is_empty())
                    .collect();
                if self.reasoning_added {
                    if let Some(part) = self.current_part {
                        frames.push(event_frame(
                            "response.reasoning_summary_part.done",
                            &reasoning_part_done(&self.rs_id, part),
                        ));
                    }
                    frames.push(event_frame(
                        "response.output_item.done",
                        &output_item_done(1, &reasoning_item(&self.rs_id, &summaries)),
                    ));
                }
                frames.push(event_frame(
                    "response.output_item.done",
                    &output_item_done(0, &message_item(&self.msg_id, &self.text)),
                ));
                let rs_id = self.reasoning_added.then(|| self.rs_id.clone());
                frames.push(event_frame(
                    "response.completed",
                    &completed_event(&response_object(ResponseObjectArgs {
                        resp_id: &self.resp_id,
                        model: &self.model,
                        created: self.created,
                        status: "completed",
                        msg_id: &self.msg_id,
                        text: &self.text,
                        rs_id: rs_id.as_deref(),
                        summaries: &summaries,
                        usage: Some(&usage),
                    })),
                ));
                SseRender::terminal(frames)
            }
            TurnOutcome::Cancelled { .. } => SseRender::terminal(vec![event_frame(
                "response.failed",
                &failed_event(
                    &self.resp_id,
                    "server_error",
                    "turn was cancelled (fail-closed policy with nothing safe to do)",
                ),
            )]),
            TurnOutcome::Failed { kind, message, .. } => {
                let error = ApiError::from_turn_failure(&kind, &message, &self.tail.text());
                SseRender::terminal(vec![event_frame(
                    "response.failed",
                    &failed_event(&self.resp_id, &error.code, &error.message),
                )])
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Event builders (pure; golden-tested)
// ---------------------------------------------------------------------------

/// `response.created` data (Codex requires `response.id`).
pub fn created_event(resp_id: &str, model: &str, created: u64) -> Value {
    serde_json::json!({
        "type": "response.created",
        "response": {
            "id": resp_id, "object": "response", "created_at": created,
            "model": model, "status": "in_progress",
        },
    })
}

/// `response.output_item.added` data.
pub fn output_item_added(output_index: u32, item: &Value) -> Value {
    serde_json::json!({
        "type": "response.output_item.added",
        "output_index": output_index,
        "item": item,
    })
}

/// `response.output_item.done` data.
pub fn output_item_done(output_index: u32, item: &Value) -> Value {
    serde_json::json!({
        "type": "response.output_item.done",
        "output_index": output_index,
        "item": item,
    })
}

/// `response.output_text.delta` data (Codex requires `delta`).
pub fn output_text_delta(item_id: &str, delta: &str) -> Value {
    serde_json::json!({
        "type": "response.output_text.delta",
        "item_id": item_id,
        "output_index": 0,
        "content_index": 0,
        "delta": delta,
    })
}

/// `response.reasoning_summary_part.added` data (`part.summary_index` +
/// `item_id` required by Codex).
pub fn reasoning_part_added(item_id: &str, summary_index: u32) -> Value {
    serde_json::json!({
        "type": "response.reasoning_summary_part.added",
        "item_id": item_id,
        "output_index": 1,
        "summary_index": summary_index,
        "part": {"type": "summary_text", "text": "", "summary_index": summary_index},
    })
}

/// `response.reasoning_summary_part.done` data.
pub fn reasoning_part_done(item_id: &str, summary_index: u32) -> Value {
    serde_json::json!({
        "type": "response.reasoning_summary_part.done",
        "item_id": item_id,
        "output_index": 1,
        "summary_index": summary_index,
        "part": {"type": "summary_text", "summary_index": summary_index},
    })
}

/// `response.reasoning_summary_text.delta` data (`summary_index` + `delta`
/// required by Codex).
pub fn reasoning_text_delta(item_id: &str, summary_index: u32, delta: &str) -> Value {
    serde_json::json!({
        "type": "response.reasoning_summary_text.delta",
        "item_id": item_id,
        "output_index": 1,
        "summary_index": summary_index,
        "delta": delta,
    })
}

/// `response.completed` data (Codex requires `response.id`; usage strongly
/// recommended — a missing block is a client-visible data hole).
pub fn completed_event(response: &Value) -> Value {
    serde_json::json!({"type": "response.completed", "response": response})
}

/// Responses-API usage object. Field names differ from chat ON PURPOSE:
/// Codex's `ResponseCompletedUsage` parser requires `input_tokens` /
/// `output_tokens` / `total_tokens` (chat's `prompt_tokens` /
/// `completion_tokens` fail the whole `response.completed` parse and
/// Codex retries the turn — found live in P7).
pub fn responses_usage_value(usage: &Usage) -> Value {
    serde_json::json!({
        "input_tokens": usage.prompt_tokens,
        "output_tokens": usage.completion_tokens,
        "total_tokens": usage.total_tokens,
    })
}

/// `response.failed` data (Codex classifies by `error.code`).
pub fn failed_event(resp_id: &str, code: &str, message: &str) -> Value {
    serde_json::json!({
        "type": "response.failed",
        "response": {
            "id": resp_id, "object": "response", "status": "failed",
            "error": {"code": code, "message": message},
        },
    })
}

/// Assistant message item shell for `.added` (content fills via deltas).
pub fn message_item_shell(item_id: &str) -> Value {
    serde_json::json!({
        "id": item_id, "type": "message", "role": "assistant", "content": [],
    })
}

/// Assistant message item (Codex `ResponseItem::Message`).
pub fn message_item(item_id: &str, text: &str) -> Value {
    serde_json::json!({
        "id": item_id, "type": "message", "role": "assistant",
        "content": [{"type": "output_text", "text": text, "annotations": []}],
    })
}

/// Reasoning item with per-part summaries.
pub fn reasoning_item(item_id: &str, summaries: &[String]) -> Value {
    serde_json::json!({
        "id": item_id, "type": "reasoning",
        "summary": summaries.iter().map(|text| serde_json::json!({"type": "summary_text", "text": text})).collect::<Vec<_>>(),
    })
}

/// Full response object arguments (non-stream body + `completed` payload).
pub struct ResponseObjectArgs<'a> {
    /// Response id.
    pub resp_id: &'a str,
    /// Model label.
    pub model: &'a str,
    /// Unix seconds.
    pub created: u64,
    /// `completed`.
    pub status: &'a str,
    /// Message item id (matches the streamed items so clients can join).
    pub msg_id: &'a str,
    /// Message text.
    pub text: &'a str,
    /// Reasoning item id (`None` ⇒ no reasoning item even with summaries).
    pub rs_id: Option<&'a str>,
    /// Reasoning summaries (empties dropped).
    pub summaries: &'a [String],
    /// Usage block (`None` omits it).
    pub usage: Option<&'a Usage>,
}

/// Full response object (non-stream body + `response.completed` payload).
pub fn response_object(args: ResponseObjectArgs<'_>) -> Value {
    let mut output = Vec::new();
    let kept: Vec<String> = args
        .summaries
        .iter()
        .filter(|s| !s.is_empty())
        .cloned()
        .collect();
    if !kept.is_empty()
        && let Some(rs_id) = args.rs_id
    {
        output.push(reasoning_item(rs_id, &kept));
    }
    output.push(message_item(args.msg_id, args.text));
    let mut response = serde_json::json!({
        "id": args.resp_id, "object": "response", "created_at": args.created,
        "model": args.model, "status": args.status, "output": output,
    });
    if let Some(usage) = args.usage {
        response["usage"] = responses_usage_value(usage);
    }
    response
}

/// Non-stream text: answer plus appended status lines.
fn join_text(text: &str, status_lines: &[String]) -> String {
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
    fn event_goldens_carry_codex_required_fields() {
        let created = created_event("resp_1", "m", 7);
        assert_eq!(created["response"]["id"], "resp_1");

        let delta = output_text_delta("msg_1", "Hi");
        assert_eq!(delta["delta"], "Hi");

        let added = reasoning_part_added("rs_1", 2);
        assert_eq!(added["item_id"], "rs_1");
        assert_eq!(added["part"]["summary_index"], 2);

        let rdelta = reasoning_text_delta("rs_1", 2, "think");
        assert_eq!(rdelta["summary_index"], 2);
        assert_eq!(rdelta["delta"], "think");

        let failed = failed_event("resp_1", "context_length_exceeded", "too big");
        assert_eq!(
            failed["response"]["error"]["code"],
            "context_length_exceeded"
        );

        let item = message_item("msg_1", "answer");
        assert_eq!(item["content"][0]["type"], "output_text");
    }

    #[test]
    fn response_object_orders_reasoning_first_and_omits_empties() {
        let usage = Usage {
            prompt_tokens: 1,
            completion_tokens: 2,
            total_tokens: 3,
        };
        let response = response_object(ResponseObjectArgs {
            resp_id: "resp_1",
            model: "m",
            created: 7,
            status: "completed",
            msg_id: "msg_1",
            text: "answer",
            rs_id: Some("rs_1"),
            summaries: &["r0".to_string(), "".to_string()],
            usage: Some(&usage),
        });
        assert_eq!(response["output"][0]["type"], "reasoning");
        assert_eq!(response["output"][0]["id"], "rs_1");
        assert_eq!(
            response["output"][0]["summary"].as_array().unwrap().len(),
            1
        );
        assert_eq!(response["output"][1]["type"], "message");
        assert_eq!(response["output"][1]["id"], "msg_1");
        assert_eq!(response["usage"]["input_tokens"], 1);
        assert_eq!(response["usage"]["output_tokens"], 2);
        assert_eq!(response["usage"]["total_tokens"], 3);

        let response = response_object(ResponseObjectArgs {
            resp_id: "resp_1",
            model: "m",
            created: 7,
            status: "completed",
            msg_id: "msg_1",
            text: "answer",
            rs_id: Some("rs_1"),
            summaries: &[],
            usage: Some(&usage),
        });
        assert_eq!(response["output"].as_array().unwrap().len(), 1);
    }
}
