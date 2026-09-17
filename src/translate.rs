//! OpenAI requests → MSP turn input parts (SPEC §5.1).
//!
//! This module owns the OpenAI REQUEST shapes (both endpoints) and their
//! translation into ordered MSP `text`/`image` parts. Liberal parsing
//! throughout: every field is optional, unknown fields are ignored
//! (serde default), and strict 400-on-unknown never happens — both clients
//! send fields we ignore.
//!
//! Flattening rules:
//!
//! - Chat `messages[]` → one ordered text: `system` = leading context,
//!   `user`/`assistant` with role labels, `tool` as
//!   `Tool result (<tool_call_id>): …`. `tools`/`tool_choice` accepted and
//!   ignored (N2): client tool calls are never echoed.
//! - Responses `instructions` → leading context; `input` (string or items)
//!   flattened likewise; function-call items/outputs as labeled text;
//!   `reasoning` items skipped — provider reasoning is never resubmitted.
//! - `image_url` parts → MSP `image` parts (`data:` decode or http(s) fetch
//!   with timeout + size cap; oversize ⇒ caller 400).
//! - `displayText`: `"<endpoint> <model>"` transcript label.
//! - `reasoningEffort`: tiers pass through verbatim (OQ1: muse 1.2.1
//!   accepts none/minimal/low/medium/high/xhigh/max/ultra); unknown values
//!   are ignored (logged), never fatal.
//! - `temperature` and friends: accepted, ignored, documented.

use std::time::Duration;

use serde::Deserialize;
use serde_json::Value;

// ---------------------------------------------------------------------------
// OpenAI request shapes (liberal: all-optional + unknown-tolerant)
// ---------------------------------------------------------------------------

/// `stream_options` (chat): only `include_usage` is honored.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct StreamOptions {
    /// Emit a final usage chunk on chat SSE streams.
    pub include_usage: Option<bool>,
}

/// One chat message. `tool_calls` is accepted and ignored (N2).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct ChatMessage {
    /// `system`/`user`/`assistant`/`tool` (missing → `user`; unknown →
    /// capitalized label).
    pub role: Option<String>,
    /// String, part array, or null.
    pub content: Option<MessageContent>,
    /// Accepted and ignored: the muse agent runs its own host tools.
    pub tool_calls: Option<Vec<Value>>,
    /// Folded into the `Tool result (…)` label for `tool` messages.
    pub tool_call_id: Option<String>,
}

/// Chat message content: plain string or structured parts.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum MessageContent {
    /// Plain text.
    Text(String),
    /// Ordered content parts.
    Parts(Vec<ContentPart>),
}

/// One structured content part (`type` + payload; unknown types ignored).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct ContentPart {
    /// `text`, `image_url`, … (unknown → ignored).
    #[serde(rename = "type")]
    pub part_type: Option<String>,
    /// Text payload (`text` / `input_text` parts).
    pub text: Option<String>,
    /// Image payload (`image_url` / `input_image` parts).
    pub image_url: Option<FlexibleImageUrl>,
}

/// `image_url`: object form (`{url, detail?}`) or a bare URL string.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum FlexibleImageUrl {
    /// Bare URL string.
    Url(String),
    /// Object form.
    Obj(ImageUrlObj),
}

/// Object-form image reference.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct ImageUrlObj {
    /// `data:` URL or http(s) URL.
    pub url: Option<String>,
    /// `auto`/`low`/`high`: accepted, ignored.
    pub detail: Option<String>,
}

impl FlexibleImageUrl {
    /// The URL regardless of form (`None` when absent).
    pub fn url(&self) -> Option<&str> {
        match self {
            FlexibleImageUrl::Url(url) => Some(url),
            FlexibleImageUrl::Obj(obj) => obj.url.as_deref(),
        }
    }
}

/// `POST /v1/chat/completions` request (superset-tolerant).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct ChatRequest {
    /// Model id (empty/absent → server default, no `setModel`).
    pub model: Option<String>,
    /// Conversation to flatten (empty/absent → caller 400).
    pub messages: Option<Vec<ChatMessage>>,
    /// SSE stream (`None`/`false` → single JSON).
    pub stream: Option<bool>,
    /// `{"include_usage": true}` → final usage chunk.
    pub stream_options: Option<StreamOptions>,
    /// Chat reasoning effort (tiers pass through, `max`→`ultra`).
    pub reasoning_effort: Option<String>,
}

/// Responses `reasoning` config: only `effort` is honored.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct ReasoningConfig {
    /// `none|minimal|low|medium|high|xhigh|max|ultra` (tiers pass through verbatim).
    pub effort: Option<String>,
}

/// One Responses input item (string form handled by [`ResponseInput`]).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct ResponseItem {
    /// `message`, `function_call`, `function_call_output`, `reasoning`, …
    #[serde(rename = "type")]
    pub item_type: Option<String>,
    /// Message role (default `user`).
    pub role: Option<String>,
    /// Message content (string or parts).
    pub content: Option<ResponseContent>,
    /// Function name (`function_call`).
    pub name: Option<String>,
    /// Function arguments, usually a JSON string (`function_call`).
    pub arguments: Option<Value>,
    /// Call correlation id.
    pub call_id: Option<String>,
    /// Function result (`function_call_output`).
    pub output: Option<Value>,
}

/// Responses message content: string or parts.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum ResponseContent {
    /// Plain text.
    Text(String),
    /// Ordered parts (`input_text`, `input_image`, …).
    Parts(Vec<ContentPart>),
}

/// Responses `input`: string, item array, or null.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum ResponseInput {
    /// Plain prompt text.
    Text(String),
    /// Structured items.
    Items(Vec<ResponseItem>),
}

/// `POST /v1/responses` request (superset-tolerant).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct ResponsesRequest {
    /// Model id (empty/absent → server default).
    pub model: Option<String>,
    /// Leading context block.
    pub instructions: Option<String>,
    /// Prompt: string or items.
    pub input: Option<ResponseInput>,
    /// Reasoning config (`effort` honored).
    pub reasoning: Option<ReasoningConfig>,
    /// SSE stream (`None` defaults to streaming for Codex compat… see http).
    pub stream: Option<bool>,
}

// ---------------------------------------------------------------------------
// Translation output
// ---------------------------------------------------------------------------

/// One ordered MSP input part.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InputPart {
    /// UTF-8 prompt text (non-empty).
    Text(String),
    /// Image bytes as base64 + media type.
    Image {
        /// Base64 payload (validated on the way in).
        base64: String,
        /// `image/*` media type.
        media_type: String,
    },
}

/// A fully translated turn submission (P4 → dispatch).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnInput {
    /// Ordered parts (text runs merged, images inline).
    pub parts: Vec<InputPart>,
    /// `"<endpoint> <model>"` transcript label (never model-visible).
    pub display_text: String,
    /// Validated MSP reasoning tier (`None` → host default).
    pub reasoning_effort: Option<String>,
}

impl TurnInput {
    /// Render the MSP `turn/start` `input` array.
    pub fn to_msp_json(&self) -> Value {
        Value::Array(
            self.parts
                .iter()
                .map(|part| match part {
                    InputPart::Text(text) => serde_json::json!({"type": "text", "text": text}),
                    InputPart::Image { base64, media_type } => serde_json::json!({
                        "type": "image",
                        "base64Data": base64,
                        "mediaType": media_type,
                    }),
                })
                .collect(),
        )
    }
}

/// Translation failure (all ⇒ caller 400 `invalid_request_error`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TranslateError {
    /// No text or image content survived flattening.
    EmptyPrompt,
    /// An image exceeds [`MAX_IMAGE_BYTES`].
    ImageTooLarge {
        /// Responsible size in bytes (when known).
        bytes: Option<u64>,
    },
    /// An http(s) image could not be fetched (host + status only: full URLs
    /// can carry secrets and are never echoed).
    ImageFetch {
        /// URL host (when parseable).
        host: Option<String>,
        /// What went wrong (status or transport class, never the URL).
        detail: String,
    },
    /// A `data:` URL is malformed, non-base64, or not an image.
    InvalidImageData(String),
}

impl std::fmt::Display for TranslateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TranslateError::EmptyPrompt => write!(f, "no prompt text or images in request"),
            TranslateError::ImageTooLarge { bytes } => match bytes {
                Some(n) => write!(f, "image is {n} bytes, over the {MAX_IMAGE_BYTES}-byte cap"),
                None => write!(f, "image exceeds the {MAX_IMAGE_BYTES}-byte cap"),
            },
            TranslateError::ImageFetch { host, detail } => match host {
                Some(h) => write!(f, "could not fetch image from host {h}: {detail}"),
                None => write!(f, "could not fetch image: {detail}"),
            },
            TranslateError::InvalidImageData(detail) => {
                write!(f, "invalid image data URL: {detail}")
            }
        }
    }
}

impl std::error::Error for TranslateError {}

/// Per-image byte cap (decoded bytes; oversize ⇒ caller 400).
pub const MAX_IMAGE_BYTES: u64 = 5 * 1024 * 1024;
/// http(s) image fetch timeout.
const IMAGE_FETCH_TIMEOUT: Duration = Duration::from_secs(10);

// ---------------------------------------------------------------------------
// Entry points
// ---------------------------------------------------------------------------

/// Translate a chat request into MSP turn input (`endpoint = "chat"`).
pub async fn translate_chat(request: &ChatRequest) -> Result<TurnInput, TranslateError> {
    let mut blocks = Vec::new();
    for message in request.messages.as_deref().unwrap_or(&[]) {
        if let Some(block) = message_block(message).await? {
            blocks.push(block);
        }
    }
    let effort = map_reasoning_effort(request.reasoning_effort.as_deref());
    finish(blocks, "chat", request.model.as_deref(), effort)
}

/// Translate a responses request (`endpoint = "responses"`).
pub async fn translate_responses(request: &ResponsesRequest) -> Result<TurnInput, TranslateError> {
    let mut blocks = Vec::new();
    if let Some(instructions) = request.instructions.as_deref().filter(|s| !s.is_empty()) {
        blocks.push(TextBlock::labelled(
            "Instructions",
            instructions.to_string(),
        ));
    }
    match &request.input {
        None => {}
        Some(ResponseInput::Text(text)) if !text.is_empty() => {
            blocks.push(TextBlock::labelled("User", text.clone()));
        }
        Some(ResponseInput::Text(_)) => {}
        Some(ResponseInput::Items(items)) => {
            for item in items {
                if let Some(block) = response_item_block(item).await? {
                    blocks.push(block);
                }
            }
        }
    }
    let effort = map_reasoning_effort(request.reasoning.as_ref().and_then(|r| r.effort.as_deref()));
    finish(blocks, "responses", request.model.as_deref(), effort)
}

/// Map a client reasoning effort to the MSP tier vocabulary: tiers pass
/// through verbatim (SPEC §5.1); unknown values are ignored (logged),
/// never fatal.
pub fn map_reasoning_effort(raw: Option<&str>) -> Option<String> {
    let tier = raw?.trim().to_lowercase();
    match tier.as_str() {
        "none" | "minimal" | "low" | "medium" | "high" | "xhigh" | "max" | "ultra" => Some(tier),
        "" => None,
        unknown => {
            tracing::debug!(effort = unknown, "ignoring unknown reasoning effort");
            None
        }
    }
}

// ---------------------------------------------------------------------------
// Flattening
// ---------------------------------------------------------------------------

/// One flattened block: labeled text plus any inline images.
struct TextBlock {
    label: String,
    text: String,
    images: Vec<InputPart>,
}

impl TextBlock {
    fn labelled(label: &str, text: String) -> Self {
        Self {
            label: label.to_string(),
            text,
            images: Vec::new(),
        }
    }

    /// Render `Label: text` (empty text ⇒ no text line, images still kept).
    fn render(&self) -> Option<String> {
        if self.text.is_empty() {
            return None;
        }
        Some(format!("{}: {}", self.label, self.text))
    }
}

/// Flatten one chat message (`None` ⇒ nothing worth sending).
async fn message_block(message: &ChatMessage) -> Result<Option<TextBlock>, TranslateError> {
    let role = message.role.as_deref().unwrap_or("user");
    let (text, images) = message_content(message.content.as_ref()).await?;
    if text.is_empty() && images.is_empty() {
        return Ok(None);
    }
    let label = match role {
        "system" => "System".to_string(),
        "user" => "User".to_string(),
        "assistant" => "Assistant".to_string(),
        "tool" => format!(
            "Tool result ({})",
            message.tool_call_id.as_deref().unwrap_or("unknown")
        ),
        other => capitalize(other),
    };
    Ok(Some(TextBlock {
        label,
        text,
        images,
    }))
}

/// Flatten one responses input item (`None` ⇒ skipped: reasoning items and
/// unrecognized shapes are never resubmitted).
async fn response_item_block(item: &ResponseItem) -> Result<Option<TextBlock>, TranslateError> {
    match item.item_type.as_deref().unwrap_or("message") {
        // Provider reasoning is never resubmitted verbatim.
        "reasoning" => Ok(None),
        "message" => {
            let (text, images) = match &item.content {
                None => (String::new(), Vec::new()),
                Some(ResponseContent::Text(text)) => (text.clone(), Vec::new()),
                Some(ResponseContent::Parts(parts)) => content_parts(parts).await?,
            };
            if text.is_empty() && images.is_empty() {
                return Ok(None);
            }
            let role = item.role.as_deref().unwrap_or("user");
            let label = match role {
                "system" => "System".to_string(),
                "user" => "User".to_string(),
                "assistant" => "Assistant".to_string(),
                other => capitalize(other),
            };
            Ok(Some(TextBlock {
                label,
                text,
                images,
            }))
        }
        "function_call" => {
            let name = item.name.as_deref().unwrap_or("unknown");
            let call = item.call_id.as_deref().unwrap_or("unknown");
            let args = render_value(item.arguments.as_ref());
            Ok(Some(TextBlock::labelled(
                &format!("Function call {name} ({call})"),
                args,
            )))
        }
        "function_call_output" => {
            let call = item.call_id.as_deref().unwrap_or("unknown");
            let output = render_value(item.output.as_ref());
            Ok(Some(TextBlock::labelled(
                &format!("Function result ({call})"),
                output,
            )))
        }
        other => {
            tracing::debug!(item_type = other, "skipping unrecognized responses item");
            Ok(None)
        }
    }
}

/// Flatten chat message content into joined text + images (in order).
async fn message_content(
    content: Option<&MessageContent>,
) -> Result<(String, Vec<InputPart>), TranslateError> {
    match content {
        None => Ok((String::new(), Vec::new())),
        Some(MessageContent::Text(text)) => Ok((text.clone(), Vec::new())),
        Some(MessageContent::Parts(parts)) => content_parts(parts).await,
    }
}

/// Flatten content parts: text segments join in order, images inline.
async fn content_parts(parts: &[ContentPart]) -> Result<(String, Vec<InputPart>), TranslateError> {
    let mut text = String::new();
    let mut images = Vec::new();
    for part in parts {
        match part.part_type.as_deref().unwrap_or("text") {
            "text" | "input_text" => {
                if let Some(segment) = part.text.as_deref() {
                    text.push_str(segment);
                }
            }
            "image_url" | "input_image" => {
                if let Some(image_ref) = part.image_url.as_ref().and_then(FlexibleImageUrl::url) {
                    images.push(resolve_image(image_ref).await?);
                } else {
                    tracing::debug!("skipping image part without a URL");
                }
            }
            other => {
                tracing::debug!(part_type = other, "skipping unrecognized content part");
            }
        }
    }
    Ok((text, images))
}

/// Assemble blocks into MSP parts in encounter order: one text part per
/// block (non-first text parts carry a `"\n\n"` prefix, so the assembly is
/// deterministic no matter how MSP joins multiple text parts), images inline
/// after their block's text. Empty text parts are dropped; zero parts ⇒
/// caller 400.
fn finish(
    blocks: Vec<TextBlock>,
    endpoint: &str,
    model: Option<&str>,
    reasoning_effort: Option<String>,
) -> Result<TurnInput, TranslateError> {
    let mut parts = Vec::new();
    let mut first_text = true;
    for block in &blocks {
        if let Some(rendered) = block.render() {
            if first_text {
                parts.push(InputPart::Text(rendered));
                first_text = false;
            } else {
                parts.push(InputPart::Text(format!("\n\n{rendered}")));
            }
        }
        parts.extend(block.images.iter().cloned());
    }
    if parts.is_empty() {
        return Err(TranslateError::EmptyPrompt);
    }
    let model_label = model
        .map(str::trim)
        .filter(|m| !m.is_empty())
        .unwrap_or("default");
    Ok(TurnInput {
        parts,
        display_text: format!("{endpoint} {model_label}"),
        reasoning_effort,
    })
}

fn capitalize(role: &str) -> String {
    let mut chars = role.chars();
    match chars.next() {
        None => "User".to_string(),
        Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
    }
}

/// Render an echoed JSON value: strings as-is, else compact JSON.
fn render_value(value: Option<&Value>) -> String {
    match value {
        None => String::new(),
        Some(Value::String(s)) => s.clone(),
        Some(other) => serde_json::to_string(other).unwrap_or_default(),
    }
}

// ---------------------------------------------------------------------------
// Images
// ---------------------------------------------------------------------------

/// Shared fetch client (built once; honors proxy env).
fn fetch_client() -> reqwest::Client {
    static CLIENT: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();
    CLIENT
        .get_or_init(|| {
            reqwest::Client::builder()
                .timeout(IMAGE_FETCH_TIMEOUT)
                .build()
                .expect("reqwest client builds")
        })
        .clone()
}

/// Resolve one `image_url` into an MSP image part: `data:` URLs decode
/// locally, http(s) URLs fetch with timeout + size cap. Anything else ⇒
/// caller 400 (full URLs are never logged or echoed).
async fn resolve_image(url: &str) -> Result<InputPart, TranslateError> {
    if let Some(rest) = url.strip_prefix("data:") {
        return decode_data_url(rest);
    }
    if url.starts_with("http://") || url.starts_with("https://") {
        return fetch_image(url).await;
    }
    Err(TranslateError::InvalidImageData(
        "URL must be a data: URL or http(s)".to_string(),
    ))
}

/// Decode `"<media-type>;base64,<payload>"` (the `data:` prefix already
/// stripped). The payload must be strict base64 of an `image/*` type within
/// [`MAX_IMAGE_BYTES`].
fn decode_data_url(rest: &str) -> Result<InputPart, TranslateError> {
    let invalid = |detail: &str| TranslateError::InvalidImageData(detail.to_string());
    let (head, payload) = rest
        .split_once(',')
        .ok_or_else(|| invalid("missing ',' separator"))?;
    let base64_payload = head
        .strip_suffix(";base64")
        .ok_or_else(|| invalid("only base64 data URLs are supported"))?;
    let media_type = base64_payload.split(';').next().unwrap_or("");
    if !media_type.starts_with("image/") {
        return Err(invalid("media type must be image/*"));
    }
    let payload = payload.trim();
    let bytes = decode_base64(payload).ok_or_else(|| invalid("payload is not valid base64"))?;
    if bytes.len() as u64 > MAX_IMAGE_BYTES {
        return Err(TranslateError::ImageTooLarge {
            bytes: Some(bytes.len() as u64),
        });
    }
    Ok(InputPart::Image {
        base64: payload.to_string(),
        media_type: media_type.to_string(),
    })
}

/// Strict standard-base64 decode (the exact validated string rides to MSP).
fn decode_base64(payload: &str) -> Option<Vec<u8>> {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD
        .decode(payload)
        .ok()
}

/// Fetch an http(s) image: 10 s timeout, `Content-Length` pre-check, chunked
/// read capped at [`MAX_IMAGE_BYTES`], `image/*` content type required.
async fn fetch_image(url: &str) -> Result<InputPart, TranslateError> {
    let host = url_host(url);
    let fetch_fail = |detail: String| TranslateError::ImageFetch {
        host: host.clone(),
        detail,
    };
    let response = fetch_client()
        .get(url)
        .send()
        .await
        .map_err(|e| fetch_fail(fetch_error_kind(&e)))?;
    if !response.status().is_success() {
        return Err(fetch_fail(format!("HTTP {}", response.status())));
    }
    if let Some(len) = response.content_length()
        && len > MAX_IMAGE_BYTES
    {
        return Err(TranslateError::ImageTooLarge { bytes: Some(len) });
    }
    let media_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.split(';').next().unwrap_or("").trim().to_string())
        .unwrap_or_default();
    if !media_type.starts_with("image/") {
        return Err(fetch_fail(format!(
            "content type '{media_type}' is not an image"
        )));
    }
    let mut bytes = Vec::new();
    let mut stream = response.bytes_stream();
    use futures::StreamExt as _;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| fetch_fail(fetch_error_kind(&e)))?;
        bytes.extend_from_slice(&chunk);
        if bytes.len() as u64 > MAX_IMAGE_BYTES {
            return Err(TranslateError::ImageTooLarge {
                bytes: Some(bytes.len() as u64),
            });
        }
    }
    if bytes.is_empty() {
        return Err(fetch_fail("empty body".to_string()));
    }
    use base64::Engine as _;
    Ok(InputPart::Image {
        base64: base64::engine::general_purpose::STANDARD.encode(&bytes),
        media_type,
    })
}

/// URL host for diagnostics (never the full URL: query strings can carry
/// secrets). Crude parse, no new deps: scheme + authority + path split.
fn url_host(url: &str) -> Option<String> {
    let after_scheme = url.split_once("://")?.1;
    let authority = after_scheme.split('/').next()?;
    // Strip userinfo and port; keep it short.
    let host = authority.rsplit('@').next()?.split(':').next()?;
    (!host.is_empty()).then(|| host.chars().take(128).collect())
}

/// Classify a fetch failure without echoing the URL.
fn fetch_error_kind(error: &reqwest::Error) -> String {
    if error.is_timeout() {
        "timed out".to_string()
    } else if error.is_connect() {
        "connection failed".to_string()
    } else if error.is_redirect() {
        "redirect failed".to_string()
    } else if error.is_body() || error.is_decode() {
        "body read failed".to_string()
    } else {
        "request failed".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn chat_request(messages: Value) -> ChatRequest {
        serde_json::from_value(json!({
            "model": "muse-spark-1.3",
            "messages": messages,
            "stream": true,
            "stream_options": {"include_usage": true},
            // Hermes-shaped extras: accepted, ignored.
            "reasoning_effort": "high",
            "thinking": {"type": "enabled"},
            "options": {"num_ctx": 8192},
            "prompt_cache_key": "abc",
            "temperature": 0.7,
            "tools": [{"type": "function", "function": {"name": "f"}}],
            "tool_choice": "auto",
        }))
        .expect("liberal parse")
    }

    fn texts(input: &TurnInput) -> Vec<String> {
        input
            .parts
            .iter()
            .filter_map(|p| match p {
                InputPart::Text(t) => Some(t.clone()),
                InputPart::Image { .. } => None,
            })
            .collect()
    }

    #[tokio::test]
    async fn chat_flatten_golden() {
        let request = chat_request(json!([
            {"role": "system", "content": "Be terse."},
            {"role": "user", "content": "Hello"},
            {"role": "assistant", "content": "Hi.", "tool_calls": [
                {"id": "c1", "type": "function", "function": {"name": "f", "arguments": "{}"}}
            ]},
            {"role": "tool", "tool_call_id": "c1", "content": "42"},
        ]));
        let input = translate_chat(&request).await.expect("translate");
        assert_eq!(
            texts(&input),
            vec![
                "System: Be terse.",
                "\n\nUser: Hello",
                "\n\nAssistant: Hi.",
                "\n\nTool result (c1): 42",
            ]
        );
        assert_eq!(input.display_text, "chat muse-spark-1.3");
        assert_eq!(input.reasoning_effort.as_deref(), Some("high"));
    }

    #[tokio::test]
    async fn unknown_roles_capitalize_and_contentless_messages_skip() {
        let request = chat_request(json!([
            {"role": "developer", "content": "note"},
            {"role": "assistant", "content": null, "tool_calls": []},
            {"role": "user", "content": [{"type": "text", "text": "a"}, {"type": "text", "text": "b"}]},
            {"role": "user", "content": [{"type": "input_audio", "input_audio": {}}]},
        ]));
        let input = translate_chat(&request).await.expect("translate");
        assert_eq!(texts(&input), vec!["Developer: note", "\n\nUser: ab"]);
    }

    #[tokio::test]
    async fn empty_prompt_is_a_caller_error() {
        for request in [
            chat_request(json!([])),
            chat_request(json!([{"role": "user", "content": null}])),
            chat_request(json!([{"role": "assistant", "tool_calls": []}])),
        ] {
            assert_eq!(
                translate_chat(&request).await,
                Err(TranslateError::EmptyPrompt)
            );
        }
        let request: ResponsesRequest =
            serde_json::from_value(json!({"model": "m"})).expect("parse");
        assert_eq!(
            translate_responses(&request).await,
            Err(TranslateError::EmptyPrompt)
        );
    }

    #[tokio::test]
    async fn image_data_url_decodes() {
        let request = chat_request(json!([
            {"role": "user", "content": [
                {"type": "text", "text": "see:"},
                {"type": "image_url", "image_url": {"url": "data:image/png;base64,aGVsbG8="}},
                {"type": "image_url", "image_url": "data:image/jpeg;base64,aGVsbG8="},
            ]},
        ]));
        let input = translate_chat(&request).await.expect("translate");
        assert_eq!(input.parts.len(), 3);
        assert_eq!(texts(&input), vec!["User: see:"]);
        match &input.parts[1..] {
            [
                InputPart::Image { base64, media_type },
                InputPart::Image {
                    base64: b2,
                    media_type: m2,
                },
            ] => {
                assert_eq!(base64, "aGVsbG8=");
                assert_eq!(media_type, "image/png");
                assert_eq!(b2, "aGVsbG8=");
                assert_eq!(m2, "image/jpeg");
            }
            other => panic!("expected two images, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn image_data_url_rejects_garbage() {
        for (url, expect) in [
            ("data:image/png;base64,!!!not-base64!!!", "base64"),
            ("data:text/plain;base64,aGVsbG8=", "image/*"),
            ("data:image/png,aGVsbG8=", "base64"),
            ("data:image/png;base64aGVsbG8=", "','"),
            ("ftp://example.com/x.png", "http(s)"),
        ] {
            let request = chat_request(json!([
                {"role": "user", "content": [
                    {"type": "image_url", "image_url": {"url": url}},
                ]},
            ]));
            match translate_chat(&request).await {
                Err(TranslateError::InvalidImageData(detail)) => {
                    assert!(detail.contains(expect), "{url}: {detail}")
                }
                other => panic!("{url}: expected InvalidImageData, got {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn image_data_url_oversize_is_400() {
        use base64::Engine as _;
        let big =
            base64::engine::general_purpose::STANDARD
                .encode(vec![0u8; MAX_IMAGE_BYTES as usize + 1]);
        let request = chat_request(json!([
            {"role": "user", "content": [
                {"type": "image_url", "image_url": {"url": format!("data:image/png;base64,{big}")}},
            ]},
        ]));
        assert!(matches!(
            translate_chat(&request).await,
            Err(TranslateError::ImageTooLarge { .. })
        ));
    }

    /// Serve one canned HTTP response on loopback; returns the URL.
    async fn serve_once(head: &str, body: &[u8]) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let url = format!(
            "http://127.0.0.1:{}/img.png",
            listener.local_addr().unwrap().port()
        );
        let response = [head.as_bytes(), body].concat();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept");
            // Read the request head, then answer (HTTP/1.1, close-delimited).
            let mut buf = vec![0u8; 8192];
            use tokio::io::AsyncReadExt as _;
            use tokio::io::AsyncWriteExt as _;
            let mut read = 0;
            while read < buf.len() {
                let n = socket.read(&mut buf[read..]).await.expect("read");
                if n == 0 {
                    break;
                }
                read += n;
                if buf[..read].windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }
            socket.write_all(&response).await.expect("write");
            socket.shutdown().await.ok();
        });
        url
    }

    async fn fetch_via_chat(url: &str) -> Result<TurnInput, TranslateError> {
        let request = chat_request(json!([
            {"role": "user", "content": [
                {"type": "text", "text": "q"},
                {"type": "image_url", "image_url": {"url": url}},
            ]},
        ]));
        translate_chat(&request).await
    }

    #[tokio::test]
    async fn image_http_fetch_round_trips_bytes() {
        let url = serve_once(
            "HTTP/1.1 200 OK\r\nContent-Type: image/png\r\nContent-Length: 4\r\nConnection: close\r\n\r\n",
            b"PNG!",
        )
        .await;
        let input = fetch_via_chat(&url).await.expect("fetch");
        match input.parts.as_slice() {
            [InputPart::Text(_), InputPart::Image { base64, media_type }] => {
                use base64::Engine as _;
                assert_eq!(
                    base64::engine::general_purpose::STANDARD
                        .decode(base64)
                        .unwrap(),
                    b"PNG!"
                );
                assert_eq!(media_type, "image/png");
            }
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test]
    async fn image_http_fetch_failures_are_caller_errors() {
        // 404: host named, full URL never echoed.
        let url = serve_once(
            "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            b"",
        )
        .await;
        match fetch_via_chat(&url).await {
            Err(TranslateError::ImageFetch { host, detail }) => {
                assert_eq!(host.as_deref(), Some("127.0.0.1"));
                assert!(detail.contains("404"), "{detail}");
            }
            other => panic!("expected ImageFetch, got {other:?}"),
        }
        // Non-image content type.
        let url = serve_once(
            "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: 2\r\nConnection: close\r\n\r\n",
            b"hi",
        )
        .await;
        assert!(matches!(
            fetch_via_chat(&url).await,
            Err(TranslateError::ImageFetch { .. })
        ));
        // Lying Content-Length pre-check trips before the body matters.
        let url = serve_once(
            &format!(
                "HTTP/1.1 200 OK\r\nContent-Type: image/png\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                MAX_IMAGE_BYTES + 1
            ),
            b"tiny",
        )
        .await;
        assert!(matches!(
            fetch_via_chat(&url).await,
            Err(TranslateError::ImageTooLarge { .. })
        ));
        // Unreachable host.
        match fetch_via_chat("http://127.0.0.1:9/img.png").await {
            Err(TranslateError::ImageFetch { host, detail }) => {
                assert_eq!(host.as_deref(), Some("127.0.0.1"));
                assert!(!detail.contains("127.0.0.1:9/img.png"), "{detail}");
            }
            other => panic!("expected ImageFetch, got {other:?}"),
        }
    }

    #[test]
    fn reasoning_effort_mapping() {
        for tier in [
            "none", "minimal", "low", "medium", "high", "xhigh", "max", "ultra",
        ] {
            assert_eq!(map_reasoning_effort(Some(tier)).as_deref(), Some(tier));
        }
        assert_eq!(map_reasoning_effort(Some("HIGH")).as_deref(), Some("high"));
        assert_eq!(map_reasoning_effort(None), None);
        assert_eq!(map_reasoning_effort(Some("")), None);
        assert_eq!(map_reasoning_effort(Some("extreme")), None);
    }

    #[tokio::test]
    async fn responses_flatten_golden() {
        let request: ResponsesRequest = serde_json::from_value(json!({
            "model": "muse-spark-1.3",
            "instructions": "Be brief.",
            "input": [
                {"type": "message", "role": "user", "content": "Run it."},
                {"type": "function_call", "name": "shell", "call_id": "c1",
                 "arguments": "{\"cmd\": \"ls\"}"},
                {"type": "function_call_output", "call_id": "c1", "output": "a\nb"},
                {"type": "reasoning", "summary": [{"text": "SECRET THINKING"}]},
                {"type": "future_thing", "content": "skip me"},
            ],
            "reasoning": {"effort": "max", "summary": "auto"},
            "stream": true,
            // Codex-shaped extras: accepted, ignored.
            "client_metadata": {}, "prompt_cache_key": "k", "service_tier": "x",
            "text": {}, "include": [], "tools": [], "store": false,
            "previous_response_id": "resp_1", "conversation": "c",
        }))
        .expect("liberal parse");
        let input = translate_responses(&request).await.expect("translate");
        assert_eq!(
            texts(&input),
            vec![
                "Instructions: Be brief.",
                "\n\nUser: Run it.",
                "\n\nFunction call shell (c1): {\"cmd\": \"ls\"}",
                "\n\nFunction result (c1): a\nb",
            ]
        );
        assert_eq!(input.display_text, "responses muse-spark-1.3");
        assert_eq!(input.reasoning_effort.as_deref(), Some("max"));
        let joined: String = texts(&input).concat();
        assert!(!joined.contains("SECRET"), "reasoning never resubmitted");
        assert!(!joined.contains("skip me"), "unknown items skipped");
    }

    #[tokio::test]
    async fn responses_string_input_flattens() {
        let request: ResponsesRequest =
            serde_json::from_value(json!({"input": "plain prompt"})).expect("parse");
        let input = translate_responses(&request).await.expect("translate");
        assert_eq!(texts(&input), vec!["User: plain prompt"]);
        assert_eq!(input.display_text, "responses default");
    }

    #[test]
    fn msp_json_shape_is_ordered_and_typed() {
        let input = TurnInput {
            parts: vec![
                InputPart::Text("a".to_string()),
                InputPart::Image {
                    base64: "QUJD".to_string(),
                    media_type: "image/png".to_string(),
                },
            ],
            display_text: "chat m".to_string(),
            reasoning_effort: None,
        };
        assert_eq!(
            input.to_msp_json(),
            json!([
                {"type": "text", "text": "a"},
                {"type": "image", "base64Data": "QUJD", "mediaType": "image/png"},
            ])
        );
    }

    #[test]
    fn url_host_never_echoes_path_or_query() {
        assert_eq!(
            url_host("https://example.com/a/b?token=secret").as_deref(),
            Some("example.com")
        );
        assert_eq!(
            url_host("http://user:pw@internal:8080/x").as_deref(),
            Some("internal")
        );
        assert_eq!(url_host("not a url"), None);
        assert_eq!(url_host("data:image/png;base64,x"), None);
    }
}
