//! Per-request view fold: notification stream → exactly-once output events.
//!
//! The fold is pure: [`TurnFold::feed`] takes one `(method, params)` pair and
//! returns zero or more [`OutputEvent`]s. Delivery guarantees implemented
//! (SPEC §4.3, fold-model guide):
//!
//! - Deltas accumulate per `(itemId, field)`; `item/completed` is truth:
//!   a full object emits only the suffix past already-streamed text, so gap
//!   refills converge without double-rendering.
//! - Replace-iff-higher-`revision` on full objects; `updated`/`completed`
//!   are accepted for never-`started` items.
//! - `done` + `usage_seen` idempotency sets make re-feeds (gap pages,
//!   redelivery) converge.
//! - Terminals are open: `completed`→ok, `cancelled`→gone, everything else
//!   (including unknown strings)→failure. `turn/unqueued`/`turn/retracted`
//!   settle; `turn/retryScheduled` never settles.
//! - Usage accumulates counted-once per `viewCursor`; `cumulative`
//!   reconciles at close. `contextUsage` is never tracked (a null snapshot
//!   arm is normal and must never blank known state).
//! - Unknown methods/kinds/enums: debug-log + ignore; the fold survives.
//!
//! Gap recovery is split across layers: the fold emits [`OutputEvent::Gap`];
//! dispatch (P4) buffers live events, pages `(after, next)`, and re-feeds.

use std::collections::{HashMap, HashSet};

use serde_json::Value;

/// Counted-once OpenAI-style token usage for one turn.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Usage {
    /// Counted-once prompt tokens.
    pub prompt_tokens: u64,
    /// Raw output tokens.
    pub completion_tokens: u64,
    /// Honest total.
    pub total_tokens: u64,
}

/// How the awaited turn ended (settles the fold exactly once).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TurnOutcome {
    /// `terminal: "completed"`.
    Completed {
        /// Reconciled usage.
        usage: Usage,
    },
    /// `terminal: "cancelled"`, `turn/unqueued`, or `turn/retracted`.
    Cancelled {
        /// Reconciled usage.
        usage: Usage,
    },
    /// Any other terminal (including unknown strings).
    Failed {
        /// `error.kind`, or the terminal string when no error rode along.
        kind: String,
        /// Human text (diagnostics only; branch on `kind`).
        message: String,
        /// The server's resubmission judgment (`false` when unstated).
        retryable: bool,
        /// Reconciled usage.
        usage: Usage,
    },
}

impl TurnOutcome {
    /// Usage regardless of outcome.
    pub fn usage(&self) -> &Usage {
        match self {
            TurnOutcome::Completed { usage }
            | TurnOutcome::Cancelled { usage }
            | TurnOutcome::Failed { usage, .. } => usage,
        }
    }
}

/// One folded output event (consumed by dispatch, P4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutputEvent {
    /// `agentMessage` text (streamed or gap-filled).
    ContentDelta(String),
    /// `reasoning` summary part text (`part` = the `summary.N` index, so
    /// part breaks stay clean downstream).
    ReasoningDelta {
        /// Summary part index.
        part: u32,
        /// Appended text.
        text: String,
    },
    /// Marked progress text (`[tool: name] …`, retry notices, truncation
    /// markers, generic fallback text). Never silent, always labeled.
    StatusLine(String),
    /// `view/gap`: dispatch must page `(after, next)` and re-feed before
    /// treating the fold as current.
    Gap {
        /// Exclusive lower bound (last delivered cursor).
        after: String,
        /// Exclusive upper bound (delivery resumes here).
        next: String,
    },
    /// The awaited turn settled (exactly once per fold).
    Terminal(TurnOutcome),
}

/// Item kinds of the v1 schema. Anything else is a future kind and renders
/// generically via `fallbackText` (schema guidance); known-but-unmapped
/// kinds (`subagent`, `reminderChild`, …) stay silent — they are
/// host-internal noise, and their absence loses nothing durable.
const KNOWN_ITEM_KINDS: &[&str] = &[
    "userMessage",
    "agentMessage",
    "reasoning",
    "toolCall",
    "userShell",
    "subagent",
    "workflow",
    "reminderChild",
    "compaction",
];

fn str_field<'a>(value: &'a Value, key: &str) -> &'a str {
    value.get(key).and_then(Value::as_str).unwrap_or("")
}

fn num_field(value: &Value, key: &str) -> u64 {
    value.get(key).and_then(Value::as_u64).unwrap_or(0)
}

/// Per-item fold state.
struct ItemState {
    kind: String,
    /// Highest full-object revision folded (replace-iff-higher).
    rev: u64,
    /// Text already emitted per field path (for suffix diffs on full truth).
    streamed: HashMap<String, String>,
    tool_announced: bool,
    tool_terminal: bool,
    fallback_announced: bool,
    truncated_announced: bool,
}

impl ItemState {
    fn new(kind: &str) -> Self {
        Self {
            kind: kind.to_string(),
            rev: 0,
            streamed: HashMap::new(),
            tool_announced: false,
            tool_terminal: false,
            fallback_announced: false,
            truncated_announced: false,
        }
    }
}

/// Per-request fold over one session's view events for one awaited turn.
pub struct TurnFold {
    session_id: String,
    turn_id: String,
    settled: bool,
    cursor: Option<String>,
    items: HashMap<String, ItemState>,
    /// Items whose terminal full object was folded (late/duplicate deltas
    /// for these are dropped).
    done: HashSet<String>,
    /// `viewCursor`s of consumed `session/tokenUsage` events (counted-once).
    usage_seen: HashSet<String>,
    prompt_sum: u64,
    completion_sum: u64,
    cumulative: Option<Usage>,
    approval_mode: Option<String>,
    model: Option<String>,
}

impl TurnFold {
    /// New fold awaiting `turn_id` on `session_id`.
    pub fn new(session_id: &str, turn_id: &str) -> Self {
        Self {
            session_id: session_id.to_string(),
            turn_id: turn_id.to_string(),
            settled: false,
            cursor: None,
            items: HashMap::new(),
            done: HashSet::new(),
            usage_seen: HashSet::new(),
            prompt_sum: 0,
            completion_sum: 0,
            cumulative: None,
            approval_mode: None,
            model: None,
        }
    }

    /// The awaited turn id (for `turn/cancel` on client disconnect).
    pub fn turn_id(&self) -> &str {
        &self.turn_id
    }

    /// Whether a terminal was emitted (later events are ignored).
    pub fn is_settled(&self) -> bool {
        self.settled
    }

    /// Last tracked `viewCursor` (every routed event advances it).
    pub fn cursor(&self) -> Option<&str> {
        self.cursor.as_deref()
    }

    /// Folded effective approval mode (`session/started`,
    /// `session/approvalModeChanged`). Dispatch verifies this against the
    /// request; a mismatch fails, never silently downgrades.
    pub fn approval_mode(&self) -> Option<&str> {
        self.approval_mode.as_deref()
    }

    /// Folded model (`session/modelChanged`), the source of truth for what
    /// actually served (never the `setModel` echo).
    pub fn model(&self) -> Option<&str> {
        self.model.as_deref()
    }

    /// Fold one event. Returns the output events in emission order.
    pub fn feed(&mut self, method: &str, params: &Value) -> Vec<OutputEvent> {
        if self.settled {
            return Vec::new();
        }
        if !self.session_matches(method, params) {
            return Vec::new();
        }
        // Cursor-track everything (rule 9). Unknown events without a cursor
        // simply don't advance it.
        if let Some(cursor) = params.get("viewCursor").and_then(Value::as_str) {
            self.cursor = Some(cursor.to_string());
        }

        match method {
            "turn/completed" => self.feed_turn_completed(params),
            "turn/unqueued" | "turn/retracted" => self.feed_turn_gone(method, params),
            "turn/retryScheduled" => self.feed_retry_scheduled(params),
            "turn/started" => Vec::new(),
            "item/started" | "item/updated" | "item/completed" => self.feed_item(method, params),
            "item/delta" => self.feed_delta(params),
            "session/tokenUsage" => self.feed_usage(params),
            "session/approvalModeChanged" => {
                let mode = str_field(params, "mode");
                if !mode.is_empty() {
                    self.approval_mode = Some(mode.to_string());
                }
                Vec::new()
            }
            "session/started" => {
                if let Some(mode) = params
                    .get("session")
                    .and_then(|s| s.get("approvalMode"))
                    .and_then(|m| m.get("mode"))
                    .and_then(Value::as_str)
                {
                    self.approval_mode = Some(mode.to_string());
                }
                Vec::new()
            }
            "session/modelChanged" => {
                let model = str_field(params, "modelId");
                if !model.is_empty() {
                    self.model = Some(model.to_string());
                }
                Vec::new()
            }
            "view/gap" => {
                let after = str_field(params, "after");
                let next = str_field(params, "next");
                if after.is_empty() || next.is_empty() {
                    tracing::warn!("malformed view/gap without after/next cursors");
                    return Vec::new();
                }
                vec![OutputEvent::Gap {
                    after: after.to_string(),
                    next: next.to_string(),
                }]
            }
            // Cursor-tracked above, otherwise ignored: session-state facts,
            // approval/user-input view legs (policy rides the server
            // requests), and context pressure (never tracked: a null arm is
            // normal and must never blank known usage).
            "session/contextUsage"
            | "session/goalChanged"
            | "session/todoListChanged"
            | "session/branchChanged"
            | "session/nameChanged"
            | "session/reasoningEffortChanged"
            | "session/modelRouteUnserved"
            | "approval/requested"
            | "approval/updated"
            | "approval/resolved"
            | "userInput/requested"
            | "userInput/settled" => Vec::new(),
            _ => {
                tracing::debug!(method, "ignoring unknown notification method");
                Vec::new()
            }
        }
    }

    /// Route by session: `session/started` nests the id under
    /// `params.session`; everything else carries `params.sessionId`.
    fn session_matches(&self, method: &str, params: &Value) -> bool {
        let session = if method == "session/started" {
            params
                .get("session")
                .and_then(|s| s.get("sessionId"))
                .and_then(Value::as_str)
                .unwrap_or("")
        } else {
            str_field(params, "sessionId")
        };
        session == self.session_id
    }

    /// Turn-scope check: an absent/null turn id passes (tolerant position),
    /// a present one must equal the awaited turn.
    fn turn_matches(&self, params: &Value) -> bool {
        match params.get("turnId").and_then(Value::as_str) {
            None => true,
            Some("") => true,
            Some(turn) => turn == self.turn_id,
        }
    }

    /// Reconciled usage: the last `cumulative` block when one landed,
    /// else the counted-once sums.
    fn final_usage(&self) -> Usage {
        if let Some(cumulative) = &self.cumulative {
            return cumulative.clone();
        }
        Usage {
            prompt_tokens: self.prompt_sum,
            completion_tokens: self.completion_sum,
            total_tokens: self.prompt_sum.saturating_add(self.completion_sum),
        }
    }

    fn settle(&mut self, outcome: TurnOutcome) -> Vec<OutputEvent> {
        self.settled = true;
        vec![OutputEvent::Terminal(outcome)]
    }

    fn feed_turn_completed(&mut self, params: &Value) -> Vec<OutputEvent> {
        let turn = str_field(params, "turnId");
        if turn.is_empty() || turn != self.turn_id {
            return Vec::new();
        }
        let terminal = str_field(params, "terminal");
        let usage = self.final_usage();
        match terminal {
            "completed" => self.settle(TurnOutcome::Completed { usage }),
            "cancelled" => self.settle(TurnOutcome::Cancelled { usage }),
            // Open terminal vocabulary: anything else (including unknown
            // strings) is a failure. Never match exhaustively.
            other => {
                let error = params.get("error");
                let kind = error
                    .and_then(|e| e.get("kind"))
                    .and_then(Value::as_str)
                    .filter(|k| !k.is_empty())
                    .unwrap_or(other);
                let message = error
                    .and_then(|e| e.get("message"))
                    .and_then(Value::as_str)
                    .filter(|m| !m.is_empty())
                    .map(str::to_string)
                    .or_else(|| {
                        let reason = str_field(params, "reason");
                        (!reason.is_empty()).then(|| reason.to_string())
                    })
                    .unwrap_or_else(|| format!("turn ended with terminal '{other}'"));
                let retryable = error
                    .and_then(|e| e.get("retryable"))
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                tracing::warn!(
                    terminal = other,
                    kind,
                    retryable,
                    "turn failed (terminal open-enum arm)"
                );
                self.settle(TurnOutcome::Failed {
                    kind: kind.to_string(),
                    message,
                    retryable,
                    usage,
                })
            }
        }
    }

    fn feed_turn_gone(&mut self, method: &str, params: &Value) -> Vec<OutputEvent> {
        // Reclaimed/retracted: the turn never runs to completion, so no
        // `turn/completed` will ever arrive — settle now as cancelled.
        // Match by turn id; `turn/retracted` also carries the submitting
        // `commandId`, which equals the turn id for fresh turns.
        let turn = str_field(params, "turnId");
        let command = str_field(params, "commandId");
        if turn != self.turn_id && command != self.turn_id {
            return Vec::new();
        }
        tracing::info!(method, turn = %self.turn_id, "turn reclaimed before completion");
        let usage = self.final_usage();
        self.settle(TurnOutcome::Cancelled { usage })
    }

    fn feed_retry_scheduled(&mut self, params: &Value) -> Vec<OutputEvent> {
        if !self.turn_matches(params) {
            return Vec::new();
        }
        // Non-terminal: NEVER settles. A visible marker beats dead air.
        let attempt = num_field(params, "attempt");
        let max = num_field(params, "maxAttempts");
        let delay = num_field(params, "retryDelayMs");
        let reason = str_field(params, "reason");
        tracing::info!(
            attempt,
            max_attempts = max,
            retry_delay_ms = delay,
            reason,
            "turn retry scheduled"
        );
        vec![OutputEvent::StatusLine(format!(
            "[retry: model attempt {attempt}/{max} failed ({reason}); next try in {delay}ms]"
        ))]
    }

    fn feed_item(&mut self, method: &str, params: &Value) -> Vec<OutputEvent> {
        let item = params.get("item").unwrap_or(&Value::Null);
        // Turn-scope: a null/absent turn id passes (the `userShell` case),
        // a present one must equal the awaited turn.
        match item.get("turnId").and_then(Value::as_str) {
            Some("") | None => {}
            Some(turn) if turn == self.turn_id => {}
            Some(_) => return Vec::new(),
        }
        let item_id = str_field(item, "itemId");
        if item_id.is_empty() {
            tracing::warn!(method, "item event without itemId");
            return Vec::new();
        }
        let kind = str_field(item, "kind");
        let rev = num_field(item, "revision").max(1);
        let state = self
            .items
            .entry(item_id.to_string())
            .or_insert_with(|| ItemState::new(kind));
        // Replace-iff-higher-revision: older or equal revisions never apply
        // backwards (redelivery and gap-fill overlap converge here).
        if rev <= state.rev {
            return Vec::new();
        }
        state.rev = rev;
        // `item/completed` is terminal by event type; `item/updated` by a
        // terminal status; `item/started` never (a missing status must not
        // read as terminal, or later deltas would be dropped).
        let terminal = method == "item/completed"
            || (method == "item/updated" && {
                let status = str_field(item, "status");
                !status.is_empty() && status != "inProgress"
            });

        let mut out = Vec::new();
        if matches!(kind, "agentMessage" | "reasoning" | "toolCall") {
            out.extend(render_mapped_item(item_id, state, item, kind));
        } else if !KNOWN_ITEM_KINDS.contains(&kind) {
            // Truly unknown future kind: generic rendering via fallbackText.
            let fallback = str_field(item, "fallbackText");
            if !fallback.is_empty() && !state.fallback_announced {
                state.fallback_announced = true;
                out.push(OutputEvent::StatusLine(format!("[{kind}] {fallback}")));
            }
        } else {
            tracing::debug!(kind, item_id, "skipping known-but-unmapped item kind");
        }
        // `truncated: true` surfaces metadata, never claims completeness.
        if item.get("truncated").and_then(Value::as_bool) == Some(true)
            && !state.truncated_announced
        {
            state.truncated_announced = true;
            out.push(OutputEvent::StatusLine(format!(
                "[truncated: {kind} output hit the host text budget; shown text is incomplete]"
            )));
        }
        if terminal {
            self.done.insert(item_id.to_string());
        }
        out
    }

    fn feed_delta(&mut self, params: &Value) -> Vec<OutputEvent> {
        if !self.turn_matches(params) {
            return Vec::new();
        }
        let item_id = str_field(params, "itemId");
        if item_id.is_empty() {
            return Vec::new();
        }
        // Late/duplicate deltas for terminally folded items are dropped.
        if self.done.contains(item_id) {
            return Vec::new();
        }
        // Deltas for never-started items are dropped: the committed object
        // always restates the whole field, so losing them loses nothing
        // durable (and without the item's kind there is no mapping).
        let Some(state) = self.items.get_mut(item_id) else {
            tracing::debug!(item_id, "dropping delta for unknown item");
            return Vec::new();
        };
        let field = str_field(params, "field");
        let field = if field.is_empty() { "text" } else { field };
        let delta = str_field(params, "delta");
        if delta.is_empty() {
            return Vec::new();
        }
        state
            .streamed
            .entry(field.to_string())
            .or_default()
            .push_str(delta);
        match (state.kind.as_str(), field) {
            ("agentMessage", "text") => vec![OutputEvent::ContentDelta(delta.to_string())],
            ("reasoning", field) => match field.strip_prefix("summary.") {
                Some(index) => match index.parse::<u32>() {
                    Ok(part) => vec![OutputEvent::ReasoningDelta {
                        part,
                        text: delta.to_string(),
                    }],
                    Err(_) => {
                        tracing::debug!(field, "ignoring malformed reasoning field");
                        Vec::new()
                    }
                },
                None => Vec::new(),
            },
            ("toolCall", "output") => vec![OutputEvent::StatusLine(delta.to_string())],
            _ => Vec::new(),
        }
    }

    fn feed_usage(&mut self, params: &Value) -> Vec<OutputEvent> {
        if !self.turn_matches(params) {
            return Vec::new();
        }
        // Counted-once per viewCursor: gap-refill replays must not double
        // count. Usage never emits live; it rides the terminal.
        if let Some(cursor) = params.get("viewCursor").and_then(Value::as_str)
            && !self.usage_seen.insert(cursor.to_string())
        {
            return Vec::new();
        }
        self.prompt_sum = self
            .prompt_sum
            .saturating_add(num_field(params, "promptTokens"));
        self.completion_sum = self.completion_sum.saturating_add(
            params
                .get("usage")
                .and_then(|u| u.get("outputTokens"))
                .and_then(Value::as_u64)
                .unwrap_or(0),
        );
        if let Some(cumulative) = params.get("cumulative") {
            self.cumulative = Some(Usage {
                prompt_tokens: num_field(cumulative, "promptTokens"),
                completion_tokens: num_field(cumulative, "outputTokens"),
                total_tokens: num_field(cumulative, "totalTokens"),
            });
        }
        Vec::new()
    }
}

/// Render a full mapped-kind item, emitting only the suffix past streamed
/// text (deltas-accumulate + committed-truth convergence).
fn render_mapped_item(
    item_id: &str,
    state: &mut ItemState,
    item: &Value,
    kind: &str,
) -> Vec<OutputEvent> {
    let mut out = Vec::new();
    match kind {
        "agentMessage" => {
            let text = str_field(item, "text");
            let known = state.streamed.get("text").map(String::as_str).unwrap_or("");
            let suffix = suffix_after(item_id, "text", known, text);
            if !suffix.is_empty() {
                state.streamed.insert("text".to_string(), text.to_string());
                out.push(OutputEvent::ContentDelta(suffix));
            }
        }
        "reasoning" => {
            // Summaries only: `reasoning.text` (raw provider reasoning, never
            // streamed) is deliberately not rendered.
            if let Some(summary) = item.get("summary").and_then(Value::as_array) {
                for (index, part) in summary.iter().enumerate() {
                    let text = part.as_str().unwrap_or("");
                    let field = format!("summary.{index}");
                    let known = state.streamed.get(&field).map(String::as_str).unwrap_or("");
                    let suffix = suffix_after(item_id, &field, known, text);
                    if !suffix.is_empty() {
                        state.streamed.insert(field, text.to_string());
                        if let Ok(part) = u32::try_from(index) {
                            out.push(OutputEvent::ReasoningDelta { part, text: suffix });
                        }
                    }
                }
            }
        }
        "toolCall" => {
            let name = tool_display_name(item);
            if !state.tool_announced {
                state.tool_announced = true;
                out.push(OutputEvent::StatusLine(format!("[tool: {name}] running")));
            }
            let output = str_field(item, "visibleOutput");
            let known = state
                .streamed
                .get("output")
                .map(String::as_str)
                .unwrap_or("");
            let suffix = suffix_after(item_id, "output", known, output);
            if !suffix.is_empty() {
                state
                    .streamed
                    .insert("output".to_string(), output.to_string());
                out.push(OutputEvent::StatusLine(suffix));
            }
            let status = str_field(item, "status");
            if status != "inProgress" && !status.is_empty() && !state.tool_terminal {
                state.tool_terminal = true;
                let reason = str_field(item, "failureReason");
                if reason.is_empty() {
                    out.push(OutputEvent::StatusLine(format!("[tool: {name}] {status}")));
                } else {
                    out.push(OutputEvent::StatusLine(format!(
                        "[tool: {name}] {status}: {reason}"
                    )));
                }
            }
        }
        _ => {}
    }
    out
}

/// Suffix of `full` past already-`known` text. The wire invariant is
/// concatenation-equals-final; on divergence the authoritative full object
/// wins (logged) rather than silently dropping truth.
fn suffix_after(item_id: &str, field: &str, known: &str, full: &str) -> String {
    match full.strip_prefix(known) {
        Some(suffix) => suffix.to_string(),
        None => {
            tracing::warn!(
                item_id,
                field,
                "full item text diverges from streamed deltas; emitting full text"
            );
            full.to_string()
        }
    }
}

/// Tool display name: `tool`, else `fallbackText`, else `"tool"`.
fn tool_display_name(item: &Value) -> String {
    let tool = str_field(item, "tool");
    if !tool.is_empty() {
        return tool.to_string();
    }
    let fallback = str_field(item, "fallbackText");
    if !fallback.is_empty() {
        return fallback.to_string();
    }
    "tool".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const SESSION: &str = "sess-1";
    const TURN: &str = "turn-1";

    fn fold() -> TurnFold {
        TurnFold::new(SESSION, TURN)
    }

    fn item_params(item: Value, cursor: &str) -> Value {
        json!({"sessionId": SESSION, "viewCursor": cursor, "item": item})
    }

    fn delta_params(item_id: &str, field: &str, delta: &str, cursor: &str) -> Value {
        json!({
            "sessionId": SESSION, "viewCursor": cursor,
            "itemId": item_id, "field": field, "delta": delta,
        })
    }

    fn agent_item(item_id: &str, rev: u64, status: &str, text: &str) -> Value {
        json!({
            "itemId": item_id, "kind": "agentMessage", "turnId": TURN,
            "revision": rev, "status": status, "text": text,
        })
    }

    fn completed_params(terminal: &str, cursor: &str) -> Value {
        json!({
            "sessionId": SESSION, "viewCursor": cursor,
            "turnId": TURN, "terminal": terminal,
        })
    }

    fn content_text(events: &[OutputEvent]) -> String {
        events
            .iter()
            .filter_map(|e| match e {
                OutputEvent::ContentDelta(t) => Some(t.as_str()),
                _ => None,
            })
            .collect()
    }

    fn status_lines(events: &[OutputEvent]) -> Vec<String> {
        events
            .iter()
            .filter_map(|e| match e {
                OutputEvent::StatusLine(t) => Some(t.clone()),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn delta_concat_equals_final_with_no_double_emit() {
        let mut fold = fold();
        let started = fold.feed(
            "item/started",
            &item_params(agent_item("m1", 1, "inProgress", ""), "v:1"),
        );
        assert!(started.is_empty());
        let d1 = fold.feed("item/delta", &delta_params("m1", "text", "Hello, ", "v:2"));
        let d2 = fold.feed("item/delta", &delta_params("m1", "text", "world", "v:3"));
        assert_eq!(content_text(&d1), "Hello, ");
        assert_eq!(content_text(&d2), "world");
        // Committed truth restates the concatenation: nothing new to emit.
        let done = fold.feed(
            "item/completed",
            &item_params(agent_item("m1", 2, "completed", "Hello, world"), "v:4"),
        );
        assert!(done.is_empty(), "{done:?}");
        assert_eq!(fold.cursor(), Some("v:4"));
    }

    #[test]
    fn completed_for_never_started_item_emits_full_text() {
        let mut fold = fold();
        // Single-shot items emit only `item/completed` (also the gap-fill
        // shape): the full object is the whole truth.
        let done = fold.feed(
            "item/completed",
            &item_params(agent_item("m1", 1, "completed", "whole truth"), "v:1"),
        );
        assert_eq!(content_text(&done), "whole truth");
    }

    #[test]
    fn updated_before_started_is_accepted() {
        let mut fold = fold();
        let updated = fold.feed(
            "item/updated",
            &item_params(agent_item("m1", 2, "inProgress", "early"), "v:1"),
        );
        assert_eq!(content_text(&updated), "early");
    }

    #[test]
    fn duplicate_completed_and_late_deltas_are_dropped() {
        let mut fold = fold();
        fold.feed(
            "item/completed",
            &item_params(agent_item("m1", 2, "completed", "once"), "v:1"),
        );
        // Redelivery at the same revision: silent.
        let dup = fold.feed(
            "item/completed",
            &item_params(agent_item("m1", 2, "completed", "once"), "v:2"),
        );
        assert!(dup.is_empty(), "{dup:?}");
        // A stale lower revision never applies backwards.
        let stale = fold.feed(
            "item/updated",
            &item_params(agent_item("m1", 1, "inProgress", "stale"), "v:3"),
        );
        assert!(stale.is_empty(), "{stale:?}");
        // A delta arriving after the terminal is dropped.
        let late = fold.feed("item/delta", &delta_params("m1", "text", "late", "v:4"));
        assert!(late.is_empty(), "{late:?}");
    }

    #[test]
    fn gap_page_suffix_converges_without_resend() {
        let mut fold = fold();
        fold.feed(
            "item/started",
            &item_params(agent_item("m1", 1, "inProgress", ""), "v:1"),
        );
        fold.feed("item/delta", &delta_params("m1", "text", "a", "v:2"));
        // Gap page replays the full truth past what streamed: only "bc".
        let page = fold.feed(
            "item/completed",
            &item_params(agent_item("m1", 2, "completed", "abc"), "v:9"),
        );
        assert_eq!(content_text(&page), "bc");
        // Re-feeding the same page converges to silence.
        let again = fold.feed(
            "item/completed",
            &item_params(agent_item("m1", 2, "completed", "abc"), "v:9"),
        );
        assert!(again.is_empty(), "{again:?}");
    }

    #[test]
    fn delta_for_unknown_item_is_dropped_without_losing_truth() {
        let mut fold = fold();
        let dropped = fold.feed("item/delta", &delta_params("ghost", "text", "x", "v:1"));
        assert!(dropped.is_empty());
        // The committed object still delivers the whole field.
        let done = fold.feed(
            "item/completed",
            &item_params(agent_item("ghost", 1, "completed", "x"), "v:2"),
        );
        assert_eq!(content_text(&done), "x");
    }

    #[test]
    fn completed_terminal_settles_success() {
        let mut fold = fold();
        let out = fold.feed("turn/completed", &completed_params("completed", "v:1"));
        assert!(fold.is_settled());
        assert_eq!(
            out,
            vec![OutputEvent::Terminal(TurnOutcome::Completed {
                usage: Usage::default()
            })]
        );
    }

    #[test]
    fn cancelled_terminal_settles_cancelled() {
        let mut fold = fold();
        let out = fold.feed("turn/completed", &completed_params("cancelled", "v:1"));
        assert!(matches!(
            out.as_slice(),
            [OutputEvent::Terminal(TurnOutcome::Cancelled { .. })]
        ));
    }

    #[test]
    fn failed_terminal_carries_kind_message_retryable() {
        let mut fold = fold();
        let params = json!({
            "sessionId": SESSION, "viewCursor": "v:1", "turnId": TURN,
            "terminal": "failed",
            "error": {"kind": "authRequired", "message": "login first", "retryable": false},
        });
        let out = fold.feed("turn/completed", &params);
        assert_eq!(
            out,
            vec![OutputEvent::Terminal(TurnOutcome::Failed {
                kind: "authRequired".to_string(),
                message: "login first".to_string(),
                retryable: false,
                usage: Usage::default(),
            })]
        );
    }

    #[test]
    fn unknown_terminal_string_is_a_failure_not_a_hang() {
        let mut fold = fold();
        // No error object at all: kind falls back to the terminal string.
        let out = fold.feed("turn/completed", &completed_params("exploded", "v:1"));
        match out.as_slice() {
            [
                OutputEvent::Terminal(TurnOutcome::Failed {
                    kind,
                    message,
                    retryable,
                    ..
                }),
            ] => {
                assert_eq!(kind, "exploded");
                assert!(message.contains("exploded"), "{message}");
                assert!(!retryable);
            }
            other => panic!("expected Failed terminal, got {other:?}"),
        }
        assert!(fold.is_settled());
    }

    #[test]
    fn unqueued_settles_cancelled_with_no_completion_ever() {
        let mut fold = fold();
        let params = json!({
            "sessionId": SESSION, "viewCursor": "v:1",
            "turnId": TURN, "commandId": TURN,
        });
        let out = fold.feed("turn/unqueued", &params);
        assert!(matches!(
            out.as_slice(),
            [OutputEvent::Terminal(TurnOutcome::Cancelled { .. })]
        ));
        // A late completion after reclaim is impossible; if one arrives the
        // settled fold ignores it (settle-once).
        let late = fold.feed("turn/completed", &completed_params("completed", "v:2"));
        assert!(late.is_empty());
    }

    #[test]
    fn retracted_settles_cancelled() {
        let mut fold = fold();
        let params = json!({
            "sessionId": SESSION, "viewCursor": "v:1",
            "turnId": TURN, "commandId": TURN,
        });
        let out = fold.feed("turn/retracted", &params);
        assert!(matches!(
            out.as_slice(),
            [OutputEvent::Terminal(TurnOutcome::Cancelled { .. })]
        ));
    }

    #[test]
    fn retry_scheduled_never_settles() {
        let mut fold = fold();
        for cursor in ["v:1", "v:2"] {
            let params = json!({
                "sessionId": SESSION, "viewCursor": cursor, "turnId": TURN,
                "attempt": 1, "nextAttempt": 2, "maxAttempts": 5,
                "retryDelayMs": 800, "reason": "model busy",
            });
            let out = fold.feed("turn/retryScheduled", &params);
            assert_eq!(out.len(), 1);
            assert!(matches!(out[0], OutputEvent::StatusLine(_)));
            assert!(!fold.is_settled());
        }
        let lines = status_lines(&fold.feed(
            "turn/retryScheduled",
            &json!({
                "sessionId": SESSION, "viewCursor": "v:3", "turnId": TURN,
                "attempt": 2, "nextAttempt": 3, "maxAttempts": 5,
                "retryDelayMs": 1600, "reason": "model busy",
            }),
        ));
        assert_eq!(lines.len(), 1);
        assert!(lines[0].contains("2/5"), "{}", lines[0]);
        assert!(lines[0].contains("1600ms"), "{}", lines[0]);
        // The turn still completes normally afterwards.
        let out = fold.feed("turn/completed", &completed_params("completed", "v:4"));
        assert!(matches!(
            out.as_slice(),
            [OutputEvent::Terminal(TurnOutcome::Completed { .. })]
        ));
    }

    #[test]
    fn other_sessions_and_turns_are_ignored() {
        let mut fold = fold();
        // Another session's terminal.
        let mut params = completed_params("completed", "v:1");
        params["sessionId"] = "sess-2".into();
        assert!(fold.feed("turn/completed", &params).is_empty());
        // Another turn's terminal on our session.
        let mut params = completed_params("completed", "v:2");
        params["turnId"] = "turn-9".into();
        assert!(fold.feed("turn/completed", &params).is_empty());
        // Another turn's item.
        let mut item = agent_item("m1", 1, "completed", "nope");
        item["turnId"] = "turn-9".into();
        assert!(
            fold.feed("item/completed", &item_params(item, "v:3"))
                .is_empty()
        );
        assert!(!fold.is_settled());
        // Other-session events never touch the cursor; same-session events
        // (even for other turns) advance it — cursors are per-session.
        assert_eq!(fold.cursor(), Some("v:3"));
    }

    #[test]
    fn usage_is_counted_once_per_cursor_and_reconciled() {
        let mut fold = fold();
        let usage = |cursor: &str| {
            json!({
                "sessionId": SESSION, "viewCursor": cursor, "turnId": TURN,
                "promptTokens": 100,
                "usage": {"outputTokens": 10},
                "cumulative": {"promptTokens": 100, "outputTokens": 10, "totalTokens": 110},
            })
        };
        assert!(fold.feed("session/tokenUsage", &usage("v:1")).is_empty());
        // Same cursor redelivered (gap overlap): not double counted.
        assert!(fold.feed("session/tokenUsage", &usage("v:1")).is_empty());
        let out = fold.feed("turn/completed", &completed_params("completed", "v:2"));
        match out.as_slice() {
            [OutputEvent::Terminal(outcome)] => assert_eq!(
                outcome.usage(),
                &Usage {
                    prompt_tokens: 100,
                    completion_tokens: 10,
                    total_tokens: 110
                }
            ),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn usage_sums_accumulate_without_cumulative() {
        let mut fold = fold();
        for (cursor, prompt, output) in [("v:1", 100, 10), ("v:2", 50, 5)] {
            fold.feed(
                "session/tokenUsage",
                &json!({
                    "sessionId": SESSION, "viewCursor": cursor, "turnId": TURN,
                    "promptTokens": prompt,
                    "usage": {"outputTokens": output},
                }),
            );
        }
        let out = fold.feed("turn/completed", &completed_params("completed", "v:3"));
        match out.as_slice() {
            [OutputEvent::Terminal(outcome)] => assert_eq!(
                outcome.usage(),
                &Usage {
                    prompt_tokens: 150,
                    completion_tokens: 15,
                    total_tokens: 165
                }
            ),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn context_usage_never_blanks_known_state() {
        let mut fold = fold();
        fold.feed(
            "session/tokenUsage",
            &json!({
                "sessionId": SESSION, "viewCursor": "v:1", "turnId": TURN,
                "promptTokens": 7, "usage": {"outputTokens": 1},
                "cumulative": {"promptTokens": 7, "outputTokens": 1, "totalTokens": 8},
            }),
        );
        // Context pressure (and null snapshot arms) are not tracked at all.
        let out = fold.feed(
            "session/contextUsage",
            &json!({
                "sessionId": SESSION, "viewCursor": "v:2",
                "pressure": "normal", "usedTokens": 8, "windowTokens": 1000,
            }),
        );
        assert!(out.is_empty());
        let out = fold.feed("turn/completed", &completed_params("completed", "v:3"));
        match out.as_slice() {
            [OutputEvent::Terminal(outcome)] => {
                assert_eq!(outcome.usage().prompt_tokens, 7);
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn tool_call_is_announced_never_silent_with_terminal() {
        let mut fold = fold();
        let started = fold.feed(
            "item/started",
            &item_params(
                json!({
                    "itemId": "t1", "kind": "toolCall", "turnId": TURN,
                    "revision": 1, "status": "inProgress", "tool": "shell",
                }),
                "v:1",
            ),
        );
        assert_eq!(status_lines(&started), vec!["[tool: shell] running"]);
        let delta = fold.feed("item/delta", &delta_params("t1", "output", "hi\n", "v:2"));
        assert_eq!(status_lines(&delta), vec!["hi\n"]);
        let done = fold.feed(
            "item/completed",
            &item_params(
                json!({
                    "itemId": "t1", "kind": "toolCall", "turnId": TURN,
                    "revision": 2, "status": "completed", "tool": "shell",
                    "visibleOutput": "hi\nbye\n",
                }),
                "v:3",
            ),
        );
        // Only the suffix past streamed output, then the terminal marker.
        assert_eq!(
            status_lines(&done),
            vec!["bye\n", "[tool: shell] completed"]
        );
    }

    #[test]
    fn failed_tool_call_terminal_carries_the_reason() {
        let mut fold = fold();
        let done = fold.feed(
            "item/completed",
            &item_params(
                json!({
                    "itemId": "t1", "kind": "toolCall", "turnId": TURN,
                    "revision": 1, "status": "failed",
                    "tool": "files", "failureReason": "denied by policy",
                }),
                "v:1",
            ),
        );
        assert_eq!(
            status_lines(&done),
            vec![
                "[tool: files] running",
                "[tool: files] failed: denied by policy"
            ]
        );
    }

    #[test]
    fn reasoning_summaries_stream_per_part() {
        let mut fold = fold();
        fold.feed(
            "item/started",
            &item_params(
                json!({
                    "itemId": "r1", "kind": "reasoning", "turnId": TURN,
                    "revision": 1, "status": "inProgress",
                }),
                "v:1",
            ),
        );
        let d0 = fold.feed(
            "item/delta",
            &delta_params("r1", "summary.0", "think", "v:2"),
        );
        assert_eq!(
            d0,
            vec![OutputEvent::ReasoningDelta {
                part: 0,
                text: "think".to_string()
            }]
        );
        let d1 = fold.feed(
            "item/delta",
            &delta_params("r1", "summary.1", "more", "v:3"),
        );
        assert_eq!(
            d1,
            vec![OutputEvent::ReasoningDelta {
                part: 1,
                text: "more".to_string()
            }]
        );
        // Committed summaries restate; only new suffixes emit. Raw
        // `reasoning.text` is never rendered.
        let done = fold.feed(
            "item/completed",
            &item_params(
                json!({
                    "itemId": "r1", "kind": "reasoning", "turnId": TURN,
                    "revision": 2, "status": "completed",
                    "summary": ["thinking", "more!"],
                    "text": "RAW PROVIDER REASONING",
                }),
                "v:4",
            ),
        );
        assert_eq!(
            done,
            vec![
                OutputEvent::ReasoningDelta {
                    part: 0,
                    text: "ing".to_string()
                },
                OutputEvent::ReasoningDelta {
                    part: 1,
                    text: "!".to_string()
                },
            ]
        );
    }

    #[test]
    fn truncated_surfaces_metadata_without_claiming_completeness() {
        let mut fold = fold();
        let done = fold.feed(
            "item/completed",
            &item_params(
                json!({
                    "itemId": "m1", "kind": "agentMessage", "turnId": TURN,
                    "revision": 2, "status": "completed",
                    "text": "partial…", "truncated": true,
                }),
                "v:1",
            ),
        );
        assert_eq!(content_text(&done), "partial…");
        let lines = status_lines(&done);
        assert_eq!(lines.len(), 1);
        assert!(lines[0].contains("truncated"), "{}", lines[0]);
        assert!(lines[0].contains("incomplete"), "{}", lines[0]);
    }

    #[test]
    fn user_message_echo_is_never_rendered() {
        let mut fold = fold();
        let out = fold.feed(
            "item/completed",
            &item_params(
                json!({
                    "itemId": "u1", "kind": "userMessage", "turnId": TURN,
                    "revision": 1, "status": "completed", "text": "our own prompt",
                }),
                "v:1",
            ),
        );
        assert!(out.is_empty(), "{out:?}");
    }

    #[test]
    fn unknown_future_kind_renders_fallback_once() {
        let mut fold = fold();
        let item = json!({
            "itemId": "f1", "kind": "futureKind", "turnId": TURN,
            "revision": 1, "status": "inProgress", "fallbackText": "a future thing",
        });
        let out = fold.feed("item/started", &item_params(item.clone(), "v:1"));
        assert_eq!(status_lines(&out), vec!["[futureKind] a future thing"]);
        // Higher revision re-announces nothing (already rendered).
        let mut rev2 = item.clone();
        rev2["revision"] = 2.into();
        rev2["status"] = "completed".into();
        let out = fold.feed("item/completed", &item_params(rev2, "v:2"));
        assert!(out.is_empty(), "{out:?}");
    }

    #[test]
    fn unknown_kind_without_fallback_and_unknown_methods_are_silent() {
        let mut fold = fold();
        let out = fold.feed(
            "item/completed",
            &item_params(
                json!({
                    "itemId": "f1", "kind": "futureKind", "turnId": TURN,
                    "revision": 1, "status": "completed",
                }),
                "v:1",
            ),
        );
        assert!(out.is_empty());
        // Live-but-undeclared methods (e.g. a future `session/frobnicate`)
        // are ignored, never fatal — cursor still tracks.
        let out = fold.feed(
            "session/frobnicate",
            &json!({"sessionId": SESSION, "viewCursor": "v:2"}),
        );
        assert!(out.is_empty());
        assert_eq!(fold.cursor(), Some("v:2"));
    }

    #[test]
    fn known_but_unmapped_kinds_stay_silent() {
        let mut fold = fold();
        // Reminder children arrive on nearly every live turn; they must not
        // spam the transcript even though they carry fallbackText.
        let out = fold.feed(
            "item/completed",
            &item_params(
                json!({
                    "itemId": "r1", "kind": "reminderChild", "turnId": TURN,
                    "revision": 2, "status": "completed",
                    "fallbackText": "Reminder child session",
                }),
                "v:1",
            ),
        );
        assert!(out.is_empty(), "{out:?}");
    }

    #[test]
    fn approval_mode_and_model_fold_from_view_events() {
        let mut fold = fold();
        assert_eq!(fold.approval_mode(), None);
        assert_eq!(fold.model(), None);
        fold.feed(
            "session/started",
            &json!({
                "session": {
                    "sessionId": SESSION,
                    "approvalMode": {"mode": "denyUnmatched"},
                },
            }),
        );
        assert_eq!(fold.approval_mode(), Some("denyUnmatched"));
        fold.feed(
            "session/approvalModeChanged",
            &json!({"sessionId": SESSION, "viewCursor": "v:1", "mode": "allowAll"}),
        );
        assert_eq!(fold.approval_mode(), Some("allowAll"));
        fold.feed(
            "session/modelChanged",
            &json!({"sessionId": SESSION, "viewCursor": "v:2", "modelId": "m-x"}),
        );
        assert_eq!(fold.model(), Some("m-x"));
    }

    #[test]
    fn gap_emits_a_page_action_and_malformed_gap_warns() {
        let mut fold = fold();
        let out = fold.feed(
            "view/gap",
            &json!({"sessionId": SESSION, "after": "v:3", "next": "v:9"}),
        );
        assert_eq!(
            out,
            vec![OutputEvent::Gap {
                after: "v:3".to_string(),
                next: "v:9".to_string(),
            }]
        );
        let malformed = fold.feed("view/gap", &json!({"sessionId": SESSION}));
        assert!(malformed.is_empty());
    }

    #[test]
    fn settle_is_exactly_once() {
        let mut fold = fold();
        fold.feed("turn/completed", &completed_params("completed", "v:1"));
        assert!(fold.is_settled());
        // Everything after the terminal is ignored, including cursors.
        assert!(
            fold.feed("item/delta", &delta_params("m1", "text", "late", "v:99"))
                .is_empty()
        );
        assert_eq!(fold.cursor(), Some("v:1"));
    }
}
