//! NDJSON framing, JSON-RPC envelope, and the two id families (SPEC §4).
//!
//! Wire rules implemented here:
//!
//! - One JSON object + `\n` per frame, flushed per frame, never pretty.
//! - Inbound: tolerate `\r\n`, skip blank lines, skip unparsable lines
//!   WITHOUT dropping the connection, enforce the 10 MiB cap both directions.
//! - Unknown top-level members are ignored on every frame.
//! - Responses carry exactly one of `result` (object) / `error` (typed).
//! - Request ids correlate by exact JSON rendering (`1` ≠ `"1"`).
//! - `commandId`s are monotonic UUIDv7 from one minter per connection.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::Value;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Maximum NDJSON frame size in either direction (SPEC §4).
pub const FRAME_LIMIT_BYTES: usize = 10 * 1024 * 1024;

/// JSON-RPC request id: a JSON string or integer (SPEC §4.2, envelope guide).
///
/// A string and an integer never compare equal: [`RpcId::Int(1)`] and
/// [`RpcId::Str`] `"1"` are different ids and key different pending entries.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RpcId {
    /// JSON integer id.
    Int(i64),
    /// JSON string id.
    Str(String),
}

impl RpcId {
    /// Render the id back to its exact JSON value (for echoing server ids).
    pub fn to_value(&self) -> Value {
        match self {
            RpcId::Int(n) => Value::Number((*n).into()),
            RpcId::Str(s) => Value::String(s.clone()),
        }
    }

    /// Parse a raw `id` member. Returns `None` for ids that can never
    /// correlate (bool, null, float, out-of-range): the frame is logged and
    /// skipped, never fatal.
    pub fn from_raw(raw: &Value) -> Option<RpcId> {
        match raw {
            Value::Number(n) => n.as_i64().map(RpcId::Int),
            Value::String(s) => Some(RpcId::Str(s.clone())),
            _ => None,
        }
    }

    /// Pending-map key. The `i:`/`s:` tag makes the type part of the key so
    /// `1` and `"1"` never collide (exact-rendering equality).
    pub fn pending_key(&self) -> String {
        match self {
            RpcId::Int(n) => format!("i:{n}"),
            RpcId::Str(s) => format!("s:{s}"),
        }
    }

    /// Pending-map key straight from a raw `id` member.
    pub fn pending_key_for_raw(raw: &Value) -> Option<String> {
        Self::from_raw(raw).map(|id| id.pending_key())
    }
}

/// In-flight request correlation: response id → waiter.
///
/// Keyed by exact JSON rendering via [`RpcId::pending_key`]; see [`RpcId`].
#[derive(Debug, Default)]
pub struct Pending<T> {
    map: HashMap<String, T>,
}

impl<T> Pending<T> {
    /// Empty correlation map.
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    /// Number of in-flight requests.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Whether no request is in flight.
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Register a waiter under a client-minted id.
    pub fn insert(&mut self, id: &RpcId, waiter: T) {
        self.map.insert(id.pending_key(), waiter);
    }

    /// Remove the waiter for a client-minted id (timeout/cancel path).
    pub fn remove(&mut self, id: &RpcId) -> Option<T> {
        self.map.remove(&id.pending_key())
    }

    /// Remove the waiter matching a raw response `id` member. `None` means
    /// the id is unparseable or unknown: the caller logs and continues.
    pub fn remove_raw(&mut self, raw: &Value) -> Option<T> {
        let key = RpcId::pending_key_for_raw(raw)?;
        self.map.remove(&key)
    }

    /// Drop every waiter (connection death: their `oneshot`s fail, and each
    /// waiter maps the disconnect to a host-dead error).
    pub fn clear(&mut self) {
        self.map.clear();
    }
}

/// Typed MSP error object (SPEC §4.2, errors guide).
///
/// Clients branch on `data.kind`, never on `message`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErrorObject {
    /// Coarse numeric code (JSON-RPC range plus `-32000..-32099` MSP block).
    pub code: i64,
    /// Human-readable, unstable. Never a branch point.
    pub message: String,
    /// Structured detail; carries `kind` when present.
    pub data: Option<Value>,
}

impl ErrorObject {
    /// The stable camelCase category clients branch on (`data.kind`).
    pub fn kind(&self) -> Option<&str> {
        self.data
            .as_ref()
            .and_then(|d| d.get("kind"))
            .and_then(Value::as_str)
    }

    /// Per-error retry override (`data.retryable`), when the host states one.
    pub fn retryable(&self) -> Option<bool> {
        self.data
            .as_ref()
            .and_then(|d| d.get("retryable"))
            .and_then(Value::as_bool)
    }

    /// Locally synthesized error (timeout, protocol violation, dead host).
    /// Always carries `data.kind` so downstream branching keeps working.
    pub fn local(code: i64, message: impl Into<String>, kind: &str) -> Self {
        Self {
            code,
            message: message.into(),
            data: Some(serde_json::json!({"kind": kind})),
        }
    }
}

/// One classified inbound frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IncomingFrame {
    /// Response to a client request: id + exactly one of result/error.
    Response {
        /// Correlating id.
        id: RpcId,
        /// Validated payload: object result or typed error.
        body: Result<Value, ErrorObject>,
    },
    /// Either-direction notification: method, no id, never answered.
    Notification {
        /// Slash-namespaced method name.
        method: String,
        /// Raw params (`Null` when omitted).
        params: Value,
    },
    /// Server-initiated request: id + method. Exactly one response is owed.
    ServerRequest {
        /// Connection-scoped id to echo in the response.
        id: RpcId,
        /// Method name.
        method: String,
        /// Raw params (`Null` when omitted).
        params: Value,
    },
    /// Malformed frame. The connection survives; the `id`, when recoverable,
    /// lets the caller fail that waiter's command locally instead of hanging
    /// it until timeout.
    Invalid {
        /// Stable machine-readable reason (never host text).
        reason: &'static str,
        /// Recovered id, if the frame carried a correlatable one.
        id: Option<RpcId>,
    },
}

/// Classify one parsed frame into the four envelope shapes (SPEC §4.2).
///
/// Unknown top-level members are ignored. Anything that is not exactly one
/// shape becomes [`IncomingFrame::Invalid`]: log, skip, survive.
pub fn classify_frame(frame: &Value) -> IncomingFrame {
    let method = frame.get("method").and_then(Value::as_str);
    let raw_id = frame.get("id");
    // `id: null` appears only on unrecoverable parse-error frames; it
    // correlates with nothing.
    let id = raw_id.and_then(|raw| {
        if raw.is_null() {
            None
        } else {
            RpcId::from_raw(raw)
        }
    });
    let params = frame.get("params").cloned().unwrap_or(Value::Null);

    match (raw_id, method) {
        (Some(_), Some(method)) => match id {
            Some(id) => IncomingFrame::ServerRequest {
                id,
                method: method.to_string(),
                params,
            },
            None => IncomingFrame::Invalid {
                reason: "server request has non-string/non-integer id",
                id: None,
            },
        },
        (Some(_), None) => {
            let Some(id) = id else {
                return IncomingFrame::Invalid {
                    reason: "response has non-string/non-integer id",
                    id: None,
                };
            };
            validate_response(id, frame)
        }
        (None, Some(method)) => IncomingFrame::Notification {
            method: method.to_string(),
            params,
        },
        (None, None) => IncomingFrame::Invalid {
            reason: "frame has neither id nor method",
            id: None,
        },
    }
}

/// Validate the `result`/`error` half of a response frame.
fn validate_response(id: RpcId, frame: &Value) -> IncomingFrame {
    let has_result = frame.get("result").is_some();
    let has_error = frame.get("error").is_some();
    if has_result == has_error {
        return IncomingFrame::Invalid {
            reason: "response must carry exactly one of result/error",
            id: Some(id),
        };
    }
    if let Some(result) = frame.get("result") {
        if !result.is_object() {
            return IncomingFrame::Invalid {
                reason: "response result is not an object",
                id: Some(id),
            };
        }
        return IncomingFrame::Response {
            id,
            body: Ok(result.clone()),
        };
    }
    let Some(error) = frame.get("error") else {
        return IncomingFrame::Invalid {
            reason: "response must carry exactly one of result/error",
            id: Some(id),
        };
    };
    match parse_error_object(error) {
        Some(err) => IncomingFrame::Response { id, body: Err(err) },
        None => IncomingFrame::Invalid {
            reason: "error object lacks integer code/string message/data.kind",
            id: Some(id),
        },
    }
}

/// Parse a typed error object: object with integer `code`, string `message`,
/// and `data.kind` (HANDOFF P1 contract).
fn parse_error_object(error: &Value) -> Option<ErrorObject> {
    let obj = error.as_object()?;
    let code = obj.get("code")?.as_i64()?;
    let message = obj.get("message")?.as_str()?.to_string();
    let data = obj.get("data").cloned();
    let kind_present = data
        .as_ref()
        .and_then(|d| d.as_object())
        .and_then(|d| d.get("kind"))
        .is_some_and(Value::is_string);
    if !kind_present {
        return None;
    }
    Some(ErrorObject {
        code,
        message,
        data,
    })
}

/// Build a client→server request frame. `params` is always an object here;
/// methods that take none omit it via [`notification_frame].
pub fn request_frame(id: &RpcId, method: &str, params: &Value) -> Value {
    serde_json::json!({
        "jsonrpc": "2.0",
        "id": id.to_value(),
        "method": method,
        "params": params,
    })
}

/// Build a client→server notification frame. `params` is omitted entirely
/// when `None`, never sent as null.
pub fn notification_frame(method: &str, params: Option<&Value>) -> Value {
    let mut frame = serde_json::json!({
        "jsonrpc": "2.0",
        "method": method,
    });
    if let Some(params) = params {
        frame["params"] = params.clone();
    }
    frame
}

/// Build the `{}` presentation receipt for a server-initiated request
/// (`approval/request`, `userInput/request`). Receipt only: it changes no
/// state, and the real effect travels via a separate command.
pub fn ok_response_frame(id: &RpcId) -> Value {
    serde_json::json!({
        "jsonrpc": "2.0",
        "id": id.to_value(),
        "result": {},
    })
}

/// Build a typed `methodNotFound` reply for a server-initiated request this
/// client does not handle. Unknown server methods must get this, never a
/// synthetic `{}` (which could corrupt host state).
pub fn method_not_found_frame(id: &RpcId, method: &str) -> Value {
    serde_json::json!({
        "jsonrpc": "2.0",
        "id": id.to_value(),
        "error": {
            "code": -32601,
            "message": format!("method not found: {method}"),
            "data": {"kind": "methodNotFound", "retryable": false},
        },
    })
}

/// Framing failure. Inbound garbage is warn-and-skip inside [`next_frame`],
/// so this surfaces only I/O errors and oversize OUTBOUND frames (an
/// oversize send is refused, never truncated or retried as-is).
#[derive(Debug)]
pub enum FrameError {
    /// Underlying stdio I/O failure.
    Io(std::io::Error),
    /// Outbound frame exceeds [`FRAME_LIMIT_BYTES`]; nothing was written.
    Oversize {
        /// Refused frame size in bytes (excluding the terminator).
        len: usize,
    },
}

impl std::fmt::Display for FrameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FrameError::Io(e) => write!(f, "stdio I/O error: {e}"),
            FrameError::Oversize { len } => write!(
                f,
                "outbound frame is {len} bytes, over the {FRAME_LIMIT_BYTES}-byte cap"
            ),
        }
    }
}

impl std::error::Error for FrameError {}

impl From<std::io::Error> for FrameError {
    fn from(e: std::io::Error) -> Self {
        FrameError::Io(e)
    }
}

/// Write one frame: a single JSON object + `\n`, flushed per frame, never
/// pretty-printed. Refuses oversize frames without writing anything.
pub async fn write_frame<W: AsyncWrite + Unpin>(
    writer: &mut W,
    frame: &Value,
) -> Result<(), FrameError> {
    let mut bytes = serde_json::to_vec(frame).expect("serde_json::Value serializes");
    if bytes.len() > FRAME_LIMIT_BYTES {
        return Err(FrameError::Oversize { len: bytes.len() });
    }
    bytes.push(b'\n');
    writer.write_all(&bytes).await?;
    writer.flush().await?;
    Ok(())
}

/// Read the next valid frame, skipping everything invalid.
///
/// Tolerates `\r\n`, skips blank lines, and skips unparsable / non-object /
/// invalid-UTF-8 / oversize lines with a `warn!` (length + reason only) plus
/// a `debug!` preview — the connection outlives any one frame. Returns
/// `Ok(None)` on clean EOF.
pub async fn next_frame<R: AsyncBufRead + Unpin>(
    reader: &mut R,
) -> Result<Option<Value>, FrameError> {
    let mut buf = Vec::new();
    loop {
        buf.clear();
        let n = AsyncReadExt::take(&mut *reader, (FRAME_LIMIT_BYTES + 1) as u64)
            .read_until(b'\n', &mut buf)
            .await?;
        if n == 0 {
            return Ok(None);
        }
        // A full take without a newline proves the line exceeds the cap;
        // anything shorter either terminated or hit EOF (a short final line
        // without `\n` still parses below).
        let complete = buf.ends_with(b"\n");
        if complete {
            buf.pop();
        }
        if buf.ends_with(b"\r") {
            buf.pop();
        }
        if n == FRAME_LIMIT_BYTES + 1 && !complete {
            // Drain to the next newline so the stream resyncs, then keep
            // reading: one oversize line never drops the connection.
            drain_line(reader).await?;
            tracing::warn!(frame_len = buf.len(), "skipping oversized inbound frame");
            continue;
        }
        let line = match String::from_utf8(std::mem::take(&mut buf)) {
            Ok(line) => line,
            Err(e) => {
                tracing::warn!(
                    frame_len = e.as_bytes().len(),
                    "skipping non-UTF-8 inbound line"
                );
                continue;
            }
        };
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<Value>(&line) {
            Ok(frame @ Value::Object(_)) => return Ok(Some(frame)),
            Ok(_) => {
                tracing::warn!(frame_len = line.len(), "skipping non-object inbound frame");
                tracing::debug!(preview = %preview(&line), "non-object frame");
            }
            Err(e) => {
                tracing::warn!(
                    frame_len = line.len(),
                    error = %e,
                    "skipping unparsable inbound line"
                );
                tracing::debug!(preview = %preview(&line), "unparsable line");
            }
        }
    }
}

/// Discard bytes through the next `\n` (or EOF) to resync after an oversize
/// line. Bounded per read so a hostile peer cannot grow memory.
async fn drain_line<R: AsyncBufRead + Unpin>(reader: &mut R) -> Result<(), FrameError> {
    let mut chunk = Vec::new();
    loop {
        chunk.clear();
        let n = AsyncReadExt::take(&mut *reader, 65536)
            .read_until(b'\n', &mut chunk)
            .await?;
        if n == 0 || chunk.ends_with(b"\n") {
            return Ok(());
        }
    }
}

/// Truncated single-line preview for `debug!` logs (never at `warn`+: a
/// garbage line can still carry message text).
fn preview(line: &str) -> String {
    const MAX: usize = 200;
    let mut out: String = line.chars().take(MAX).collect();
    if line.chars().count() > MAX {
        out.push('…');
    }
    out
}

/// Monotonic UUIDv7 minter: one per connection (SPEC §4.2, AGENTS rule 3).
///
/// Layout is timestamp-ms (48 bits) + randomness (74 bits) with the version
/// and variant bits fixed. The minted sequence is non-decreasing even when
/// the clock stands still or steps backward: a candidate that does not exceed
/// the previous mint is bumped to previous-plus-one. (In the 2⁻⁷⁴ event that
/// the bump carries into the version nibble, shape wins over monotonicity:
/// the version/variant bits are re-forced, since the host rejects non-v7
/// ids.)
///
/// Clock and randomness are injectable so tests mint deterministically.
pub struct IdMinter {
    last: Mutex<u128>,
    now_ms: Box<dyn Fn() -> u64 + Send + Sync>,
    rand10: Box<dyn Fn() -> [u8; 10] + Send + Sync>,
}

impl IdMinter {
    /// Production minter: wall clock plus `Uuid::new_v4` randomness.
    pub fn new() -> Self {
        Self::with_sources(system_now_ms, system_rand10)
    }

    /// Minter with injected clock/randomness (tests, deterministic replay).
    pub fn with_sources(
        now_ms: impl Fn() -> u64 + Send + Sync + 'static,
        rand10: impl Fn() -> [u8; 10] + Send + Sync + 'static,
    ) -> Self {
        Self {
            last: Mutex::new(0),
            now_ms: Box::new(now_ms),
            rand10: Box::new(rand10),
        }
    }

    /// Mint one hyphenated lowercase UUIDv7, monotonic per minter.
    pub fn mint(&self) -> String {
        let ms = (self.now_ms)() & 0xffff_ffff_ffff;
        let r = (self.rand10)();
        let mut bytes = [0u8; 16];
        bytes[0..6].copy_from_slice(&ms.to_be_bytes()[2..8]);
        bytes[6] = 0x70 | (r[0] & 0x0f);
        bytes[7] = r[1];
        bytes[8] = 0x80 | (r[2] & 0x3f);
        bytes[9..16].copy_from_slice(&r[3..10]);

        let mut candidate = u128::from_be_bytes(bytes);
        let mut last = self.last.lock().unwrap_or_else(|p| p.into_inner());
        if candidate <= *last {
            candidate = last.wrapping_add(1);
            // Re-force shape after the bump (see struct docs).
            let mut bumped = candidate.to_be_bytes();
            bumped[6] = (bumped[6] & 0x0f) | 0x70;
            bumped[8] = (bumped[8] & 0x3f) | 0x80;
            candidate = u128::from_be_bytes(bumped);
        }
        *last = candidate;
        drop(last);

        uuid::Uuid::from_bytes(candidate.to_be_bytes())
            .hyphenated()
            .to_string()
    }
}

impl Default for IdMinter {
    fn default() -> Self {
        Self::new()
    }
}

fn system_now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn system_rand10() -> [u8; 10] {
    let b = uuid::Uuid::new_v4().into_bytes();
    std::array::from_fn(|i| b[i])
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Feed `bytes` through a duplex pipe and read back every frame until EOF.
    async fn read_all(bytes: &[u8]) -> Vec<Value> {
        let (mut writer, reader) = tokio::io::duplex(bytes.len().max(64) + 64);
        writer.write_all(bytes).await.expect("duplex write");
        drop(writer);
        let mut reader = tokio::io::BufReader::new(reader);
        let mut frames = Vec::new();
        while let Some(frame) = next_frame(&mut reader).await.expect("read frame") {
            frames.push(frame);
        }
        frames
    }

    fn is_v7(id: &str) -> bool {
        let parsed = uuid::Uuid::parse_str(id).expect("minted id parses");
        parsed.get_version() == Some(uuid::Version::SortRand)
            && parsed.get_variant() == uuid::Variant::RFC4122
            && id == id.to_lowercase()
            && id.len() == 36
    }

    #[test]
    fn request_ids_distinguish_int_from_string() {
        assert_ne!(
            RpcId::Int(1).pending_key(),
            RpcId::Str("1".into()).pending_key()
        );
        assert_eq!(
            RpcId::pending_key_for_raw(&serde_json::json!(1)),
            Some(RpcId::Int(1).pending_key())
        );
        assert_eq!(
            RpcId::pending_key_for_raw(&serde_json::json!("1")),
            Some(RpcId::Str("1".into()).pending_key())
        );
        // Non-string/non-integer ids never correlate.
        for raw in [serde_json::json!(1.5), serde_json::json!(true), Value::Null] {
            assert_eq!(RpcId::pending_key_for_raw(&raw), None, "{raw}");
            assert_eq!(RpcId::from_raw(&raw), None, "{raw}");
        }
    }

    #[test]
    fn pending_map_matches_by_exact_rendering() {
        let mut pending = Pending::new();
        assert!(pending.is_empty());
        pending.insert(&RpcId::Int(1), "int-one");
        pending.insert(&RpcId::Str("1".into()), "str-one");
        assert_eq!(pending.len(), 2);
        // Each rendering resolves only its own waiter.
        assert_eq!(pending.remove_raw(&serde_json::json!("1")), Some("str-one"));
        assert_eq!(pending.remove_raw(&serde_json::json!(1)), Some("int-one"));
        assert!(pending.is_empty());
        // Unknown ids and unparseable ids miss without panic.
        pending.insert(&RpcId::Int(7), "seven");
        assert_eq!(pending.remove_raw(&serde_json::json!(8)), None);
        assert_eq!(pending.remove_raw(&serde_json::json!("7")), None);
        assert_eq!(pending.remove_raw(&Value::Null), None);
        assert_eq!(pending.remove(&RpcId::Int(7)), Some("seven"));
    }

    #[test]
    fn uuidv7_shape_holds_for_system_and_deterministic_minters() {
        for id in (0..50).map(|_| IdMinter::new().mint()) {
            assert!(is_v7(&id), "{id}");
        }
        let fixed = IdMinter::with_sources(|| 1_789_348_928_664, || [0xab; 10]);
        for id in (0..50).map(|_| fixed.mint()) {
            assert!(is_v7(&id), "{id}");
        }
    }

    #[test]
    fn same_ms_burst_is_strictly_monotonic() {
        let minter = IdMinter::with_sources(|| 1_700_000_000_000, || [0x11; 10]);
        let mut prev = String::new();
        for _ in 0..10_000 {
            let id = minter.mint();
            assert!(id > prev, "not monotonic: {prev} -> {id}");
            prev = id;
        }
    }

    #[test]
    fn clock_regression_keeps_sequence_monotonic() {
        use std::sync::atomic::{AtomicU64, Ordering};
        let clock = std::sync::Arc::new(AtomicU64::new(1_800_000_000_000));
        let reader = clock.clone();
        let minter = IdMinter::with_sources(move || reader.load(Ordering::SeqCst), || [0x22; 10]);
        let before = minter.mint();
        clock.store(1_700_000_000_000, Ordering::SeqCst); // clock steps back
        let after = minter.mint();
        assert!(after > before, "{before} -> {after}");
        assert!(is_v7(&after));
    }

    #[test]
    fn minted_ids_are_unique_across_bursts() {
        use std::collections::HashSet;
        let minter = IdMinter::new();
        let ids: HashSet<String> = (0..5_000).map(|_| minter.mint()).collect();
        assert_eq!(ids.len(), 5_000);
    }

    #[test]
    fn response_validation_matrix() {
        // Happy paths.
        let ok = serde_json::json!({"jsonrpc": "2.0", "id": 4, "result": {"a": 1}});
        assert!(matches!(
            classify_frame(&ok),
            IncomingFrame::Response {
                id: RpcId::Int(4),
                body: Ok(_)
            }
        ));
        let err = serde_json::json!({
            "jsonrpc": "2.0", "id": "a1",
            "error": {"code": -32010, "message": "nope",
                      "data": {"kind": "capabilityRequired", "retryable": false}},
        });
        match classify_frame(&err) {
            IncomingFrame::Response {
                id: RpcId::Str(id),
                body: Err(e),
            } => {
                assert_eq!(id, "a1");
                assert_eq!(e.code, -32010);
                assert_eq!(e.kind(), Some("capabilityRequired"));
                assert_eq!(e.retryable(), Some(false));
            }
            other => panic!("expected typed error response, got {other:?}"),
        }
        // Both/neither result+error.
        for frame in [
            serde_json::json!({"jsonrpc": "2.0", "id": 1, "result": {}, "error": {}}),
            serde_json::json!({"jsonrpc": "2.0", "id": 1}),
        ] {
            match classify_frame(&frame) {
                IncomingFrame::Invalid { reason, id } => {
                    assert!(reason.contains("exactly one"), "{reason}");
                    assert_eq!(id, Some(RpcId::Int(1)));
                }
                other => panic!("expected Invalid, got {other:?}"),
            }
        }
        // Non-object result.
        let scalar = serde_json::json!({"jsonrpc": "2.0", "id": 2, "result": 42});
        assert!(matches!(
            classify_frame(&scalar),
            IncomingFrame::Invalid {
                id: Some(RpcId::Int(2)),
                ..
            }
        ));
        // Malformed errors: each missing member fails validation.
        for error in [
            serde_json::json!({"message": "x", "data": {"kind": "internal"}}),
            serde_json::json!({"code": -32603, "data": {"kind": "internal"}}),
            serde_json::json!({"code": -32603, "message": "x"}),
            serde_json::json!({"code": -32603, "message": "x", "data": {}}),
            serde_json::json!({"code": "NaN", "message": "x", "data": {"kind": "k"}}),
            serde_json::json!("boom"),
        ] {
            let frame = serde_json::json!({"jsonrpc": "2.0", "id": 3, "error": error});
            match classify_frame(&frame) {
                IncomingFrame::Invalid { reason, id } => {
                    assert!(reason.contains("error object"), "{reason}");
                    assert_eq!(id, Some(RpcId::Int(3)));
                }
                other => panic!("expected Invalid for {error}, got {other:?}"),
            }
        }
        // Null id never correlates.
        let null_id = serde_json::json!({"jsonrpc": "2.0", "id": null,
            "error": {"code": -32700, "message": "parse", "data": {"kind": "parseError"}}});
        assert!(matches!(
            classify_frame(&null_id),
            IncomingFrame::Invalid { id: None, .. }
        ));
        // Neither id nor method.
        assert!(matches!(
            classify_frame(&serde_json::json!({"jsonrpc": "2.0"})),
            IncomingFrame::Invalid { id: None, .. }
        ));
    }

    #[test]
    fn notifications_and_server_requests_classify() {
        let n = serde_json::json!({
            "jsonrpc": "2.0", "method": "item/delta",
            "params": {"itemId": "i"}, "emittedAtMs": 1,
            "futureMember": {"nested": true},
        });
        match classify_frame(&n) {
            IncomingFrame::Notification { method, params } => {
                assert_eq!(method, "item/delta");
                assert_eq!(params["itemId"], "i");
            }
            other => panic!("expected Notification, got {other:?}"),
        }
        // Omitted params decode as Null; unknown members ignored.
        let bare = serde_json::json!({"jsonrpc": "2.0", "method": "initialized"});
        assert!(matches!(
            classify_frame(&bare),
            IncomingFrame::Notification {
                params: Value::Null,
                ..
            }
        ));
        let req = serde_json::json!({
            "jsonrpc": "2.0", "id": 9, "method": "approval/request",
            "params": {"approvalId": "ap"},
        });
        match classify_frame(&req) {
            IncomingFrame::ServerRequest { id, method, params } => {
                assert_eq!(id, RpcId::Int(9));
                assert_eq!(method, "approval/request");
                assert_eq!(params["approvalId"], "ap");
            }
            other => panic!("expected ServerRequest, got {other:?}"),
        }
    }

    #[test]
    fn frame_builders_emit_the_documented_shapes() {
        let id = RpcId::Int(4);
        let req = request_frame(&id, "turn/cancel", &serde_json::json!({"a": 1}));
        assert_eq!(req["jsonrpc"], "2.0");
        assert_eq!(req["id"], 4);
        assert_eq!(req["method"], "turn/cancel");
        assert_eq!(req["params"], serde_json::json!({"a": 1}));

        let n = notification_frame("initialized", None);
        assert_eq!(
            n,
            serde_json::json!({"jsonrpc": "2.0", "method": "initialized"})
        );

        let ok = ok_response_frame(&RpcId::Str("s".into()));
        assert_eq!(
            ok,
            serde_json::json!({"jsonrpc": "2.0", "id": "s", "result": {}})
        );

        let nf = method_not_found_frame(&RpcId::Int(2), "future/method");
        assert_eq!(nf["error"]["code"], -32601);
        assert_eq!(nf["error"]["data"]["kind"], "methodNotFound");
        assert!(
            nf["error"]["message"]
                .as_str()
                .unwrap()
                .contains("future/method")
        );
    }

    #[tokio::test]
    async fn writer_emits_single_flushed_line() {
        let (mut client, mut server) = tokio::io::duplex(4096);
        let frame = serde_json::json!({"jsonrpc": "2.0", "id": 1, "nested": {"a": [1, 2]}});
        write_frame(&mut client, &frame).await.expect("write");
        drop(client);
        let mut out = Vec::new();
        server.read_to_end(&mut out).await.expect("read");
        let text = String::from_utf8(out).expect("utf8");
        assert!(text.ends_with('\n'));
        assert_eq!(text.lines().count(), 1, "must be one line, never pretty");
        assert_eq!(serde_json::from_str::<Value>(text.trim()).unwrap(), frame);
    }

    #[tokio::test]
    async fn outbound_cap_refuses_without_writing() {
        let (mut client, mut server) = tokio::io::duplex(64);
        let big = "x".repeat(FRAME_LIMIT_BYTES + 1);
        let frame = serde_json::json!({"blob": big});
        match write_frame(&mut client, &frame).await {
            Err(FrameError::Oversize { len }) => assert!(len > FRAME_LIMIT_BYTES),
            other => panic!("expected Oversize, got {other:?}"),
        }
        drop(client);
        let mut out = Vec::new();
        server.read_to_end(&mut out).await.expect("read");
        assert!(out.is_empty(), "refused frame must write nothing");
    }

    #[tokio::test]
    async fn reader_tolerates_crlf_blank_and_garbage_lines() {
        let good = r#"{"jsonrpc":"2.0","id":1,"result":{}}"#;
        let mut input = String::new();
        input.push_str("\n   \n\t\n"); // blank lines
        input.push_str(good);
        input.push_str("\r\n"); // CRLF
        input.push_str("{oops this is not json\n"); // garbage
        input.push_str("42\n"); // valid JSON, non-object frame
        input.push_str("[1,2]\n"); // valid JSON, non-object frame
        input.push_str(good);
        input.push('\n');
        let frames = read_all(input.as_bytes()).await;
        assert_eq!(frames.len(), 2, "only the two good frames survive");
        assert_eq!(frames[0]["id"], 1);
        assert_eq!(frames[1]["id"], 1);
    }

    #[tokio::test]
    async fn reader_skips_invalid_utf8_and_recovers() {
        let good = b"{\"jsonrpc\":\"2.0\",\"id\":9,\"result\":{}}\n";
        let mut input = vec![0xff, 0xfe, 0xfd, b'\n']; // invalid UTF-8 line
        input.extend_from_slice(good);
        let frames = read_all(&input).await;
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0]["id"], 9);
    }

    #[tokio::test]
    async fn reader_parses_final_line_without_newline_then_eof() {
        let frames = read_all(br#"{"jsonrpc":"2.0","id":5,"result":{}}"#).await;
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0]["id"], 5);
        let empty = read_all(b"").await;
        assert!(empty.is_empty());
    }

    #[tokio::test]
    async fn inbound_oversize_line_is_skipped_and_stream_resyncs() {
        let good = b"{\"jsonrpc\":\"2.0\",\"id\":3,\"result\":{}}\n";
        let mut input = Vec::new();
        input.extend_from_slice(good);
        input.extend(vec![b'x'; FRAME_LIMIT_BYTES + 512]);
        input.push(b'\n');
        input.extend_from_slice(good);
        let frames = read_all(&input).await;
        assert_eq!(
            frames.len(),
            2,
            "oversize line skipped, both good frames read"
        );
    }

    #[test]
    fn local_errors_carry_a_branchable_kind() {
        let e = ErrorObject::local(-32603, "timed out", "timeout");
        assert_eq!(e.code, -32603);
        assert_eq!(e.kind(), Some("timeout"));
    }
}
