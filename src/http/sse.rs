//! SSE framing and the turn-to-stream pump.
//!
//! Frames are plain strings (`data: {json}\n\n`, `event: {name}\ndata:
//! {json}\n\n`): `serde_json` never emits raw newlines, so one payload is
//! always one frame. [`sse_stream`] pumps a [`TurnHandle`] with keep-alive
//! ticks; each endpoint supplies a pure render function.

use std::collections::VecDeque;
use std::convert::Infallible;
use std::time::Duration;

use futures::Stream;
use serde_json::Value;

use crate::dispatch::TurnHandle;
use crate::msp::fold::OutputEvent;

/// Stream terminator for chat SSE (`data: [DONE]`, then close).
pub const DONE_FRAME: &str = "data: [DONE]\n\n";
/// Keep-alive comment (ignored by SSE clients, defeats idle timeouts).
pub const KEEPALIVE_FRAME: &str = ": ping\n\n";
/// Keep-alive beat for long turns.
pub const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(15);

/// One anonymous SSE frame (`data: {payload}\n\n`).
pub fn data_frame(payload: &Value) -> String {
    format!(
        "data: {}\n\n",
        serde_json::to_string(payload).unwrap_or_default()
    )
}

/// One named SSE frame (`event: {name}\ndata: {payload}\n\n`).
pub fn event_frame(name: &str, payload: &Value) -> String {
    format!(
        "event: {name}\ndata: {}\n\n",
        serde_json::to_string(payload).unwrap_or_default()
    )
}

/// What the pump hands the render function per wakeup.
#[derive(Debug)]
pub enum SsePoll {
    /// One folded turn event.
    Event(OutputEvent),
    /// Keep-alive beat (render to [`KEEPALIVE_FRAME`], never `done`).
    KeepAlive,
    /// The driver stopped without a terminal (defensive: render a 500
    /// error frame, then `done`).
    Closed,
}

/// Render output: frames to emit, plus whether the stream ends after them.
pub struct SseRender {
    /// Frames in emission order.
    pub frames: Vec<String>,
    /// Close the stream once `frames` drain.
    pub done: bool,
}

impl SseRender {
    /// Emit frames and continue.
    pub fn frames(frames: Vec<String>) -> Self {
        Self {
            frames,
            done: false,
        }
    }

    /// Emit frames, then close the stream.
    pub fn terminal(frames: Vec<String>) -> Self {
        Self { frames, done: true }
    }

    /// Emit one keep-alive comment, continue.
    pub fn keepalive() -> Self {
        Self {
            frames: vec![KEEPALIVE_FRAME.to_string()],
            done: false,
        }
    }
}

/// Pump a turn handle into an SSE byte stream. `preface` frames go first
/// (e.g. `response.created`); `render` maps each wakeup to frames; the
/// stream ends after a `done` render drains. Dropping the stream drops the
/// handle, which cancels the turn.
pub fn sse_stream<R>(
    handle: TurnHandle,
    preface: Vec<String>,
    keepalive: Duration,
    render: R,
) -> impl Stream<Item = Result<String, Infallible>>
where
    R: FnMut(SsePoll) -> SseRender + Send + 'static,
{
    struct Pump<R> {
        handle: TurnHandle,
        render: R,
        interval: tokio::time::Interval,
        queue: VecDeque<String>,
        done: bool,
    }
    let interval = tokio::time::interval_at(tokio::time::Instant::now() + keepalive, keepalive);
    let pump = Pump {
        handle,
        render,
        interval,
        queue: preface.into(),
        done: false,
    };
    futures::stream::unfold(pump, |mut pump| async move {
        loop {
            if let Some(frame) = pump.queue.pop_front() {
                return Some((Ok(frame), pump));
            }
            if pump.done {
                return None;
            }
            let poll = tokio::select! {
                biased;
                event = pump.handle.events.recv() => match event {
                    Some(event) => SsePoll::Event(event),
                    None => SsePoll::Closed,
                },
                _ = pump.interval.tick() => SsePoll::KeepAlive,
            };
            let rendered = (pump.render)(poll);
            pump.queue.extend(rendered.frames);
            pump.done = rendered.done;
        }
    })
}

/// Mint a response id (`{prefix}_{32 hex}`).
pub fn new_id(prefix: &str) -> String {
    format!("{prefix}_{}", uuid::Uuid::new_v4().simple())
}

/// Unix seconds for response envelopes.
pub fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Response headers for SSE streams.
pub fn sse_headers(headers: &mut axum::http::HeaderMap) {
    headers.insert(
        axum::http::header::CONTENT_TYPE,
        axum::http::HeaderValue::from_static("text/event-stream"),
    );
    headers.insert(
        axum::http::header::CACHE_CONTROL,
        axum::http::HeaderValue::from_static("no-cache"),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frames_match_sse_byte_layout() {
        assert_eq!(
            data_frame(&serde_json::json!({"a": 1})),
            "data: {\"a\":1}\n\n"
        );
        assert_eq!(
            event_frame("response.created", &serde_json::json!({"a": 1})),
            "event: response.created\ndata: {\"a\":1}\n\n"
        );
    }

    #[tokio::test]
    async fn pump_orders_preface_events_keepalive_and_close() {
        use crate::msp::fold::{TurnOutcome, Usage};
        use futures::StreamExt as _;
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        let handle = TurnHandle::for_tests(rx);
        tx.send(OutputEvent::ContentDelta("hi".to_string()))
            .await
            .unwrap();
        tx.send(OutputEvent::Terminal(TurnOutcome::Completed {
            usage: Usage::default(),
        }))
        .await
        .unwrap();
        drop(tx);
        let render = |poll: SsePoll| match poll {
            SsePoll::Event(OutputEvent::ContentDelta(t)) => {
                SseRender::frames(vec![format!("data: {t}\n\n")])
            }
            SsePoll::Event(OutputEvent::Terminal(_)) => {
                SseRender::terminal(vec![DONE_FRAME.to_string()])
            }
            SsePoll::KeepAlive => SseRender::keepalive(),
            _ => SseRender::terminal(vec![]),
        };
        let frames: Vec<String> = sse_stream(
            handle,
            vec!["data: pre\n\n".to_string()],
            Duration::from_secs(3600),
            render,
        )
        .map(|r| r.unwrap())
        .collect()
        .await;
        assert_eq!(frames, vec!["data: pre\n\n", "data: hi\n\n", DONE_FRAME]);
    }
}
