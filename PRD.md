# Muse Bridge (Subscription Proxy)
1. What:
- single Rust binary
- running on localhost
- exposes openai-compat HTTP API and translates each request -> Muse Session Protocol (MSP)
- speaks to a `muse serve` child process over stdio.
- Must work with ChatGPT desktop app and Hermes Agent (Desktop App)
- Zed ACP agent also wanted if extra work is not immense. Can defer to v2. 
- Authentication comes from user's existing `muse login`, inherited by `muse serve`.
- proxy handles no API keys and stores no creds. 

2. WHY:
- Be able to use muse code subscription in other agent clients (esp. Hermes Agent, chatGPT desktop app, Zed & T3Code [via ACP]).
- Meta provides developers with `muse serve` and MSP to programmatically drive Muse sessions

3. **Reference Documentation**:
- Meta MSP docs hub: https://meta-models.github.io/muse-code-sdk/guides/msp-wire/
	- concepts, wire guide, generated wire-protocol reference, sdk reference
- SDK repo: github.com/meta-models/muse-code-sdk
- Hermes Agent docs & source: 
	- https://github.com/NousResearch/hermes-agent
	- https://hermes-agent.nousresearch.com/docs/user-stories

- Codex docs & source: 
	- https://github.com/openai/codex

- Reference Implementations: `brokkai/muse-acp`(MSP)
	- https://github.com/BrokkAi/muse-acp

4. **Anticipated Tech Stack:**
- Rust, build time dependencies only, ships as single binary
- tokio 
- axum 
- serde/serde_json
- clap
- tracing
- distribution: cargo build --release; requires muse binary on PATH

5. **Research before implementing:**
- Can Hermes and/or chatgpt apps consume WebSockets?
- `/v1/responses`
