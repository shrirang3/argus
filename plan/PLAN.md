# Ollive Assignment — LLM Chat + Inference Observability Pipeline

## Context

Ollive assignment (`Assignment - Ollive`, Google Doc) asks for a chatbot **plus** a lightweight inference logging/ingestion system around it. The graded substance is not the chatbot — it is the observability plumbing: an auto-instrumenting SDK, an event-based ingestion pipeline, a sensible DB schema, and honest tradeoff documentation. The doc states the full bonus list yields a **guaranteed interview**, so we target all of it.

**Demo use case: a self-observing assistant.** The chatbot answers questions about its own inference telemetry (`"what's my p99 today?"`, `"which model is burning the most cost?"`). Chosen because it needs no external corpus, closes the demo loop in one screen recording, and — critically — generates *interesting* telemetry: 2–3 inferences per user turn across a cheap router model and a larger answer model, with real tool failures. A single-call generic chatbot would leave every dashboard panel a flat line.

Project root `/Users/shrirang/argus`. Python 3.13.9, Docker 29.6.2 available.

---

## Decisions (locked)

| Decision | Choice | Why |
|---|---|---|
| Stack | All Python — FastAPI everywhere, Jinja2 + vanilla JS UI | User preference. UI is plainer than React but fully capable of SSE streaming, cancel, resume. |
| Provider | **Groq** wired; OpenAI + Anthropic adapters present, switchable via `providers.yaml` | User has Groq keys. Multi-provider bonus satisfied structurally, not just aspirationally. |
| Orchestration | **LangGraph, checkpointer disabled** | Demo has real branching (route → tools → answer). Skipping the checkpointer keeps `conversations`/`messages` in *our* schema — the brief explicitly grades schema design, and LangGraph's `checkpoints`/`checkpoint_blobs` tables are opaque msgpack we can't join against. Also gives the README its strongest line: instrumentation proven against a framework we don't control. |
| Persistence | 100% our own Postgres tables | See above. LangGraph never touches the DB. |
| Instrumentation | Monkey-patch provider client classes at import time | Works on code we don't own — including LangGraph internals. An explicit wrapper would capture nothing there. |
| Event schema | **OTel GenAI semantic conventions** on the wire; flat typed columns in Postgres | `gen_ai.*` names make "works with any stack" provable, not theoretical. Flat columns keep the dashboard queryable and indexable. Mapping happens at the ingest boundary. |
| Delivery guarantee | At-least-once, deduplicated at write via `event_id` | Exactly-once across HTTP + queue + DB costs far more than a unique constraint. |
| Rejected | Pipecat | Voice/WebRTC pipeline framework. Wrong domain, unused deps, and its built-in metrics would replace the deliverable being graded. |
| Rejected | LangSmith / Langfuse | They *are* the product we were asked to build. |

---

## Architecture

```
Browser (Jinja2 + vanilla JS, SSE + AbortController)
   │
   ▼
chat-app (FastAPI)
   │  LangGraph: route → tools → answer  (no checkpointer)
   │  ├─ writes conversations / messages ──────────────► Postgres
   │  └─ LLM calls ──► Groq / OpenAI / Anthropic
   │                        ▲
   │                        │ intercepted, transparently, by
   │              argus SDK (monkey-patch at import time)
   │                        │  async buffered emit — NEVER blocks the chat path
   ▼                        ▼
              ingestion (FastAPI)
              /v1/events  (native JSON batch)
              /v1/traces  (OTLP)
              → Pydantic validate → PII redact → XADD
                        │
                        ▼
              Redis Streams  `llm.inference.v1`  (+ DLQ stream)
                        │  consumer group `ingest-workers`
                        ▼
              worker — idempotent upsert + 1-min rollups
                        │
                        ▼
                    Postgres ──► dashboard (FastAPI + Chart.js)
                        ▲
                        └── agent tools read this back (the loop closes)
```

**Invariant:** the chat request path never awaits the logging path. Emit is fire-and-forget onto an in-process `asyncio.Queue` drained by a background task. Ingestion down ⇒ retry → buffer → spill to local JSONL. Chat keeps serving.

---

## Repo layout

```
argus/
├── docker-compose.yml
├── .env.example
├── README.md
├── ARCHITECTURE.md
├── Makefile
├── packages/
│   └── argus/                    # the SDK — installable, app-agnostic
│       ├── pyproject.toml
│       └── argus/
│           ├── __init__.py        # init(), conversation(), wrap()
│           ├── instrument.py      # monkey-patch provider client classes
│           ├── adapters/          # groq.py, openai.py, anthropic.py
│           ├── emitter.py         # async buffered batch sender + disk spill
│           ├── redact.py          # PII regex engine
│           ├── schema.py          # InferenceEvent (OTel gen_ai.* aligned)
│           ├── otel.py            # optional OTLP span exporter path
│           └── context.py         # contextvars: conversation_id, session_id
├── services/
│   ├── chat_app/
│   │   ├── agent.py               # LangGraph StateGraph — GENERIC loop
│   │   ├── tools.py               # ◄── the only opinionated file
│   │   ├── prompts.py             # ◄── and this one
│   │   ├── providers.yaml         # model registry + pricing
│   │   ├── routes.py, db.py
│   │   └── templates/ static/
│   ├── ingestion/
│   ├── worker/
│   └── dashboard/
├── db/
│   ├── migrations/                # alembic
│   └── schema.sql
├── k8s/
└── tests/
```

`argus` is `pip install -e`'d into every service — a real package, not a helper module. The use case lives in exactly two files (`tools.py`, `prompts.py`); everything else is domain-agnostic, and the README points at that boundary.

---

## Component detail

### 1. `argus` SDK — auto-instrumentation

```python
import argus
argus.init(endpoint="http://ingestion:8001/v1/events", service="chat-app")
```

One call at startup. Zero call-site changes anywhere else.

`instrument.py` patches, only if the module imports cleanly:
- `groq.resources.chat.completions.Completions.create` **and** `AsyncCompletions.create`
- the OpenAI and Anthropic equivalents

The async patch matters — `langchain_groq.ChatGroq` uses `AsyncGroq` underneath, so missing it means silently logging nothing from the agent.

Wrapper stamps `started_at`, calls through, pulls usage via the matching adapter, builds an `InferenceEvent`, emits, returns the untouched response object. On exception: classify (`error` / `timeout` / `rate_limited`), emit, re-raise the original.

**Streaming:** `create(stream=True)` returns an iterator. Wrap it in a proxy generator that stamps `ttft_ms` on first chunk, accumulates output, and emits in `try/except/finally` — the `finally` is what makes a client disconnect produce a `status="cancelled"` row with partial token counts.

**Context:** `contextvars.ContextVar` set once per HTTP request, inherited across `await` boundaries and into LangGraph node execution. No threading IDs through signatures.

**Emitter:** `asyncio.Queue(maxsize=10_000)` → batches of ≤50 / 500ms → POST with 3 retries, exponential backoff. Queue full ⇒ drop oldest, increment `argus_dropped_total` (surfaced on the dashboard — visible loss beats silent loss). Retries exhausted ⇒ append to `/var/log/argus/spill.jsonl` for replay.

**Double-counting guard:** LangChain also exposes usage on `AIMessage.usage_metadata`. We ignore it entirely — the patch is the single source of truth. Documented, because it's a real trap.

### 2. `chat-app` — agent layer

LangGraph, **compiled without a checkpointer**:

```
route (cheap model, temp=0, tools bound) ──tool_calls?──► tools ──┐
        │                                                          │
        │ no                                            (loop back)┘
        ▼
     answer (larger model)  ──► END
```

`MAX_STEPS = 4` guard in state; on exhaustion, force a final answer rather than erroring. Three nodes regardless of tool count — a tool is one dict entry, not a node.

Per turn: load last 10 turns from `messages`, insert the user row, `graph.ainvoke`, insert the assistant row. LangGraph is stateless between turns; our DB is the memory.

**Drift control** (three stacked levers, to name in the README):
- system prompt pinned first, never trimmed — the primary scope lever
- 10-turn window — bounds cost/latency growth, stops old turns pulling the model sideways
- `temperature=0` on route, higher on answer

Tradeoff: trimming loses long-range facts. Rolling summarization was the alternative — costs an extra inference per N turns and adds a failure mode. Chose the simple one deliberately.

Routes:

| route | purpose |
|---|---|
| `GET /` | chat shell + conversation sidebar |
| `POST /api/conversations` | new |
| `GET /api/conversations` | list (bonus) |
| `GET /api/conversations/{id}` | resume — full history (bonus) |
| `DELETE /api/conversations/{id}` | soft delete |
| `POST /api/conversations/{id}/messages` | SSE stream of the reply |
| `POST /api/conversations/{id}/cancel` | cancel in-flight (bonus) |

Cancel: browser `AbortController` closes SSE → server task cancelled → SDK `finally` emits `status="cancelled"`.

### 3. Tool registry — `services/chat_app/tools.py`

```
get_latency_stats(window, provider?, model?)   → p50 / p95 / p99, TTFT
get_error_summary(window)                      → counts by error_type × provider
get_cost_breakdown(window, group_by)           → cost per model
list_conversations(limit)                      → recent chats + message counts
get_conversation_trace(conversation_id)        → every inference in that chat
```

**Parameterized queries only — no free-form SQL tool.** An LLM-authored `run_sql` against our own DB is an injection and runaway-scan risk. Each tool is a fixed statement with bound params. Tool errors are caught, returned to the model as a tool message, and logged — never raised into the chat path.

**Freshness rule:** windows < 15 min read raw `inference_logs`; wider windows read the rollup table. Without this, asking *"latency in the last minute?"* returns near-empty because the worker writes 1-minute buckets. Deliberate freshness/performance tradeoff — goes in the README.

### 4. `ingestion`

- `POST /v1/events` — native JSON batch
- `POST /v1/traces` — OTLP, so any OTel-emitting stack feeds the same pipeline

Pydantic v2 validates each row independently. Invalid rows → `dead_letter_events` with raw payload + validation error; **one bad event never poisons a batch**. Valid rows → PII redaction (defense in depth; SDK already redacted) → `XADD` to `llm.inference.v1`. Returns `202` immediately — no DB wait. `GET /health` checks Redis for k8s probes.

### 5. `worker`

Consumer group `ingest-workers`, so N replicas share load and `XAUTOCLAIM` recovers from a crashed consumer. Per message: insert with `ON CONFLICT (event_id) DO NOTHING`, bump the 1-min rollup, then `XACK`. **Ack after commit** — crash mid-write means redelivery, and the conflict clause absorbs it. 3+ delivery attempts ⇒ DLQ stream.

### 6. `dashboard`

FastAPI + Chart.js (vendored, no CDN):
- latency p50 / p95 / p99 over time, plus TTFT for streamed calls
- throughput: requests/min, tokens/min
- error rate % by provider and `error_type`
- cost from tokens × `providers.yaml` pricing
- recent inferences table — previews, filter by conversation / provider / status
- SDK health: dropped-event counter, DLQ depth, consumer lag

Time series read rollups; the recent table reads raw.

---

## Database schema (Postgres)

```
conversations
  id UUID PK, title TEXT, status TEXT ('active'|'archived'|'deleted'),
  provider TEXT, model TEXT, created_at, updated_at, message_count INT

messages
  id UUID PK, conversation_id FK, seq INT, role TEXT ('user'|'assistant'|'system'|'tool'),
  content TEXT, tool_call_id TEXT, token_count INT, created_at
  UNIQUE (conversation_id, seq)

inference_logs                      -- partitioned monthly on started_at
  id BIGSERIAL, event_id UUID UNIQUE, conversation_id UUID, message_id UUID,
  service TEXT, provider TEXT, model TEXT, operation TEXT ('chat'|'tool_route'),
  status TEXT ('success'|'error'|'cancelled'|'timeout'|'rate_limited'),
  error_type TEXT, error_message TEXT,
  latency_ms INT, ttft_ms INT, streamed BOOL,
  prompt_tokens INT, completion_tokens INT, total_tokens INT, cost_usd NUMERIC(12,6),
  input_preview TEXT, output_preview TEXT,     -- 500 chars, redacted
  redaction_hits JSONB,                        -- {"email":2,"phone":1}
  request_params JSONB, finish_reason TEXT,
  started_at TIMESTAMPTZ, ended_at TIMESTAMPTZ, ingested_at TIMESTAMPTZ

inference_metrics_1m                -- rollup, written by worker
  bucket TIMESTAMPTZ, provider TEXT, model TEXT, status TEXT,
  count INT, sum_latency_ms BIGINT, sum_tokens BIGINT, sum_cost NUMERIC,
  latency_p50/p95/p99 INT
  PK (bucket, provider, model, status)

dead_letter_events
  id BIGSERIAL, raw JSONB, error TEXT, source TEXT, created_at
```

**Wire ↔ column mapping.** On the wire, fields use OTel GenAI names (`gen_ai.system`, `gen_ai.request.model`, `gen_ai.usage.input_tokens`, `gen_ai.conversation.id`, `error.type`). In Postgres they land as flat typed columns. Rationale: OTel names buy interoperability at the boundary; flat columns buy indexes and fast dashboard queries. Storing raw OTel attribute bags as JSONB would forfeit both.

**Decisions to defend in the README:**
- Previews (500 chars) in `inference_logs`, full text only in `messages` — bounds log-table growth and limits blast radius if logs are ever exported.
- Monthly partitioning on `started_at` — append-heavy, time-queried; dropping a partition beats `DELETE`.
- Rollup table over raw scans — dashboard stays fast past millions of rows; cost is ~1 min staleness, mitigated by the <15 min freshness rule.
- JSONB for `request_params` / `redaction_hits` — provider params keep changing; avoids a migration per provider quirk.
- Indexes: `(started_at DESC)`, `(conversation_id, started_at)`, `(provider, model, started_at)`, `(status, started_at) WHERE status <> 'success'`.
- `messages` ↔ `inference_logs` deliberately **not** hard-FK'd — logs must survive conversation deletion for analytics; the ID is a soft reference.

---

## PII redaction

`redact.py`: compiled regexes for email, international + Indian phone, credit card (Luhn-verified to cut false positives), Aadhaar, PAN, IPv4/IPv6, JWT, and `sk-`/`gsk_`-style API keys. Replacement is a typed token (`[EMAIL_REDACTED]`) so text stays readable. Runs **in the SDK before the event leaves the process**, and again at the ingestion edge. Counts land in `redaction_hits`. Configurable rules + dev opt-out flag.

---

## Roadmap

| Phase | Deliverable | Done when |
|---|---|---|
| **P0** Skeleton | Repo layout, `docker-compose.yml` (postgres, redis, 4 services), `.env.example`, alembic baseline, Makefile | `docker compose up` boots; every `/health` green |
| **P1** Chat core | FastAPI chat app, Groq streaming SSE, 10-turn context, persistence, sidebar list / resume / cancel | Real conversation in browser; refresh resumes it |
| **P2** SDK | `argus`: schema, contextvars, Groq adapter, sync+async patch, streaming proxy, buffered emitter, redaction | Logs appear after adding only `argus.init()` — zero call-site changes |
| **P3** Ingestion | Validation, per-row DLQ, redaction, `XADD`, 202, `/v1/traces` OTLP | Malformed event → DLQ; valid → stream |
| **P4** Worker | Consumer group, idempotent upsert, rollups, `XAUTOCLAIM`, poison handling | Kill worker mid-batch → restart → zero loss, zero duplicates |
| **P5** Agent | LangGraph route/tools/answer, `MAX_STEPS`, tool registry, freshness rule | Ask *"what's my p99?"* → correct answer, 3 new rows logged for that turn |
| **P6** Dashboard | Latency/throughput/error/cost charts, recent table, SDK health | Charts move under load; p99 visibly separates from p50 |
| **P7** Multi-provider | OpenAI + Anthropic adapters, `providers.yaml`, per-conversation switch | Same conversation switched mid-flight; both rows normalized identically |
| **P8** k8s | namespace, deployments, services, ingress, secrets, configmap, HPA on worker; tested on kind/k3s | `kubectl apply -k k8s/` yields a working stack |
| **P9** Docs + demo | README (setup / architecture / schema decisions / tradeoffs / next), `ARCHITECTURE.md` (ingestion flow, logging strategy, scaling, failure assumptions), screenshots or Loom | Fresh clone → `docker compose up` → working app, from README alone |

Critical path P0→P2. P5 makes the demo click; P6 is the highest-visibility artifact for a reviewer.

---

## Failure handling — assumptions to state explicitly

| Failure | Behavior |
|---|---|
| Provider 5xx / timeout | Error surfaced to user; row written with `status="error"` + `error_type` |
| Ingestion down | SDK retries → buffers → spills to disk. **Chat unaffected.** |
| Redis down | Ingestion 503s; SDK buffers. Nothing written, nothing lost from spill |
| Postgres down | Worker withholds `XACK`; messages redeliver on recovery |
| Worker crash mid-write | `XAUTOCLAIM` reassigns; `ON CONFLICT DO NOTHING` absorbs the duplicate |
| SDK queue saturated | Oldest dropped, counter incremented, visible on dashboard |
| User cancels stream | `finally` emits `status="cancelled"` with partial tokens |
| Tool raises | Caught, returned to model as tool message, logged. Never reaches the user as a 500 |
| Agent loops | `MAX_STEPS=4` → forced final answer |

Delivery is **at-least-once, deduplicated at write**.

---

## Scaling notes (for ARCHITECTURE.md)

- Queue absorbs spikes; chat latency decoupled from DB write latency.
- Workers scale horizontally via the consumer group; HPA keyed on stream depth.
- `inference_logs` partitioned monthly; retention = drop partitions past 90 days.
- Dashboard reads rollups, not raw.
- Past this scale: sample successful calls at 10% while keeping 100% of errors; Redis Streams → Kafka for multi-day replay; rollups → TimescaleDB continuous aggregates or ClickHouse.

---

## Verification

1. **Boot:** `docker compose up -d`; `curl` every `/health`.
2. **Chat E2E:** 3 turns in browser, context carries; refresh → resumes from DB.
3. **Auto-instrumentation proof:** grep chat-app source — no logging calls beyond `argus.init()`; rows still land. Confirm LangGraph-internal calls are captured (the async patch).
4. **Cancel:** start a long generation, cancel → row with `status='cancelled'` and non-null partial `completion_tokens`.
5. **Agent:** ask *"what's my p99 latency in the last 5 minutes?"* → answer correct against a direct SQL check; `MAX_STEPS` respected on a deliberately looping prompt.
6. **Freshness:** query a <15 min window → served from raw, not rollups.
7. **Load:** `hey`/`locust` at ~50 concurrent against a mock provider; `count(inference_logs) == requests sent`; p99 populates.
8. **Chaos:** (a) stop `ingestion` mid-load → chat fine, spill grows, restart → replays. (b) stop `postgres` → worker stalls, no acks, restart → drains clean. (c) `kill -9` worker mid-batch → no duplicates.
9. **Redaction:** prompt containing email + phone + card → previews show only tokens; `redaction_hits` counts match.
10. **Multi-provider:** switch model mid-conversation → both rows normalized identically.
11. **OTLP:** send a raw OTel GenAI span to `/v1/traces` → lands in `inference_logs` alongside native events.
12. **Tests:** `pytest` — unit (redaction, adapters, schema, step guard) + integration (ingestion→redis→worker→postgres via testcontainers).
13. **k8s:** `kind create cluster && kubectl apply -k k8s/` → port-forward → chat works; scale worker to 3 → no duplicate rows.
