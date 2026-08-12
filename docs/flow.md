# Architecture & data flow

Living document — describes what exists today, and marks what does not. Updated as each
phase lands. Every number in here was measured, not estimated.

**Current state:** the chat app and the SDK are complete. Events reach the ingestion
service and stop there, in memory. Redis, the worker and the log tables are next.

---

## 1. Two paths

The system is two pipelines that touch at exactly one point. Keeping them separate is
the whole design, so they are drawn separately.

- **Request path** — the user's turn. Synchronous, must be fast.
- **Telemetry path** — what happened. Asynchronous, must never block the first.

### 1a. Request path

```
        ┌───────────────────────────────┐
        │            BROWSER            │
        └───────────────┬───────────────┘
                        │  ①  POST /api/conversations/{id}/messages
                        │      {"content": "..."}
                        ▼
        ┌───────────────────────────────┐
        │      chat-app  routes.py      │
        └───────────────┬───────────────┘
                        │  ②  INSERT message · UPDATE counters
                        │      SELECT last 20 rows
                        ▼
        ┌───────────────────────────────┐
        │    POSTGRES   conversations   │
        │               messages        │
        └───────────────┬───────────────┘
                        │  ③  [{role, content}, ...]
                        ▼
        ┌───────────────────────────────┐
        │       chat-app  llm.py        │
        └───────────────┬───────────────┘
                        │  ④  messages[] · stream=true
                        ▼
        ┌───────────────────────────────┐
        │           GROQ API            │
        └───────────────┬───────────────┘
                        │  ⑤  SSE chunks · usage on the last one
                        ▼
        ┌───────────────────────────────┐
        │     chat-app  _generate()     │
        └───────────────┬───────────────┘
                        │  ⑥  event: token  /  event: done
                        ▼
        ┌───────────────────────────────┐
        │            BROWSER            │
        └───────────────────────────────┘
```

### 1b. Telemetry path

Branches off at ⑤, inside the Groq call, and never rejoins.

```
                   ⑤  the provider call above
                        │
                        ▼
        ┌───────────────────────────────┐
        │   argus SDK   PATCH POINT     │   packages/argus/
        │   AsyncCompletions.create     │
        │                               │
        │   contextvar → conversation   │
        │   redact input / output       │
        │   TTFT on first token         │
        │   emit() → deque(10 000)      │
        └───────────────┬───────────────┘
                        │  ⑦  POST /v1/events
                        │      batch ≤50, every 500ms
                        ▼
        ┌───────────────────────────────┐          ┌──────────────────┐
        │      ingestion  :8001         ├─────────►│    quarantine    │
        │      validate each row        │  invalid │  dead letter     │
        └───────────────┬───────────────┘          └──────────────────┘
                        │
     ═══════════════════╪═══════════════════  everything below is P3 / P4
                        │
                        │  ⑧  XADD  llm.inference.v1
                        ▼
        ┌───────────────────────────────┐
        │         REDIS STREAMS         │
        └───────────────┬───────────────┘
                        │      XREADGROUP · group ingest-workers
                        ▼
        ┌───────────────────────────────┐
        │            worker             │
        │     idempotent · rollups      │
        └───────────────┬───────────────┘
                        │  ⑨  INSERT ... ON CONFLICT (event_id) DO NOTHING
                        ▼
        ┌───────────────────────────────┐
        │  POSTGRES   inference_logs    │
        │             inference_metrics │
        └───────────────┬───────────────┘
                        │      SELECT
                        ▼
        ┌───────────────────────────────┐
        │       dashboard  :8002        │   p50 · p95 · p99 · cost
        └───────────────────────────────┘
```

The P5 agent reads `inference_logs` back through its tools, which is what closes the
demo loop: the chatbot answers questions about the telemetry its own answers produced.

### The one invariant

**The request path never waits on the telemetry path.** Hop ⑦ is fire-and-forget — the
SDK appends to a bounded in-memory buffer and returns. Everything below it can be slow,
broken or entirely absent and the chat keeps serving.

Measured: with `ingestion` stopped mid-traffic, the chat answered normally, the emitter
recorded one send failure, and the event landed in `spill.jsonl` on disk.

---

## 2. Data formats, hop by hop

### ① browser → chat-app

`POST /api/conversations/{id}/messages`

```json
{"content": "what is my p99 latency?"}
```

One string. **The browser never sends conversation history** — it could be forged, and
the server already has it indexed.

### ② chat-app → Postgres

```sql
-- persist the user turn
INSERT INTO messages (id, conversation_id, seq, role, content) VALUES (...);
UPDATE conversations SET message_count = message_count + 1, updated_at = now()
 WHERE id = $1;

-- build the prompt: newest 20 rows, reversed in Python to oldest-first
SELECT * FROM messages WHERE conversation_id = $1 ORDER BY seq DESC LIMIT 20;
```

### ③ Postgres → chat-app

ORM rows, flattened to the provider's message shape:

```python
[{"role": "user", "content": "..."}, {"role": "assistant", "content": "..."}]
```

### ④ chat-app → Groq

OpenAI wire shape, via the official `groq` SDK (**not** raw HTTP — see §6):

```json
{"model": "llama-3.3-70b-versatile",
 "messages": [{"role": "user", "content": "..."}],
 "stream": true}
```

### ⑤ Groq → chat-app

SSE, OpenAI chunk shape. Usage arrives on the **final** chunk, unprompted:

```
data: {"choices":[{"delta":{"content":"P99 "},"finish_reason":null}]}
data: {"choices":[{"delta":{},"finish_reason":"stop"}],
       "usage":{"prompt_tokens":45,"completion_tokens":26,
                "queue_time":0.164,"prompt_time":0.001,"completion_time":0.122}}
data: [DONE]
```

`queue_time` is Groq-specific and worth keeping: on a measured call it was **0.164s
against 0.018s of actual generation** — nine tenths of the wait was queueing, invisible
in end-to-end latency alone.

### ⑥ chat-app → browser

Our own SSE vocabulary — named events, so the client dispatches without inspecting the
payload:

```
event: token
data: {"text": "P99 "}

event: done
data: {"status":"success","tokens":26,"prompt_tokens":45,
       "provider":"groq","model":"llama-3.3-70b-versatile"}

event: error
data: {"message": "..."}
```

`status` lives **inside** the stream because HTTP 200 is committed the moment the
connection opens, before the model has produced anything.

### ⑦ SDK → ingestion

`POST /v1/events`, batched. This is the `InferenceEvent` contract:

```json
{"events": [{
  "event_id": "c41d8db9-d004-4579-9c03-20f070117b5c",
  "conversation_id": "30dc0d2f-27e8-433e-a713-e44b4adaec3d",
  "message_id": null,
  "session_id": null,
  "service": "chat-app",

  "provider": "groq",
  "model": "llama-3.3-70b-versatile",
  "response_model": "llama-3.3-70b-versatile",
  "operation": "chat",

  "status": "success",
  "error_type": null,
  "error_message": null,
  "finish_reason": "stop",

  "started_at": "2026-08-12T03:55:42.924067Z",
  "ended_at":   "2026-08-12T03:55:43.295397Z",
  "latency_ms": 371,
  "ttft_ms": 271,
  "streamed": true,

  "prompt_tokens": 45,
  "completion_tokens": 26,
  "total_tokens": 71,

  "input_preview":  "In one short sentence, what is TTFT?",
  "output_preview": "TTFT stands for ...",
  "redaction_hits": {"email": 1, "card": 1},

  "request_params": {"queue_time": 0.164, "prompt_time": 0.001,
                     "completion_time": 0.122, "total_time": 0.123},
  "sdk_version": "0.1.0"
}]}
```

Field notes worth knowing:

| Field | Why it exists |
|---|---|
| `event_id` | Generated client-side. The idempotency key that makes at-least-once delivery safe to dedupe at write |
| `status` | `success` / `error` / `cancelled` / `timeout` / `rate_limited`. Separated because each demands a different response |
| `ttft_ms` | Time to first token. For a streamed call this is what a user perceives as speed |
| `*_preview` | 500 chars, already redacted. Full text lives in `messages` |
| `redaction_hits` | Proves redaction ran, rather than assuming it |
| `request_params` | Provider extras land here — JSONB downstream, so a new vendor quirk needs no migration |

`InferenceEvent.to_otel_attributes()` renders the same event under OpenTelemetry GenAI
semantic conventions (`gen_ai.system`, `gen_ai.usage.input_tokens`, …). The OTLP receiver
that consumes that form lands in P3.

### ⑧ ingestion → Redis *(P3)*

```
XADD llm.inference.v1 * data '<event json>'
```

### ⑨ worker → Postgres *(P4)*

```sql
INSERT INTO inference_logs (...) VALUES (...)
  ON CONFLICT (event_id) DO NOTHING;     -- absorbs redelivery
-- then bump inference_metrics_1m, then XACK (never before the commit)
```

---

## 3. APIs

| Service | Method | Path | Body → Returns | Status |
|---|---|---|---|---|
| chat | `GET` | `/` | HTML shell | ✅ |
| chat | `GET` | `/health` | `{status, service}` | ✅ |
| chat | `GET` | `/sdk-stats` | `{emitted, sent, dropped, spilled, send_failures}` | ✅ |
| chat | `POST` | `/api/conversations` | — → `{id, title, created_at, message_count}` | ✅ |
| chat | `GET` | `/api/conversations` | — → `[{...}]` | ✅ |
| chat | `GET` | `/api/conversations/{id}` | — → `{..., messages[]}` · 404 | ✅ |
| chat | `DELETE` | `/api/conversations/{id}` | — → 204, soft delete | ✅ |
| chat | `POST` | `/api/conversations/{id}/cancel` | — → 202 `{cancelled}` | ✅ |
| chat | `POST` | `/api/conversations/{id}/messages` | `{content}` → **SSE** · 400/404 | ✅ |
| ingestion | `POST` | `/v1/events` | `EventBatch` → `{accepted, rejected}` | 🟡 in-memory |
| ingestion | `GET` | `/v1/events/recent` | — → `{counts, events[]}` | 🟡 verification only |
| ingestion | `GET` | `/v1/events/rejected` | — → `{count, rows[]}` | 🟡 |
| ingestion | `POST` | `/v1/traces` | OTLP spans | ⚪ P3 |
| dashboard | `GET` | `/health` | `{status, service}` | ✅ |
| dashboard | `GET` | `/api/metrics/*` | latency / throughput / errors / cost | ⚪ P6 |

Two status-code choices that are deliberate rather than incidental:

- **`POST /cancel` → 202, not 200.** It sets a flag the generator reads on its next
  token. 202 Accepted is honest about being asynchronous.
- **Malformed conversation id → 404, not 422.** A client cannot distinguish "wrong
  format" from "does not exist", and should not be able to.

---

## 4. Database

### Built — migration `35dd8f15fad2`

```
conversations
  id             UUID  PK
  title          TEXT           default 'New conversation'
  status         TEXT           CHECK in (active, archived, deleted)
  provider       TEXT
  model          TEXT
  message_count  INT            denormalised — sidebar never COUNTs
  created_at     TIMESTAMPTZ
  updated_at     TIMESTAMPTZ

  IX ix_conversations_updated  (updated_at DESC) WHERE status <> 'deleted'
```

```
messages
  id               UUID  PK
  conversation_id  UUID  FK → conversations  ON DELETE CASCADE
  seq              INT            per-conversation ordinal
  role             TEXT           CHECK in (user, assistant, system, tool)
  content          TEXT
  tool_call_id     TEXT
  token_count      INT
  truncated        BOOL           set when a stream was cut short
  created_at       TIMESTAMPTZ

  UQ uq_messages_conversation_seq  (conversation_id, seq)
  IX ix_messages_conversation_seq  (conversation_id, seq)
```

**Three decisions to defend:**

**Soft delete.** Inference logs reference conversations and must outlive them; analytics
should not vanish because a user cleared their sidebar. The cost is that every read must
filter — mitigated by routing all reads through `repo.py`.

**`seq` instead of ordering by `created_at`.** `now()` in Postgres is *transaction* time,
so rows written together share a timestamp and the planner breaks ties arbitrarily. An
explicit ordinal removes the ambiguity; the unique constraint makes gaps and duplicates
impossible rather than merely unlikely.

**Partial index.** It carries only the rows the sidebar query can return — smaller, more
of it stays cached, and deleted rows cost nothing to maintain. Confirmed with `EXPLAIN`:
`Index Scan using ix_conversations_updated`, not a sequential scan.

### Planned — P3 / P4

```
inference_logs                      -- partitioned monthly on started_at
  event_id UUID UNIQUE              -- dedupe key
  conversation_id UUID              -- soft reference, NO FK: logs outlive conversations
  service · provider · model · operation
  status · error_type · error_message · finish_reason
  latency_ms · ttft_ms · streamed
  prompt_tokens · completion_tokens · total_tokens · cost_usd
  input_preview · output_preview
  redaction_hits JSONB · request_params JSONB
  started_at · ended_at · ingested_at

  IX (started_at DESC)
  IX (conversation_id, started_at)
  IX (provider, model, started_at)
  IX (status, started_at) WHERE status <> 'success'    -- partial: errors are rare
```

```
inference_metrics_1m                -- rollup written by the worker
  PK (bucket, provider, model, status)
  count · sum_latency_ms · sum_tokens · sum_cost
  latency_p50 · latency_p95 · latency_p99
```

```
dead_letter_events
  raw JSONB · error · source · created_at
```

Dashboard time series read the rollup, not the raw table. Cost: about a minute of
staleness — which is why the P5 agent tools read **raw** for windows under 15 minutes and
rollups for anything wider.

---

## 5. Failure handling

| Failure | Behaviour |
|---|---|
| Provider 5xx / timeout | Surfaced to the user; row written with `status=error` / `timeout` |
| Provider 429 | `status=rate_limited` — its own series, not buried in errors |
| Ingestion down | SDK retries with backoff → buffers → spills to disk. **Chat unaffected** ✅ measured |
| Redis down *(P3)* | Ingestion 503s; SDK buffers. Nothing lost from the spill file |
| Postgres down *(P4)* | Worker withholds `XACK`; messages redeliver on recovery |
| Worker crash mid-write | `XAUTOCLAIM` reassigns; `ON CONFLICT DO NOTHING` absorbs the duplicate |
| SDK buffer full | Oldest dropped, counter incremented, visible on `/sdk-stats` |
| User cancels a stream | `finally` in the proxy emits `status=cancelled` with partial output ✅ measured |
| Stream abandoned, never consumed | `__del__` emits `cancelled` — it still happened and still cost tokens |

Delivery guarantee: **at-least-once, deduplicated at write.** Exactly-once across an HTTP
hop, a queue and a database costs far more than a unique constraint does.

---

## 6. Two constraints that shaped the design

**The SDK dictates the integration.** The provider adapter goes through the official
`groq` SDK rather than `httpx`, because instrumentation patches
`groq.resources.chat.completions.AsyncCompletions.create`. A hand-rolled HTTP call would
be invisible to the very SDK being built. *What you must observe constrains how you may
integrate.*

**`EventSource` could not be used.** The browser's built-in SSE client is GET-only, and
the message body has to be POSTed — so streaming is `fetch()` + `ReadableStream`. That
turned out better than the default: `fetch` gives an `AbortController`, and aborting it
is simultaneously the cancel feature and the thing that produces the `status=cancelled`
log. One mechanism, two requirements.

---

## 7. Where things stand

| | Component | State |
|---|---|---|
| ✅ | Browser UI — streaming, cancel, list, resume | done |
| ✅ | chat-app — routes, repo, Postgres persistence | done |
| ✅ | Groq provider, mock default (no key needed) | done |
| ✅ | `conversations`, `messages` | done |
| ✅ | argus SDK — patching, contextvars, TTFT, redaction, emitter | done |
| 🟡 | ingestion | validates per row, holds in memory. No Redis yet |
| ⚪ | Redis Streams, worker, `inference_logs`, rollups | P3 / P4 |
| ⚪ | Agent + telemetry tools | P5 |
| ⚪ | Dashboard | P6 |
| ⚪ | OpenAI / Cerebras adapters | P7 |
| ⚪ | Kubernetes | P8 |

**The gap right now:** events stop at ingestion's in-memory buffer. P3 pushes them to
Redis, P4 drains them into `inference_logs`. Once that closes, the agent in P5 can answer
questions about its own telemetry and the demo loop completes on one screen.
