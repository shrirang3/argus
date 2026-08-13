# Architecture & data flow

Living document — describes what exists today, and marks what does not. Updated as each
phase lands. Every number in here was measured, not estimated.

**Current state:** the pipeline runs end to end — chat → SDK → ingestion → Redis →
worker → Postgres → dashboard. Groq and Cerebras are both wired (open-weight models
only), pinned per conversation. Kubernetes and the demo remain.

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
        │      ingestion  :8001         ├─────────►│  dead_letter_    │
        │                               │  invalid │  events          │
        │   validate each row           │          │  raw + reason    │
        │   redact again at the edge    │          └──────────────────┘
        │   price → cost_usd            │
        │   XADD pipelined, MAXLEN ~1M  │◄──── ⑦b  POST /v1/traces
        └───────────────┬───────────────┘            OTLP, any stack
                        │  ⑧  XADD  llm.inference.v1
                        ▼
        ┌───────────────────────────────┐
        │         REDIS STREAMS         │
        └───────────────┬───────────────┘
                        │      XREADGROUP · group ingest-workers
                        │
                        ▼
        ┌───────────────────────────────┐
        │            worker             │
        │     idempotent · rollups      │
        └───────────────┬───────────────┘
                        │  ⑨  INSERT ... ON CONFLICT (event_id, started_at)
                        │                          DO NOTHING
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

The dashboard reads `inference_logs` for exact percentiles over short windows and
`inference_metrics_1m` for wider ones — see §4 for why that split is a correctness
requirement rather than an optimisation.

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

### ④b chat-app → Cerebras (open-weight, alternate provider)

Same wire shape, via the official `openai` SDK pointed at `base_url=https://api.cerebras.ai/v1`
— no closed-weight provider is wired into this stack; Cerebras serves the same class of
model (Llama, Qwen) Groq does. One real divergence from Groq: usage is **not** attached
to the final chunk unless asked —

```json
{"model": "llama-3.3-70b", "messages": [...], "stream": true,
 "stream_options": {"include_usage": true}}
```

`stream_options` missing → `Usage` never fires → `cost_usd` stays `NULL` forever. This
bit us in the design, not in prod — worth stating as a known trap.

**Provider tagging problem:** Cerebras and OpenAI would share the exact same patched
SDK classes (`openai.resources.chat.completions.*`). The instrumentation resolves the
provider tag **per call**, from `self._client.base_url`, not from which class got
patched — one patch point, two identities, resolved late. See `instrument.py`,
`_resolve_openai_wire_provider`.

**Per-conversation pin:** `POST /api/conversations {"provider": "cerebras"}` locks a
conversation to one backend for its life. Left unset, the conversation adopts whatever
`DEFAULT_PROVIDER` answers on turn one (`repo.set_provider_if_unset`, a guarded
`UPDATE ... WHERE provider IS NULL` — race-safe by construction, not by convention).

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

### ⑦b OTLP → ingestion

`POST /v1/traces`, OTLP/HTTP JSON. Any stack emitting OpenTelemetry GenAI spans —
LangChain, LlamaIndex, Pipecat, OTel's own auto-instrumentation — lands in the same
tables as our SDK with no new code on either side.

```json
{"resourceSpans": [{
  "resource": {"attributes": [
    {"key": "service.name", "value": {"stringValue": "some-langchain-app"}}]},
  "scopeSpans": [{"spans": [{
    "startTimeUnixNano": "1786550000000000000",
    "endTimeUnixNano":   "1786550000900000000",
    "status": {"code": 1},
    "attributes": [
      {"key": "gen_ai.system",             "value": {"stringValue": "groq"}},
      {"key": "gen_ai.request.model",      "value": {"stringValue": "llama-3.3-70b-versatile"}},
      {"key": "gen_ai.usage.input_tokens", "value": {"intValue": "120"}},
      {"key": "gen_ai.usage.output_tokens","value": {"intValue": "60"}}
    ]}]}]}]}
```

Latency is derived from the span timestamps (900ms above). Spans without `gen_ai.*`
attributes are reported as `skipped` rather than guessed at — silent skipping would
leave a sender wondering why nothing appeared.

### ⑧ ingestion → Redis

Enriched by the ingestion service before publishing:

| Added | Value |
|---|---|
| `cost_usd` | `prompt_tokens × input_price + completion_tokens × output_price`, per million |
| `input_preview` / `output_preview` | redacted **again** at the edge |
| `redaction_hits` | merged with whatever the SDK already found |

```
XADD llm.inference.v1 MAXLEN ~ 1000000 * data '<enriched event json>'
```

Pipelined — fifty separate `XADD`s would be fifty round trips, making the collector's
latency scale with batch size for no reason. `MAXLEN ~` trims on node boundaries, which
is far cheaper than exact trimming and is why the cap is approximate by design.

**Cost is computed here, not in the SDK.** Pricing is a platform concern that changes
without any client changing; an SDK shipping a price list is wrong in production the day
a vendor updates it. An unpriced model yields `NULL`, never `0` — zero is
indistinguishable from a free call and would quietly understate spend.

### ⑨ worker → Postgres *(P4)*

```sql
INSERT INTO inference_logs (...) VALUES (...)
  ON CONFLICT (event_id, started_at) DO NOTHING;   -- absorbs redelivery
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
| ingestion | `POST` | `/v1/events` | `EventBatch` → `{accepted, rejected}` · **202** / 503 | ✅ |
| ingestion | `POST` | `/v1/traces` | OTLP JSON → `{accepted, rejected, skipped}` · **202** / 503 | ✅ |
| ingestion | `GET` | `/health` | `{status, service, redis}` · 200 / **503** | ✅ |
| ingestion | `GET` | `/v1/stats` | — → `{counts, stream:{length, groups}}` | ✅ |
| ingestion | `GET` | `/v1/stream/peek` | — → `{entries[]}`, read without consuming | ✅ diagnostics |
| ingestion | `GET` | `/v1/dead-letters` | — → `{rows[]}` | ✅ |
| dashboard | `GET` | `/health` | `{status, service}` | ✅ |
| dashboard | `GET` | `/api/metrics/*` | latency / throughput / errors / cost | ⚪ P6 |

Four status-code choices that are deliberate rather than incidental:

- **`POST /cancel` → 202, not 200.** It sets a flag the generator reads on its next
  token. 202 Accepted is honest about being asynchronous.
- **Malformed conversation id → 404, not 422.** A client cannot distinguish "wrong
  format" from "does not exist", and should not be able to.
- **`POST /v1/events` → 202, not 200.** Durability is the worker's job. "Accepted" is
  the honest word for "queued".
- **Redis unavailable → 503, not 500.** A false 202 would be data loss that looks like
  success. 500 says "I am broken"; 503 says "retry" — and the SDK does exactly that,
  buffering and then spilling to disk. `/health` returns 503 for the same reason: a
  health check that only proves the process is running is theatre.

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

### Built — migration `44eecf4a7405`

All three tables exist. `dead_letter_events` is written today by ingestion; the other two
are written by the worker in P4.

```
inference_logs                          -- PARTITION BY RANGE (started_at), monthly
  id            BIGINT IDENTITY
  event_id      UUID                    -- client-generated idempotency key
  conversation_id UUID                  -- soft reference, NO FK
  message_id · session_id · service
  provider · model · response_model · operation
  status · error_type · error_message · finish_reason
  latency_ms · ttft_ms · streamed
  prompt_tokens · completion_tokens · total_tokens
  cost_usd      NUMERIC(12,6)           -- NULL when the model is unpriced
  input_preview · output_preview        -- 500 chars, redacted twice
  redaction_hits JSONB · request_params JSONB · sdk_version
  started_at · ended_at · ingested_at

  PK (id, started_at)
  UQ uq_inference_logs_event (event_id, started_at)      -- see below
  CK status in (success, error, cancelled, timeout, rate_limited)

  IX ix_inference_logs_started      (started_at DESC)
  IX ix_inference_logs_conversation (conversation_id, started_at DESC)
  IX ix_inference_logs_model        (provider, model, started_at DESC)
  IX ix_inference_logs_errors       (status, started_at DESC) WHERE status <> 'success'

  partitions: 2026_07 … 2026_11  +  inference_logs_default
```

**The unique key is `(event_id, started_at)`, not `event_id` alone** — and that is not a
compromise, it is a requirement with a condition attached.

Postgres requires every unique constraint on a partitioned table to include the partition
key. Deduplication therefore only works if a redelivered event carries an *identical*
`started_at`. It does, because `started_at` is generated **client-side by the SDK** and
travels in the payload.

Had we stamped it on arrival instead, a retry would land in a different partition with a
different timestamp, the conflict would never be detected, and the idempotency guarantee
would have disappeared **silently** — no error, just duplicate rows. *A constraint you
cannot enforce is a constraint you do not have.*

The `DEFAULT` partition is the safety net: without it, an insert for a month with no
partition fails outright. Production would create partitions on a schedule; the default
means forgetting is a warning rather than an outage.

```
inference_metrics_1m                    -- rollup, written by the worker (P4)
  PK (bucket, provider, model, status)
  count · sum_latency_ms · sum_ttft_ms · ttft_count
  sum_tokens · sum_cost_usd · max_latency_ms
  IX (bucket DESC)
```

```
dead_letter_events                      -- written by ingestion today
  id BIGINT IDENTITY · raw JSONB · error TEXT · source TEXT · created_at
  IX (created_at DESC)
```

Raw payloads are kept **verbatim**. Storing a parsed or partially-coerced version would
destroy the evidence needed to work out what the sender actually sent.

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
| Redis down | Ingestion returns **503**, never a false 202. SDK buffers then spills. **Chat unaffected** ✅ measured |
| Malformed event | Quarantined in `dead_letter_events` with raw payload + reason. The rest of the batch still publishes ✅ measured |
| Dead-letter write fails | Logged, but the good rows already published are not failed |
| Unpriced model | `cost_usd = NULL`, never 0 — unknown spend is not free spend ✅ measured |
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
| ✅ | ingestion — per-row validation, edge redaction, pricing, Redis publish | done |
| ✅ | OTLP `/v1/traces` — foreign stacks feed the same pipeline | done |
| ✅ | `inference_logs` (partitioned), `inference_metrics_1m`, `dead_letter_events` | tables built |
| ✅ | worker — consumer group, dedupe, rollups | done |
| ✅ | dashboard — latency, throughput, errors, cost, pipeline health | done |
| ⚪ | Agent + telemetry tools | deferred — not asked for by the brief |
| ✅ | Cerebras adapter, open-weight only, per-conversation pin | done — P7 |
| ⚪ | Kubernetes | P8 |

**The gap right now:** events reach Redis and stop there. `XLEN` grows, `groups` is empty
— nothing is consuming the stream, so `inference_logs` stays empty even though the table
exists.

**P4 closes it:** a consumer group, `ON CONFLICT (event_id, started_at) DO NOTHING`,
`XACK` only after the commit, `XAUTOCLAIM` to recover a dead consumer's messages, and
rollup maintenance in the same transaction. Once that lands the loop is complete, and the
P5 agent can answer questions about the telemetry its own answers produced.
