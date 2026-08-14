# Architecture

The system in one pass: what moves where, how logging stays out of the request path,
how it scales, and what happens when each piece fails. For wire-level formats at every
hop, see [`docs/flow.md`](docs/flow.md). For the container/Kubernetes layer, see
[`docs/devops.md`](docs/devops.md). For why each decision was made over the alternative,
see [`plan/PLAN.md`](plan/PLAN.md).

---

## 1. System overview

```
Browser (Jinja2 + vanilla JS, SSE + AbortController)
   │
   ▼
chat-app (FastAPI)
   │  writes conversations / messages ──────────────► Postgres
   │  LLM calls ──► Groq / Cerebras (open-weight models only)
   │                     ▲
   │                     │ intercepted, transparently, by
   │              argus SDK (monkey-patch at import time)
   │                     │  async buffered emit — NEVER blocks the chat path
   ▼                     ▼
              ingestion (FastAPI)
              /v1/events  (native JSON batch)
              /v1/traces  (OTLP)
              → Pydantic validate → PII redact → price → XADD
                        │
                        ▼
              Redis Streams  `llm.inference.v1`  (+ DLQ stream)
                        │  consumer group `ingest-workers`
                        ▼
              worker — idempotent upsert + 1-min rollups
                        │
                        ▼
                    Postgres ──► dashboard (FastAPI + Chart.js)
```

Two independent write paths land in the same Postgres: `chat-app` writes
`conversations`/`messages` directly (the application's own state), while every LLM call
also produces an `inference_logs` row through the SDK → ingestion → Redis → worker path
(the observability data). They share a database, not a code path — a bug in one cannot
corrupt the other's writes.

---

## 2. Ingestion flow

1. **Capture.** `argus.instrument` patches the provider SDK's `Completions.create` (sync
   and async) at import time. A patched method times the call, extracts token
   usage/errors from the response, and builds an `InferenceEvent` — all without the
   application changing a single call site. This is also what captures calls made by
   code the app doesn't own (LangGraph internals, a background job).
2. **Emit.** The event goes onto an in-process `asyncio.Queue`, drained by a background
   task in batches of ≤50 or every 500ms, POSTed to `/v1/events` with retries. The
   request that triggered the LLM call never awaits this — it returns as soon as the
   provider response is ready.
3. **Validate + redact + price.** Ingestion validates each row independently with
   Pydantic — one malformed event goes to `dead_letter_events`, the rest of the batch
   proceeds. PII redaction runs again here (defense in depth; the SDK already redacted
   before the event left the process). Cost is computed from a static per-model price
   table; an unpriced model yields `cost_usd = NULL`, never a silently wrong `0`.
4. **Publish.** Valid rows are `XADD`ed to the `llm.inference.v1` Redis Stream. Ingestion
   returns `202` immediately — nothing here waits on a database write.
5. **Consume + persist.** The worker, in a consumer group (`ingest-workers`), reads
   batches, inserts with `ON CONFLICT (event_id) DO NOTHING`, updates the 1-minute
   rollup table, and only then `XACK`s. Multiple worker replicas share the stream via
   the consumer group; `XAUTOCLAIM` reassigns a crashed replica's unacked messages.
6. **Serve.** The dashboard reads rollups for time series (fast at scale) and raw
   `inference_logs` for the recent-inferences table and any window under 15 minutes
   (rollups are 1-minute buckets — a sub-15-minute query against them returns
   near-empty; raw rows are what makes "latency in the last minute?" answerable).

## 3. Logging & instrumentation strategy

**Auto-instrumentation over an explicit wrapper.** Patching the provider client class
(not wrapping a client instance the app constructs) is what captures calls the
application never sees — a library or framework building its own client still gets
logged, because the class itself carries the patch.

**Non-blocking by construction, not by convention.** The emit call is fire-and-forget
onto a bounded queue. If ingestion is unreachable, the emitter retries, then buffers in
memory, then spills to `/var/log/argus/spill.jsonl` for replay — the chat request path
never sees any of that latency. A full queue drops the oldest event and increments a
counter that's visible on the dashboard: visible loss beats silent loss.

**Wire format vs storage format are deliberately different.** Events travel as OTel
GenAI-shaped attributes (`gen_ai.system`, `gen_ai.usage.input_tokens`, …) so any
OTel-emitting stack can feed the same ingestion endpoint — proven by the fact that
`/v1/traces` accepts raw OTLP spans, not just this SDK's native format. Postgres stores
them as flat typed columns instead of a JSONB attribute bag, because the dashboard's
queries need to filter and aggregate by provider/model/status at scale, and an
attribute bag would forfeit every index that makes that fast.

**Redaction runs twice, deliberately.** Once in the SDK, before the event leaves the
process — so a sensitive prompt is never on the wire in the first place. Once more at
the ingestion edge, in case a future emitter skips the SDK's own redaction. Redundant by
design, not by oversight.

## 4. Scaling

| Layer | How it scales | Cost of scaling it |
|---|---|---|
| Ingestion | Stateless FastAPI — add replicas behind a Service/LB | None; every replica does the same validate→redact→XADD work independently |
| Redis Streams | Absorbs bursts between ingestion and the worker | Chat latency is decoupled from DB write latency — a slow worker never slows a chat reply |
| Worker | Horizontal, via the consumer group — `HPA` in `k8s/hpa.yaml` scales on CPU as a proxy for stream lag | `XAUTOCLAIM` recovers a crashed replica's in-flight messages; `ON CONFLICT DO NOTHING` absorbs any resulting redelivery as a no-op |
| `inference_logs` | Partitioned monthly on `started_at` | Retention past 90 days is a partition drop, not a `DELETE` — no vacuum pressure |
| Dashboard reads | Time series read the 1-minute rollup table, never raw rows | Rollup staleness is bounded to ~1 minute, mitigated by the <15-minute freshness rule falling back to raw |

**Past this project's scale**, the next moves (documented, not built): sample successful
calls at 10% while keeping 100% of errors; Redis Streams → Kafka once replay needs to
span days instead of hours; rollups → TimescaleDB continuous aggregates or ClickHouse
once a single Postgres rollup table stops being enough.

## 5. Failure assumptions

| Failure | Behavior |
|---|---|
| Provider 5xx / timeout | Error surfaced to the user; `inference_logs` row written with `status="error"` + classified `error_type` |
| Ingestion unreachable | SDK retries → buffers in memory → spills to disk. Chat is unaffected — it never awaited ingestion |
| Redis down | Ingestion returns 503; the SDK's buffer/spill path absorbs it. Nothing written, nothing lost from the spill |
| Postgres down | Worker withholds `XACK` on failed writes; messages redeliver once Postgres recovers |
| Worker crash mid-batch | `XAUTOCLAIM` reassigns its unacked messages to a live replica; `ON CONFLICT DO NOTHING` makes the redelivery a no-op, not a duplicate |
| SDK queue saturated | Oldest event dropped, a counter increments, visible on the dashboard rather than silently disappearing |
| User cancels a stream | The streaming proxy's `finally` block emits `status="cancelled"` with whatever partial tokens were produced before the abort |
| A tool call raises | Caught, returned to the model as a tool message, logged — never surfaces to the user as a 500 |

Delivery is **at-least-once, deduplicated at write** via a client-generated `event_id`
and a unique constraint — cheaper and more honest than engineering exactly-once across
an HTTP hop, a queue, and a database write.

## 6. Deployment

Two supported paths, same image, same manifests-as-config philosophy:

- **`docker compose up`** — one Docker daemon, one machine. Internal DNS resolves
  service names, named volumes survive a container recreate, `depends_on:
  condition: service_healthy` orders startup. No self-healing beyond `restart:
  unless-stopped`, no autoscaling.
- **`kubectl apply -k k8s/`** — a Deployment per stateless service, a StatefulSet for
  Postgres (stable identity + volume across restarts), a CPU-based HPA on the worker,
  host-routed Ingress. Verified live on a local `kind` cluster: all 7 pods `Running`,
  migrations applied through a port-forwarded Postgres, all three Ingress hostnames
  answering `/health`, HPA reading real CPU metrics. Full walkthrough and manual runbook
  in [`docs/devops.md`](docs/devops.md) and [`k8s/README.md`](k8s/README.md).
