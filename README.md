<h1 align="center">Argus</h1>

<p align="center">
  <strong>LLM Observability</strong> — every inference, watched.
</p>

<p align="center">
  <img alt="Python" src="https://img.shields.io/badge/python-3.12+-3776AB?logo=python&logoColor=white">
  <img alt="uv" src="https://img.shields.io/badge/uv-workspace-DE5FE9?logo=uv&logoColor=white">
  <img alt="FastAPI" src="https://img.shields.io/badge/FastAPI-009688?logo=fastapi&logoColor=white">
  <img alt="Postgres" src="https://img.shields.io/badge/Postgres-4169E1?logo=postgresql&logoColor=white">
  <img alt="Redis" src="https://img.shields.io/badge/Redis%20Streams-DC382D?logo=redis&logoColor=white">
  <img alt="Docker" src="https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=white">
  <img alt="Kubernetes" src="https://img.shields.io/badge/Kubernetes-326CE5?logo=kubernetes&logoColor=white">
</p>

---

Argus is an **auto-instrumenting SDK** and an **event-driven ingestion pipeline** for LLM
applications. Add one line at startup and every provider call in your process is
captured — model, latency, TTFT, tokens, cost, errors, conversation ID — then shipped
off-process without ever blocking the request path.

It ships with a chatbot that gives it something worth watching: an assistant that
answers questions about **its own inference telemetry**.

> **Status — the pipeline is complete end to end.** A chat message is captured by the
> SDK, validated and priced at the edge, queued through Redis, written to Postgres by a
> worker, and charted on the dashboard — with nothing in the request path waiting on any
> of it. Groq and Cerebras are both wired — open-weight models only, pinned per
> conversation. Runs on Kubernetes too — namespace, Deployments, a StatefulSet
> for Postgres, host-routed Ingress, HPA on the worker — verified live on a
> local `kind` cluster, with real Groq inference end to end and the dashboard populated
> by synthetic load. Nothing remains.

<br>

## One line

```python
import argus
argus.init(endpoint="http://ingestion:8001/v1/events", service="chat-app")
```

```python
# nothing below changes. it is logged anyway.
resp = client.chat.completions.create(model="llama-3.3-70b-versatile", messages=msgs)
```

Because instrumentation patches the provider client itself, it also captures calls made
by code you don't own — inside a framework, a library, or a background job.

<br>

## How it flows

```
   browser
      │  SSE, cancellable
      ▼
 ┌──────────┐        ┌────────────┐
 │ chat-app │───────►│  provider  │
 └────┬─────┘        └─────▲──────┘
      │                    │  intercepted, transparently
      │              ┌─────┴──────┐
      │              │ argus SDK  │  async buffered emit — never blocks
      │              └─────┬──────┘
      │                    ▼
      │            ┌───────────────┐     ┌────────────────┐     ┌────────┐
      │            │   ingestion   │────►│ Redis  Streams │────►│ worker │
      │            │ validate      │     │ + dead letter  │     └───┬────┘
      │            │ redact PII    │     └────────────────┘         │
      │            └───────────────┘                                │
      ▼                                                             ▼
 ┌──────────────────────────── Postgres ────────────────────────────┐
 └────────────────────────────────┬─────────────────────────────────┘
                                  ▼
                            ┌───────────┐
                            │ dashboard │  p50 / p95 / p99 · throughput · errors · cost
                            └───────────┘
```

**The invariant:** the chat path never awaits the logging path. Ingestion down? Events
retry, then buffer, then spill to disk. Chat keeps serving.

<br>

## Design notes

| | |
|---|---|
| **Auto-instrumentation** | Provider client classes are patched at import time — zero call-site changes, and third-party code is covered too. |
| **Non-blocking** | Fire-and-forget onto an in-process queue, drained by a background task. Batched, retried, spilled to disk on failure. |
| **Event-driven** | Ingestion publishes to Redis Streams; workers consume via a consumer group and scale horizontally. |
| **At-least-once, deduped at write** | Client-generated `event_id` + `ON CONFLICT DO NOTHING`. Cheaper and more honest than chasing exactly-once. |
| **OTel-shaped** | Wire fields follow OpenTelemetry GenAI conventions, so any OTel-emitting stack feeds the same pipeline. |
| **PII redaction** | Applied in the SDK *before* the event leaves the process, then again at the ingestion edge. |

<br>

## Layout

```
packages/argus/      the SDK — app-agnostic, installable
services/chat_app/   chat UI, provider adapters
services/ingestion/  log receiver
services/worker/     stream consumer
services/dashboard/  metrics UI
tools/               synthetic load generator
db/                  schema + migrations
k8s/                 deployment manifests
```

The platform is domain-agnostic — the SDK, ingestion, worker and schema know nothing
about chat. `services/chat_app/` is the only part that does.

<br>

## Data model

```
conversations ──1:N──► messages
   status (soft delete)   seq         per-conversation ordinal, unique
   message_count          role        user | assistant | system | tool
   updated_at             truncated   set when a stream was cancelled
```

Two decisions worth knowing before reading the schema:

**Conversations are soft-deleted.** Inference logs reference them, and analytics should
survive a user clearing their sidebar. A partial index — `WHERE status <> 'deleted'` —
keeps the sidebar query off the dead rows.

**Messages are ordered by an explicit `seq`, not by timestamp.** Rows written in one
transaction share a `now()`, so timestamp ties are broken arbitrarily by the planner. A
unique constraint on `(conversation_id, seq)` makes gaps and duplicates impossible
rather than merely unlikely.

<br>

## Quickstart

Requires [uv](https://docs.astral.sh/uv/) and Docker.

```bash
cp .env.example .env       # no API key needed to start
uv sync --all-packages     # resolve the workspace
make up                    # postgres, redis, all four services
make migrate               # apply the schema
make health                # every service should answer ok
make seed                  # optional: synthetic traffic so the dashboard has data
```

Open **http://localhost:8000** and start a conversation.

| Service | URL |
|---|---|
| Chat | http://localhost:8000 |
| Ingestion | http://localhost:8001 |
| Dashboard | http://localhost:8002 |
| Postgres | `localhost:5433` — 5433, not 5432, to avoid colliding with a local install |
| Redis | `localhost:6380` |

Inside the compose network the services still use `postgres:5432` and `redis:6379`;
only the published host ports are remapped.

Runs on Kubernetes too — full manual runbook in [`k8s/README.md`](k8s/README.md)
(`kind create cluster` → build → load → `kubectl apply -k k8s/`).

### Providers

The stack ships with a **mock provider as the default**, so it runs with no credentials
at all — you get real streaming, real persistence, real cancellation, against canned
tokens. For real inference, add a free [Groq](https://console.groq.com) key:

```bash
GROQ_API_KEY=gsk_...
DEFAULT_PROVIDER=groq
ANSWER_MODEL=llama-3.3-70b-versatile
```

`make up` again and the same interface returns real tokens. Nothing downstream changes —
every provider is normalised into one `Usage` record at the adapter boundary.

**Open-weight models only.** No closed-weight provider is wired in. A second backend,
[Cerebras](https://cloud.cerebras.ai), serves the same kind of model (Llama, Qwen) over an
OpenAI-wire-compatible endpoint:

```bash
CEREBRAS_API_KEY=csk-...
```

Each conversation can be pinned to a provider/model at creation
(`POST /api/conversations {"provider": "cerebras", "model": "llama-3.3-70b"}`); left
unset, it adopts whatever `DEFAULT_PROVIDER` answers on the first turn and stays there for
the rest of its life. Cerebras is reached through the `openai` SDK pointed at a different
`base_url`, so the SDK's existing `openai.*` instrumentation patch covers it — the request
handler that resolves the provider tag reads the client's `base_url` at call time rather
than trusting which class was patched, so mixed traffic on one process still lands as
`groq` or `cerebras`, never a blanket `openai`.

Mock is not only a convenience. Load tests run against it too: pointed at a free-tier
provider, a 50-concurrent test measures that provider's rate limiter rather than this
pipeline.

<br>

## Roadmap

| | Phase | Status |
|---|---|---|
| **P0** | Repo skeleton · uv workspace · compose | 🟢 done |
| **P1** | Chat app — streaming, persistence, list / resume / cancel | 🟢 done |
| **P2** | `argus` SDK — auto-instrumentation | 🟢 done |
| **P3** | Ingestion — validate, redact, price, publish | 🟢 done |
| **P4** | Worker — consumer group, dedupe, rollups | 🟢 done |
| **P5** | Agent — telemetry tools over the same data | ⚪ deferred, not required by the brief |
| **P6** | Dashboard — latency, throughput, errors, cost | 🟢 done |
| **P7** | Multi-provider — Groq + Cerebras, open-weight models only, per-conversation pin | 🟢 done |
| **P8** | Kubernetes — self-hosted deploy | 🟢 done |
| **P9** | Docs + demo | 🟢 done |

System design, ingestion flow, scaling and failure assumptions:
[`ARCHITECTURE.md`](ARCHITECTURE.md).
Data formats at every hop, API and table reference:
[`docs/flow.md`](docs/flow.md).
Docker and Kubernetes — images, objects, why each manifest is shaped the way
it is: [`docs/devops.md`](docs/devops.md).
Full design, decisions, and tradeoffs: [`plan/PLAN.md`](plan/PLAN.md).

<br>

## Stack

`Python 3.12+` · `FastAPI` · `SQLAlchemy` · `Alembic` · `Groq` · `Postgres`
· `Redis Streams` · `uv` · `Docker` · `Kubernetes`

Frontend is Jinja2, hand-written CSS and vanilla JS — no npm, no bundler, no CDN.
Streaming uses `fetch()` + `ReadableStream` rather than `EventSource`, because
`EventSource` is GET-only and the message body has to be POSTed. The `AbortController`
that stops a stream is also what produces the `status="cancelled"` inference log — the
cancel feature and the telemetry are one mechanism.

---

<p align="center"><sub>Built for the Ollive assignment.</sub></p>
