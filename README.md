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

**[Live demo →](https://argus-demo-blue.vercel.app)** — real numbers, screenshots,
and one Groq call traced end to end, from an actual run of this stack.

<br>

## Architecture

```python
import argus
argus.init(endpoint="http://ingestion:8001/v1/events", service="chat-app")
```

```python
# nothing below changes. it is logged anyway.
resp = client.chat.completions.create(model="llama-3.3-70b-versatile", messages=msgs)
```

Instrumentation patches the provider client class itself, so it also captures calls
made by code you don't own — inside a framework, a library, or a background job.

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

**Design notes:**

| | |
|---|---|
| **Auto-instrumentation** | Provider client classes are patched at import time — zero call-site changes, and third-party code is covered too. |
| **Non-blocking** | Fire-and-forget onto an in-process queue, drained by a background task. Batched, retried, spilled to disk on failure. |
| **Event-driven** | Ingestion publishes to Redis Streams; workers consume via a consumer group and scale horizontally. |
| **At-least-once, deduped at write** | Client-generated `event_id` + `ON CONFLICT DO NOTHING`. Cheaper and more honest than chasing exactly-once. |
| **OTel-shaped** | Wire fields follow OpenTelemetry GenAI conventions, so any OTel-emitting stack feeds the same pipeline. |
| **PII redaction** | Applied in the SDK *before* the event leaves the process, then again at the ingestion edge. |

**Layout:**

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

| Service | Port |
|---|---|
| Chat | 8000 |
| Ingestion | 8001 |
| Dashboard | 8002 |
| Postgres | 5432 |
| Redis | 6379 |

**Data model:**

```
conversations ──1:N──► messages
   status (soft delete)   seq         per-conversation ordinal, unique
   message_count          role        user | assistant | system | tool
   updated_at             truncated   set when a stream was cancelled
```

**Conversations are soft-deleted.** Inference logs reference them, and analytics should
survive a user clearing their sidebar. A partial index — `WHERE status <> 'deleted'` —
keeps the sidebar query off the dead rows.

**Messages are ordered by an explicit `seq`, not by timestamp.** Rows written in one
transaction share a `now()`, so timestamp ties are broken arbitrarily by the planner. A
unique constraint on `(conversation_id, seq)` makes gaps and duplicates impossible
rather than merely unlikely.

System design, scaling and failure assumptions: [`ARCHITECTURE.md`](ARCHITECTURE.md).
Data formats at every hop, API and table reference: [`docs/flow.md`](docs/flow.md).
Docker and Kubernetes, manifest by manifest: [`docs/devops.md`](docs/devops.md).
Full design, decisions, and tradeoffs: [`plan/PLAN.md`](plan/PLAN.md).

<br>

## Setup

### Docker Compose

Requires [uv](https://docs.astral.sh/uv/) and Docker.

```bash
cp .env.example .env       # no API key needed to start
uv sync --all-packages     # resolve the workspace
make up                    # postgres, redis, all four services
make migrate               # apply the schema
make health                # every service should answer ok
make seed                  # optional: synthetic traffic so the dashboard has data
```

Inside the compose network the services still use `postgres:5432` and `redis:6379`;
only the published host ports are remapped.

### Kubernetes (kind)

```bash
uv sync --all-packages
docker build -t argus:dev .                                     # one image, all four services
kind create cluster --name argus --config k8s/kind-config.yaml
kind load docker-image argus:dev --name argus
kubectl apply -k k8s/                                            # namespace → config → data layer → services → HPA → ingress
DATABASE_URL=postgresql+asyncpg://argus:argus@localhost:5433/argus uv run alembic upgrade head
```

Full manual runbook, including the one-time ingress controller and metrics-server setup:
[`k8s/README.md`](k8s/README.md). Live evidence from an actual run of the above —
pod status, screenshots, a real Groq call traced input → output → DB row — is in
[`output/`](output/README.md).

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

## Results

From a live run on the Kubernetes setup above — pods, from
`kubectl get pods,deploy,hpa -n argus`:

| Component | Replicas | Status | Notes |
|---|---|---|---|
| chat | 1/1 | Running | streams tokens over SSE, calls provider directly |
| ingestion | 1/1 | Running | validate → redact → price → `XADD` |
| worker | 2 (HPA: 2–6) | Running | consumer group, scales to 3 on manual trigger inside a `kind` node with 4 vCPU allotted |
| postgres | 1/1 (StatefulSet) | Running | |
| redis | 1/1 | Running | Streams + dead-letter list |
| dashboard | 1/1 | Running | reads the 1-minute rollup table |

End-to-end latency and cost, from `GET /api/overview` (60-minute rollup window, mixed
real + synthetic traffic):

| Metric | Value |
|---|---|
| Total calls | 6,002 |
| p50 / p95 / p99 latency | 288ms / 1,180ms / 1,765ms |
| p50 / p95 TTFT | 135ms / 685ms |
| Error rate | 9.36% (562 failures — includes a deliberate synthetic error/timeout/rate-limit tail) |
| Tokens processed | 3,713,383 |
| Cost | $1.027733 |
| Throughput | 100.03 calls/min sustained |

By model, from `GET /api/models`:

| Provider | Model | Calls | Avg latency | p95 latency | Failures |
|---|---|---|---|---|---|
| groq | llama-3.1-8b-instant | 2,651 | 199ms | 374ms | 243 |
| groq | llama-3.3-70b-versatile | 1,827 | 584ms | 1,111ms | 170 |
| groq | openai/gpt-oss-120b | 907 | 968ms | 1,798ms | 84 |
| mock | mock-1 | 617 | 133ms | 255ms | 65 |

One real (non-synthetic) Groq call traced end to end: 45 prompt tokens in, 38 tokens out,
442ms, landed as `inference_logs.id=6102` with `status=success` — full input/output pair,
screenshots, and raw JSON for every panel above: [`output/`](output/README.md).

<br>

## Using Claude to build and maintain this

This repo was built with Claude Code, and stays operable through project skills in
[`.claude/skills/`](.claude/skills/) rather than one-off manual checks:

| Skill | What it does |
|---|---|
| [`health-check`](.claude/skills/health-check/SKILL.md) | Sweeps pods, deployments, HPA, every service's `/health`, and Redis Stream lag/pending/dead-letters into one pass/fail table — the thing to run after any deploy or restart |
| [`usage-report`](.claude/skills/usage-report/SKILL.md) | Pulls real usage out of the dashboard + Postgres — top models by cost/volume, top conversations by token spend, error breakdown — ranked tables, not vibes |
| [`gen-tests`](.claude/skills/gen-tests/SKILL.md) | Reads the target code and an existing test file's conventions first, lists every branch (happy path + each error/boundary), writes one test per behavior, then actually runs them before reporting done |
| [`explain-changes`](.claude/skills/explain-changes/SKILL.md) | Turns every code change into an API inventory + edit-by-edit walkthrough — the repo doubles as interview prep, so unexplained code is unusable for that purpose |

Alongside the skills, [`.claude/agents/incident-responder.md`](.claude/agents/incident-responder.md)
is a dedicated agent, not a skill — it's called in once `health-check` or `usage-report`
has already surfaced a real symptom (crash loop, growing stream lag, dead letters, error
spike), and its job is root cause, not a re-statement of the symptom. It's read-only
(`Bash`/`Read`/`Grep`/`Glob`, no `Edit`/`Write`), reasons from the pipeline's actual
invariants (dedup on `event_id`, double-layer PII redaction, HPA-on-CPU-as-lag-proxy)
rather than generic troubleshooting, and reports `SYMPTOM` / `ROOT CAUSE` / `EVIDENCE` /
`FIX` / `BLAST RADIUS` instead of guessing.

`gen-tests` exists because generated tests are only as good as the branches someone
thought to ask for — the skill forces branch enumeration (dedup on `event_id`,
double-layer PII redaction, the non-blocking SDK guarantee, `seq` uniqueness under
concurrent writes) as a step the model can't skip, rather than trusting a single
"write tests for this file" prompt to surface them. Every generated test gets run,
not just written — a failing assertion means the code has a real bug or the test's
assumption is wrong, and the skill requires saying which before moving on.

Build history and phase-by-phase status live in `docs/roadmap.md` (gitignored — a
build log, not part of the deliverable).

<br>

## Stack

`Python 3.12+` · `FastAPI` · `SQLAlchemy` · `Alembic` · `Groq` · `Postgres`
· `Redis Streams` · `uv` · `Docker` · `Kubernetes`

Frontend is Jinja2, hand-written CSS and vanilla JS — no npm, no bundler, no CDN.
Streaming uses `fetch()` + `ReadableStream` rather than `EventSource`, because
`EventSource` is GET-only and the message body has to be POSTed. The `AbortController`
that stops a stream is also what produces the `status="cancelled"` inference log — the
cancel feature and the telemetry are one mechanism.

## Tradeoffs

| Decision | Cost | Why worth it |
|---|---|---|
| Dedup on write (`event_id`, no exact-once delivery) | A duplicate delivery can happen | Simpler than coordinating exact-once, and the end result is the same — no duplicate rows |
| Chat history trimmed, not summarized | Very old messages drop out of context | Summarizing costs an extra LLM call per turn and can misremember; trimming is simpler and fails in an obvious way |
| Dashboard reads pre-computed 1-minute buckets | Can't show sub-minute precision on wide time windows | Querying raw logs for every chart doesn't scale; a 15-minute rule falls back to raw data for anything recent |
| Worker autoscales on CPU, not queue backlog | CPU isn't a perfect stand-in for "falling behind" | Kubernetes gives CPU-based scaling for free; real backlog-based scaling needs extra infra this system doesn't need yet |
| Mock LLM provider by default | No real model output unless you add a key | Anyone can run the whole pipeline with zero setup, and load tests don't waste a real API's free quota |

## Future scope

Things left out on purpose, not missed:

- **A chat-based agent over the metrics** — ask "what's my p99 latency" in plain English
  instead of reading the dashboard. Skipped because the dashboard already answers this.
- **Sample successful calls instead of logging every one**, once traffic gets too big
  for one database to hold in full (still log 100% of errors).
- **Swap Redis Streams for Kafka** once logs need to be replayable for days, not hours.
- **Swap the rollup table for TimescaleDB or ClickHouse** once one Postgres table can't
  keep up with the dashboard's queries.
- **Scale workers on actual queue backlog**, not CPU, once that gap starts to matter.
- **Add real auth between services** — right now anything inside the cluster can write
  to ingestion; a real deployment needs to lock that down.

---

<p align="center"><sub>Built for the Ollive assignment.</sub></p>
