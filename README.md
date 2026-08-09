<h1 align="center">Argus</h1>

<p align="center">
  <strong>LLM Observability</strong> — every inference, watched.
</p>

<p align="center">
  <img alt="Python" src="https://img.shields.io/badge/python-3.12+-3776AB?logo=python&logoColor=white">
  <img alt="uv" src="https://img.shields.io/badge/uv-workspace-DE5FE9?logo=uv&logoColor=white">
  <img alt="FastAPI" src="https://img.shields.io/badge/FastAPI-009688?logo=fastapi&logoColor=white">
  <img alt="LangGraph" src="https://img.shields.io/badge/LangGraph-1C3C3C?logo=langchain&logoColor=white">
  <img alt="Postgres" src="https://img.shields.io/badge/Postgres-4169E1?logo=postgresql&logoColor=white">
  <img alt="Redis" src="https://img.shields.io/badge/Redis%20Streams-DC382D?logo=redis&logoColor=white">
  <img alt="Docker" src="https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=white">
</p>

---

Argus is an **auto-instrumenting SDK** and an **event-driven ingestion pipeline** for LLM
applications. Add one line at startup and every provider call in your process is
captured — model, latency, TTFT, tokens, cost, errors, conversation ID — then shipped
off-process without ever blocking the request path.

It ships with a chatbot that gives it something worth watching: an assistant that
answers questions about **its own inference telemetry**.

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
by code you don't own — inside LangGraph, inside a library, inside a background job.

<br>

## How it flows

```
   browser
      │  SSE, cancellable
      ▼
 ┌──────────┐        ┌────────────┐
 │ chat-app │───────►│  provider  │        LangGraph: route → tools → answer
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
services/chat_app/   chat UI + LangGraph agent
services/ingestion/  log receiver
services/worker/     stream consumer
services/dashboard/  metrics UI
db/                  schema + migrations
k8s/                 deployment manifests
```

The platform is domain-agnostic. The use case lives in exactly two files —
`services/chat_app/tools.py` and `prompts.py`. Swap them, get a different product.

<br>

## Quickstart

Requires [uv](https://docs.astral.sh/uv/) and Docker.

```bash
cp .env.example .env       # add GROQ_API_KEY
uv sync --all-packages     # resolve the workspace
make up                    # postgres, redis, services
```

| | |
|---|---|
| Chat | http://localhost:8000 |
| Dashboard | http://localhost:8002 |
| Ingestion | http://localhost:8001 |

<br>

## Roadmap

| | Phase | Status |
|---|---|---|
| **P0** | Repo skeleton · uv workspace · compose | 🟡 in progress |
| **P1** | Chat app — streaming, list / resume / cancel | ⚪ todo |
| **P2** | `argus` SDK — auto-instrumentation | ⚪ todo |
| **P3** | Ingestion — validate, redact, publish | ⚪ todo |
| **P4** | Worker — idempotent writes, rollups | ⚪ todo |
| **P5** | Agent — LangGraph route / tools / answer | ⚪ todo |
| **P6** | Dashboard — latency, throughput, errors, cost | ⚪ todo |
| **P7** | Multi-provider — OpenAI, Anthropic | ⚪ todo |
| **P8** | Kubernetes — self-hosted deploy | ⚪ todo |
| **P9** | Docs + demo | ⚪ todo |

Full design, decisions, and tradeoffs: [`plan/PLAN.md`](plan/PLAN.md).

<br>

## Stack

`Python 3.12+` · `FastAPI` · `LangGraph` · `Groq` · `Postgres` · `Redis Streams` · `uv` · `Docker` · `Kubernetes`

---

<p align="center"><sub>Built for the Ollive assignment.</sub></p>
