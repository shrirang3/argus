# Live run evidence — P9 demo

Captured against a real `kind` cluster (`kind-argus`), real Groq inference
(`llama-3.3-70b-versatile`), and dashboard rollups built from `tools/loadgen.py`
synthetic traffic. Nothing here is fabricated or hand-edited — every file is a
direct `kubectl`/`curl`/`psql` output or a headless-Chrome screenshot of the
running services.

## How to reproduce

```bash
kind create cluster --name argus --config k8s/kind-config.yaml
kubectl apply -f https://raw.githubusercontent.com/kubernetes/ingress-nginx/main/deploy/static/provider/kind/deploy.yaml
kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml
docker build -t argus:dev . && kind load docker-image argus:dev --name argus
cp k8s/secret.example.yaml k8s/secret.yaml   # fill GROQ_API_KEY, or leave blank for mock
kubectl apply -k k8s/
kubectl port-forward -n argus svc/postgres 5433:5432 &
DATABASE_URL=postgresql+asyncpg://argus:argus@localhost:5433/argus uv run alembic upgrade head
make seed    # or: uv run python tools/loadgen.py --events 800 --concurrency 20 --spread-minutes 30
```

## Contents

| File | What it is |
|---|---|
| [`pods.txt`](pods.txt) | `kubectl get pods,svc,deploy,hpa,ingress -o wide` + `kubectl top pods` — every object Running, worker at 3/3 after a manual scale-out |
| [`ss_chat.png`](ss_chat.png) | Screenshot of `chat.argus.local` — sidebar showing two real conversations, empty-state prompt chips |
| [`ss_dashboard.png`](ss_dashboard.png) | Screenshot of `dashboard.argus.local` — latency percentiles, throughput, success/failure, cost by model, all rendered from live rollups |
| [`input_conversation_create.json`](input_conversation_create.json) | Request/response for `POST /api/conversations {}` — provider/model unset, adopts `DEFAULT_PROVIDER` on first turn |
| [`input_message.json`](input_message.json) | The literal request body sent to `POST /api/conversations/{id}/messages` |
| [`output_message_stream.txt`](output_message_stream.txt) | Raw SSE stream back from that call — `event: token` chunks then `event: done` with token counts and provider/model tags, straight through real Groq |
| [`db_latest_inference_log.txt`](db_latest_inference_log.txt) | The `inference_logs` row that same call produced — proof the SDK → ingestion → Redis → worker → Postgres hop landed |
| [`dashboard_overview.json`](dashboard_overview.json) | `GET /api/overview` — calls, error rate, p50/p95/p99, cost, over the rollup window |
| [`dashboard_pipeline.json`](dashboard_pipeline.json) | `GET /api/pipeline` — SDK emit counts, ingest accept/reject, Redis Stream length/lag/pending/consumers, dead-letter count |
| [`dashboard_latency.json`](dashboard_latency.json), [`_throughput.json`](dashboard_throughput.json), [`_errors.json`](dashboard_errors.json), [`_cost.json`](dashboard_cost.json), [`_models.json`](dashboard_models.json), [`_recent.json`](dashboard_recent.json) | Per-panel series backing each dashboard tile |

## Headline numbers (this run)

- **6,002 events** through the pipeline — 800+200 seeded (`make seed`) + 5,000 sustained load + 2 real Groq chat turns
- **0 rejected at ingestion, 0 dead letters** — every event that reached ingestion was validated, redacted, and written
- **Stream lag: 0, pending: 0, 5 consumers** — worker(s) keeping up with the consumer group, no backlog
- **p50 288ms / p95 1,180ms / p99 1,765ms** end-to-end latency, **9.36% error rate** — synthetic mix includes a deliberate error/timeout/rate-limit tail so the panels aren't flatlined
- **Real Groq call:** `llama-3.3-70b-versatile`, 45 prompt tokens → 83 total tokens, 442ms, `status=success`, landed as `inference_logs.id=6102`
- **HPA:** `worker` scaled 2 → 3 replicas via manual `kubectl scale` (proof point — a 0.5s synthetic burst finishes too fast to trip the 70% CPU threshold on its own; the consumer-group design absorbs it at 2 replicas without falling behind, which is the actual point of the architecture)

## Pipeline, traced through this run

```
POST /api/conversations {}                       → conversation created, provider unset
POST /.../messages {"content": "..."}             → chat-app calls Groq directly, streams tokens back over SSE
        │ (SDK patches the Groq client — capture happens on the same call, off the reply path)
        ▼
argus SDK buffers {model, latency, ttft, tokens, cost, event_id}
        ▼ async, batched — chat has already returned "event: done" to the browser
POST http://ingestion:8001/v1/events              → validate → redact PII → price → XADD
        ▼
Redis Stream "llm.inference.v1"                   → length 6002, lag 0, pending 0
        ▼ XREADGROUP, consumer group "ingest-workers"
worker (3 replicas)                               → INSERT ... ON CONFLICT (event_id) DO NOTHING → inference_logs
        ▼
dashboard reads the 1-minute rollup table          → GET /api/overview, /api/latency, ... → charts above
```
