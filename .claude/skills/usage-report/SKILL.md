---
name: usage-report
description: Pull real usage numbers out of the running dashboard/Postgres — top models by cost and call volume, top conversations by token spend, latency percentiles, error/rate-limit breakdown — and render as ranked tables. Use when the user asks "what's using the most tokens", "top usage", "who's expensive", "usage report", "cost breakdown", "what's calling the most", or wants a monitoring snapshot beyond plain up/down health.
---

# Usage report

`health-check` answers "is it up." This answers "what is it doing, and what is it
costing." Pull real numbers — never invent or round-trip stale figures from a prior
conversation turn.

## Data sources

Prefer the dashboard API (already aggregates via the rollup table) over raw SQL; fall
back to SQL for anything the dashboard doesn't expose.

```bash
curl -s http://localhost:8002/api/overview    # calls, error_rate_pct, cost_usd, p50/p95/p99, calls_per_min
curl -s http://localhost:8002/api/models      # cost + volume broken down by model
curl -s http://localhost:8002/api/cost        # cost time series
curl -s http://localhost:8002/api/errors      # error breakdown
```

For anything not on a dashboard panel — top conversations, top time-of-day, per-provider
comparison — query Postgres directly:

```bash
kubectl exec -n argus postgres-0 -- psql -U argus -d argus -c "
  select provider, model, count(*) calls, sum(total_tokens) tokens,
         round(avg(latency_ms)) avg_ms, round(sum(total_tokens)*0.0000003, 4) approx_cost
  from inference_logs
  where started_at > now() - interval '1 hour'
  group by 1,2 order by tokens desc limit 10;
"

kubectl exec -n argus postgres-0 -- psql -U argus -d argus -c "
  select conversation_id, count(*) turns, sum(total_tokens) tokens
  from inference_logs group by 1 order by tokens desc limit 10;
"
```

(If port-forwarded instead of exec'd: `psql postgresql://argus:argus@localhost:5433/argus`.)

## Tables to produce

**Top models by volume/cost:**

| Rank | Provider | Model | Calls | Tokens | Avg latency | Cost (USD) |
|---|---|---|---|---|---|---|

**Top conversations by token spend:**

| Rank | Conversation ID | Turns | Tokens | Share of total |
|---|---|---|---|---|

**Error breakdown:**

| Status | Count | % of total | Likely cause |
|---|---|---|---|

Compute "% of total" and "share of total" yourself from the raw counts — don't ask the
API for a field it doesn't return.

## Reading the numbers

- **Cost concentrated in one model** with disproportionately few calls → check if it's
  the router calling the large model when the cheap one would've done (`ROUTER_MODEL`
  vs `ANSWER_MODEL` in `k8s/configmap.yaml`).
- **One conversation dominates token spend** → likely a long-running or looped
  conversation; worth checking `messages.seq` for that `conversation_id` for a runaway
  loop rather than genuine usage.
- **`rate_limited` climbing** as a share of errors → provider free-tier ceiling, not a
  pipeline bug — matches the `tools/loadgen.py` design note that mock exists precisely so
  load tests don't measure Groq's rate limiter instead of this system.
- **`calls_per_min` far above sustained capacity** implied by `p95_ms` × replica count →
  queue is absorbing it (that's the point of Redis Streams), check `stream.lag` via
  `health-check` to see if it's actually falling behind.

## Report format

Rank tables first, one line of interpretation under each — not a paragraph. Close with
whatever single number the user actually asked about, stated plainly.
