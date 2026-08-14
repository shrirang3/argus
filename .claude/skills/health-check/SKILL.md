---
name: health-check
description: Full health sweep of the running argus stack — Kubernetes pod/deployment/HPA status, each service's /health endpoint, Redis Stream lag, and Postgres reachability, rolled into one pass/fail table. Use when the user asks "is everything healthy", "check the cluster", "check pods", "is the pipeline up", "health check", or after any deploy/restart to confirm nothing broke.
---

# Health check

One pass produces one table. Don't narrate each command — run the sweep, then report.

## Sweep

Run these, in order, tolerating failures (a dead component is a finding, not a
blocker to the rest of the sweep):

```bash
kubectl get pods -n argus -o wide
kubectl get deploy,hpa -n argus
kubectl top pods -n argus 2>&1   # may fail if metrics-server isn't ready — not fatal

curl -sf -H "Host: chat.argus.local" http://localhost/health
curl -sf -H "Host: ingestion.argus.local" http://localhost/health
curl -sf -H "Host: dashboard.argus.local" http://localhost/health

curl -sf http://localhost:8002/api/pipeline   # SDK emit counts, ingest accept/reject,
                                                # Redis stream length/lag/pending/consumers,
                                                # dead-letter count — port-forward dashboard
                                                # svc 8002 first if ingress isn't reachable
```

If ingress hostnames aren't resolvable (no `/etc/hosts` entry), fall back to
`kubectl port-forward -n argus svc/<name> <port>:<port>` per service and hit
`localhost:<port>/health` directly.

## What each signal means

| Signal | Healthy | Unhealthy — likely cause |
|---|---|---|
| Pod `STATUS` | `Running`, `READY` = N/N | `CrashLoopBackOff` on worker at cluster start = normal (Redis/Postgres not ready yet, self-heals); persisting past 60s = real |
| `kubectl top pods` | Populates | Empty/error = metrics-server not patched with `--kubelet-insecure-tls` on kind |
| `/health` on each service | `{"status":"ok", ...}` with the dependency flag (`redis: true`, `postgres: true`) | `redis: false` / `postgres: false` = service is up but its dependency isn't — check that pod separately |
| `stream.lag` | `0` or small and shrinking | Growing = worker replicas can't keep up with ingestion rate — check HPA target vs current CPU, consider `kubectl scale deployment worker --replicas=N` |
| `stream.pending` | `0` | Nonzero and static = a consumer claimed messages and died without acking — `XAUTOCLAIM` should reassign them; if stuck, a worker is wedged |
| `dead_letters.total` | `0` | Nonzero = events failed validation/write repeatedly — read `dead_letters.rows` for the actual payload and error |
| HPA `TARGETS` vs `REPLICAS` | `REPLICAS` trends toward what `TARGETS` implies | Stuck at `MINPODS` under real load = metrics-server not feeding it; stuck at `MAXPODS` = genuinely under-provisioned |

## Report format

```
| Component  | Status | Detail                          |
|------------|--------|----------------------------------|
| chat       | OK     | 1/1 running, /health ok          |
| ingestion  | OK     | 1/1 running, redis reachable     |
| worker     | OK     | 3/3 running, stream lag 0        |
| postgres   | OK     | 1/1 running                      |
| redis      | OK     | 1/1 running                      |
| dashboard  | OK     | 1/1 running, postgres reachable  |
| HPA        | OK     | 3/6 replicas, cpu 4%/70%         |
```

One line per component, worst-first if anything failed. End with a one-line verdict
("stack healthy, 0 dead letters, 0 stream lag") — no filler around it.
