# Docker & Kubernetes — how this repo ships

This is the deploy-side counterpart to [`flow.md`](flow.md) (which covers the
application data flow). It explains the container and orchestration layer:
what an image actually is, why Compose stops being enough, and what every
object in `k8s/` is doing there.

---

## 1. Docker

**Image vs container.** An image is a read-only template — filesystem plus
metadata (entrypoint, exposed ports). A container is a running instance of
one. `argus:dev` is one image; the `chat`, `ingestion`, `dashboard`, and
`worker` Pods are four containers built from that same image — only the
`command:` they're started with differs.

**The Dockerfile is layered, and layer order is cache order:**

```dockerfile
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --all-packages --no-install-workspace   # dependency layer
COPY . .
RUN uv sync --frozen --all-packages                           # source layer
```

Each instruction is a cached, content-hashed diff on the previous layer.
Dependencies change rarely; source changes on every edit. Installing deps
*before* copying source means editing one `.py` file never invalidates the
expensive dependency-install layer — only the fast final `uv sync` re-runs.
Reverse the order and every build re-resolves the whole workspace.

**Compose orchestrates one Docker daemon on one machine.** It gives you:
internal DNS (service name → container, `chat` reaches `postgres` by name,
no manual IP wiring), ordered startup (`depends_on: condition: service_healthy`),
and named volumes that outlive a container being recreated (`pgdata`, without
which `docker compose down` would erase the database).

Its ceiling is exactly what it doesn't have: no "if this machine dies,
reschedule elsewhere," no autoscaling, no rolling updates without a hand-rolled
script. That ceiling is what Kubernetes exists to remove.

---

## 2. Kubernetes — the core idea

You don't tell Kubernetes to start a container. You declare the state you
want, and a control loop continuously reconciles actual state toward it. Kill
a Pod by hand — the Deployment notices "3 should exist," sees 2, starts a
replacement. You never invoke that loop; it never stops running. Compose
executes commands. Kubernetes maintains assertions.

## 3. The object ladder

| Object | Role | Why it exists over the layer below |
|---|---|---|
| **Pod** | smallest deployable unit — one or more containers sharing a network namespace | no self-healing on its own; always owned by something above it |
| **Deployment** | keeps N identical Pods running, handles rolling updates | Pods are disposable "cattle" — any one is replaceable by an identical fresh one |
| **StatefulSet** | like a Deployment, but each Pod gets a stable name and its own PVC that follows it across restarts | for workloads where identity matters — Postgres restarting must reattach to *its* data, not an empty volume |
| **Service** | stable name/IP load-balancing across whatever Pods currently match a label | Pod IPs churn constantly; code should never depend on one |
| **ConfigMap / Secret** | externalized config injected as env vars | split because Secrets get different RBAC/encryption treatment — nothing you'd wince at in `git log` goes in a ConfigMap |
| **PersistentVolumeClaim** | a request for durable storage, independent of which Pod/node uses it | disk that survives the Pod being deleted, unlike the Pod's own filesystem |
| **Namespace** | logical partition inside one cluster | names only need to be unique within it, not cluster-wide |
| **Ingress + Ingress Controller** | routing rule (Ingress) + the Pod that actually executes it (Controller, `ingress-nginx` here) | rule and engine are split so the engine is swappable without touching the rule |
| **HorizontalPodAutoscaler** | watches a metric against a Deployment, adjusts `replicas` | needs `metrics-server` running to have any numbers to read — not built into base k8s |

## 4. Ideas worth naming explicitly

**Readiness vs. liveness.** Liveness asks "should this be killed and
restarted?" Readiness asks "should this receive traffic right now?" A Pod can
be alive but not ready — still starting, or a dependency isn't up yet. The
Service only routes to Pods passing readiness; liveness failures trigger a
restart. One probe used for both either kills slow-starting apps needlessly
or routes traffic into a Pod that isn't ready for it.

**`imagePullPolicy: Never`.** Kubelet's default is "fetch this from a
registry." On `kind`, the image only exists because it was manually loaded
into the node (`kind load docker-image`) — without this flag, kubelet still
tries a pull and 404s against a public `argus:dev` that doesn't exist. Gone
entirely once a real registry is in play.

**Request vs. limit.** A request is what the scheduler reserves when placing
a Pod — a floor. A limit is a runtime ceiling — exceed CPU and get throttled,
exceed memory and get OOM-killed. The HPA's `averageUtilization: 70` is
literally `usage / request` — no request set, no denominator, no HPA math.
That's why `worker.yaml` sets one.

**Headless Service (`clusterIP: None`).** A normal Service hides individual
Pod identity behind one virtual, load-balanced IP. A headless one skips the
virtual IP and returns the real Pod IP straight from DNS. Postgres uses one —
"load balance across Postgres replicas" is meaningless with a single primary;
other services need to resolve directly to that one Pod.

## 5. The manifest format itself

Every object, regardless of kind, follows one shape:

```yaml
apiVersion: apps/v1     # which API group/version owns this object's schema
kind: Deployment        # the object type
metadata:
  name: chat             # unique within its namespace + kind
  namespace: argus
spec:                    # desired state — what you assert
status:                  # actual state — written back by the cluster, never by you
```

`spec` vs `status` *is* the declarative model made literal: you write `spec`,
the cluster writes `status` onto the same object, and reconciliation is the
loop closing the gap between them — `kubectl get pod x -o yaml` shows a
`status` block nobody hand-wrote.

`apiVersion` matters beyond boilerplate: `v1` is the oldest, core group
(Pod, Service, ConfigMap, Namespace, PVC); `apps/v1` covers anything managing
Pods (Deployment, StatefulSet); `networking.k8s.io/v1` is Ingress;
`autoscaling/v2` is HPA. Each group versions independently — get it wrong and
the API server rejects the object outright, no silent no-op.

**Kustomize** (`k8s/kustomization.yaml`) is not a Kubernetes object — it's a
`kubectl` feature that applies a list of plain files as one unit. No
templating, no generated values: what's in the file is exactly what gets
applied. That's the whole difference from Helm.

## 6. This repo's manifests

| File | Object(s) | Role |
|---|---|---|
| `namespace.yaml` | Namespace | isolation boundary for everything below |
| `configmap.yaml` | ConfigMap | non-secret env, shared by all four app Deployments |
| `secret.example.yaml` | Secret (template) | API keys / DB creds — copy to `secret.yaml`, never commit that copy |
| `postgres.yaml` | StatefulSet + headless Service | stable identity + volume for the database |
| `redis.yaml` | PVC + Deployment + Service | single-instance cache/stream store |
| `chat.yaml` / `ingestion.yaml` / `dashboard.yaml` | Deployment + Service | the three HTTP-facing app processes |
| `worker.yaml` | Deployment (no Service) | stream consumer — nothing calls it over HTTP |
| `hpa.yaml` | HorizontalPodAutoscaler | CPU-based scaling for the worker — the honest proxy for Redis stream lag until a metrics adapter (KEDA) is worth standing up |
| `ingress.yaml` | Ingress | host-based routing (`chat.argus.local`, etc.) — path-based would have broken these apps' internal links, none built with a URL prefix in mind |
| `kustomization.yaml` | Kustomize config | one `kubectl apply -k k8s/` for all of the above |
| `kind-config.yaml` | — | local cluster config, punches ports 80/443 through to the host so Ingress is reachable at all |

Full manual run-through: [`k8s/README.md`](../k8s/README.md).
