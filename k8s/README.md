# Running Argus on Kubernetes (local, via kind)

## 1. Create the cluster

```bash
kind create cluster --name argus --config k8s/kind-config.yaml
```

`kind` builds a Docker container that *is* a k8s node, and `kubectl` now talks
to it — check with `kubectl cluster-info`.

## 2. Install an Ingress controller

Ingress objects do nothing without a controller watching for them. kind ships
a manifest pre-tuned for its own networking quirks:

```bash
kubectl apply -f https://raw.githubusercontent.com/kubernetes/ingress-nginx/main/deploy/static/provider/kind/deploy.yaml
kubectl wait --namespace ingress-nginx \
  --for=condition=ready pod \
  --selector=app.kubernetes.io/component=controller \
  --timeout=90s
```

## 3. Install the metrics-server (needed for the HPA)

The HPA reads CPU utilisation from `metrics-server`; it isn't built into k8s.

```bash
kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml
# kind's CA setup makes metrics-server distrust kubelet certs by default —
# patch it to skip verification (fine for local dev, never for a real cluster).
kubectl patch deployment metrics-server -n kube-system --type='json' \
  -p='[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--kubelet-insecure-tls"}]'
```

## 4. Build the image and load it into the cluster

`kind`'s node can't `docker pull` an image that only exists in your local
Docker — it has to be handed over directly:

```bash
docker build -t argus:dev .
kind load docker-image argus:dev --name argus
```

Every `imagePullPolicy: Never` in these manifests exists because of this step
— it tells kubelet "don't try to pull this, it's already here."

## 5. Fill in secrets and apply everything

```bash
cp k8s/secret.example.yaml k8s/secret.yaml
# edit k8s/secret.yaml — GROQ_API_KEY / CEREBRAS_API_KEY, or leave blank for mock
kubectl apply -k k8s/
```

## 6. Point your hosts file at the Ingress

The Ingress routes by hostname (`chat.argus.local`, etc.) — your machine needs
to know those names mean "localhost":

```bash
echo "127.0.0.1 chat.argus.local ingestion.argus.local dashboard.argus.local" | sudo tee -a /etc/hosts
```

## 7. Run migrations against the cluster's Postgres

```bash
kubectl port-forward -n argus svc/postgres 5433:5432 &
DATABASE_URL=postgresql+asyncpg://argus:argus@localhost:5433/argus uv run alembic upgrade head
```

## 8. Verify

```bash
kubectl get pods -n argus                       # everything Running
curl http://chat.argus.local/health
curl http://ingestion.argus.local/health
curl http://dashboard.argus.local/health

kubectl scale deployment worker -n argus --replicas=3   # HPA proof, manual trigger
kubectl get hpa -n argus -w                              # watch it react to load instead
```

## Teardown

```bash
kind delete cluster --name argus
```
