/* Argus dashboard client.
 *
 * Polls JSON endpoints and repaints. No websockets — a metrics page that
 * refreshes every few seconds does not need a persistent connection, and
 * polling keeps the k8s deployment a plain Service with no session affinity.
 *
 * Charts are updated in place rather than destroyed and rebuilt, so the y-axis
 * does not jump and the tooltip survives a refresh landing mid-hover.
 */

const $ = (id) => document.getElementById(id);
const REFRESH_MS = 5000;

const state = { window: 15, charts: {}, timer: null };

/* ---------- helpers ---------- */

async function api(path) {
  const res = await fetch(path);
  if (!res.ok) throw new Error(`${path} → ${res.status}`);
  return res.json();
}

const fmt = {
  int: (n) => (n === null || n === undefined ? "—" : Number(n).toLocaleString()),
  ms: (n) => (n === null || n === undefined ? "—" : Math.round(n).toLocaleString()),
  pct: (n) => (n === null || n === undefined ? "—" : `${n}%`),
  // Costs here are fractions of a cent. Two decimals would render every real
  // number as $0.00, so small values keep more precision.
  usd: (n) => {
    if (n === null || n === undefined) return "—";
    const v = Number(n);
    if (v === 0) return "$0";
    return v < 0.01 ? `$${v.toFixed(6)}` : `$${v.toFixed(4)}`;
  },
  tokens: (n) => {
    const v = Number(n || 0);
    return v >= 1000 ? `${(v / 1000).toFixed(1)}k` : String(v);
  },
  time: (iso) => (iso ? new Date(iso).toLocaleTimeString() : "—"),
  clock: (iso) =>
    iso ? new Date(iso).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }) : "",
};

function cell(text, cls = "") {
  const td = document.createElement("td");
  if (cls) td.className = cls;
  td.textContent = text;
  return td;
}

function pill(status) {
  const td = document.createElement("td");
  const span = document.createElement("span");
  span.className = `pill ${status}`;
  span.textContent = status;
  td.append(span);
  return td;
}

function header(table, labels) {
  const thead = document.createElement("thead");
  const tr = document.createElement("tr");
  for (const label of labels) {
    const th = document.createElement("th");
    th.textContent = label;
    tr.append(th);
  }
  thead.append(tr);
  table.replaceChildren(thead);
  const tbody = document.createElement("tbody");
  table.append(tbody);
  return tbody;
}

function empty(table, message) {
  const tbody = header(table, []);
  const tr = document.createElement("tr");
  const td = document.createElement("td");
  td.className = "empty";
  td.textContent = message;
  tr.append(td);
  tbody.append(tr);
}

/* ---------- charts ---------- */

const CSS = getComputedStyle(document.documentElement);
const color = (name) => CSS.getPropertyValue(name).trim();

Chart.defaults.color = color("--text-faint");
Chart.defaults.borderColor = color("--line-soft");
Chart.defaults.font.family =
  "ui-monospace, SFMono-Regular, Menlo, Consolas, monospace";
Chart.defaults.font.size = 10;
Chart.defaults.animation.duration = 250;
Chart.defaults.maintainAspectRatio = false;

const baseOptions = {
  responsive: true,
  interaction: { mode: "index", intersect: false },
  plugins: {
    legend: { labels: { boxWidth: 10, boxHeight: 10, padding: 12 } },
    tooltip: {
      backgroundColor: "#0f1318",
      borderColor: color("--line"),
      borderWidth: 1,
      padding: 9,
      titleColor: color("--text"),
      bodyColor: color("--text-dim"),
    },
  },
  scales: {
    x: { grid: { display: false }, ticks: { maxRotation: 0, autoSkipPadding: 18 } },
    y: { beginAtZero: true, grid: { color: color("--line-soft") } },
  },
};

function line(id, datasets) {
  return new Chart($(id), {
    type: "line",
    data: { labels: [], datasets },
    options: baseOptions,
  });
}

function makeCharts() {
  state.charts.latency = line("chart-latency", []);

  state.charts.throughput = new Chart($("chart-throughput"), {
    type: "bar",
    data: {
      labels: [],
      datasets: [
        { label: "calls/min", data: [], backgroundColor: color("--accent-dim"), borderRadius: 2 },
      ],
    },
    options: baseOptions,
  });

  state.charts.errors = new Chart($("chart-errors"), {
    type: "bar",
    data: {
      labels: [],
      datasets: [
        { label: "ok", data: [], backgroundColor: "#2c6b47", borderRadius: 2, stack: "s" },
        { label: "failed", data: [], backgroundColor: color("--danger"), borderRadius: 2, stack: "s" },
      ],
    },
    options: {
      ...baseOptions,
      scales: {
        x: { ...baseOptions.scales.x, stacked: true },
        y: { ...baseOptions.scales.y, stacked: true },
      },
    },
  });

  state.charts.cost = new Chart($("chart-cost"), {
    type: "bar",
    data: { labels: [], datasets: [{ label: "USD", data: [], backgroundColor: color("--warn"), borderRadius: 2 }] },
    options: { ...baseOptions, indexAxis: "y", plugins: { ...baseOptions.plugins, legend: { display: false } } },
  });
}

function series(chart, labels, datasets) {
  chart.data.labels = labels;
  chart.data.datasets = datasets;
  chart.update();
}

/* ---------- panels ---------- */

async function loadOverview() {
  const d = await api(`/api/overview?window=${state.window}`);

  $("t-calls").textContent = fmt.int(d.calls);
  $("t-rpm").textContent = `${d.calls_per_min ?? 0}/min`;
  $("t-p50").textContent = fmt.ms(d.p50_ms);
  $("t-p95").textContent = fmt.ms(d.p95_ms);
  $("t-p99").textContent = fmt.ms(d.p99_ms);
  $("t-ttft").textContent = fmt.ms(d.p50_ttft_ms);

  const err = $("t-err");
  err.textContent = fmt.pct(d.error_rate_pct);
  err.className = "tile-value" + (d.error_rate_pct >= 5 ? " bad" : d.error_rate_pct > 0 ? " warn" : "");
  $("t-err-sub").textContent =
    `${fmt.int(d.failures)} failed` + (d.rate_limited ? ` · ${d.rate_limited} throttled` : "");

  $("t-tokens").textContent = fmt.tokens(d.tokens);
  $("t-cost").textContent = fmt.usd(d.cost_usd);
  // Unpriced calls are surfaced rather than folded into the total, because
  // "unknown spend" and "no spend" are different facts.
  $("t-unpriced").textContent = d.unpriced ? `${d.unpriced} unpriced` : "USD";
}

async function loadLatency() {
  const d = await api(`/api/latency?window=${state.window}`);
  const labels = d.points.map((p) => fmt.clock(p.bucket));

  const datasets = d.exact_percentiles
    ? [
        { label: "p50", data: d.points.map((p) => p.p50), borderColor: color("--accent"), backgroundColor: "transparent", tension: 0.25, pointRadius: 0, borderWidth: 2 },
        { label: "p95", data: d.points.map((p) => p.p95), borderColor: color("--warn"), backgroundColor: "transparent", tension: 0.25, pointRadius: 0, borderWidth: 1.5 },
        { label: "p99", data: d.points.map((p) => p.p99), borderColor: color("--danger"), backgroundColor: "transparent", tension: 0.25, pointRadius: 0, borderWidth: 1.5 },
        { label: "TTFT p50", data: d.points.map((p) => p.ttft), borderColor: color("--text-faint"), backgroundColor: "transparent", borderDash: [4, 3], tension: 0.25, pointRadius: 0, borderWidth: 1.5 },
      ]
    : [
        { label: "avg", data: d.points.map((p) => p.avg), borderColor: color("--accent"), backgroundColor: "transparent", tension: 0.25, pointRadius: 0, borderWidth: 2 },
        { label: "max", data: d.points.map((p) => p.max), borderColor: color("--danger"), backgroundColor: "transparent", tension: 0.25, pointRadius: 0, borderWidth: 1.5 },
        { label: "TTFT avg", data: d.points.map((p) => p.ttft), borderColor: color("--text-faint"), backgroundColor: "transparent", borderDash: [4, 3], tension: 0.25, pointRadius: 0, borderWidth: 1.5 },
      ];

  series(state.charts.latency, labels, datasets);

  const note = $("latency-note");
  note.textContent = d.exact_percentiles
    ? "raw rows · exact percentiles"
    : "rollup · percentiles not aggregable, avg and max shown";
  note.className = d.exact_percentiles ? "card-note" : "card-note warn";
  $("source-note").textContent = `source: ${d.source}`;
}

async function loadThroughput() {
  const d = await api(`/api/throughput?window=${state.window}`);
  series(state.charts.throughput, d.points.map((p) => fmt.clock(p.bucket)), [
    { label: "calls/min", data: d.points.map((p) => p.calls), backgroundColor: color("--accent-dim"), borderRadius: 2 },
  ]);
}

async function loadErrors() {
  const d = await api(`/api/errors?window=${state.window}`);

  series(state.charts.errors, d.series.map((p) => fmt.clock(p.bucket)), [
    { label: "ok", data: d.series.map((p) => p.ok), backgroundColor: "#2c6b47", borderRadius: 2, stack: "s" },
    { label: "failed", data: d.series.map((p) => p.failed), backgroundColor: color("--danger"), borderRadius: 2, stack: "s" },
  ]);

  const table = $("table-errors");
  if (!d.by_kind.length) {
    empty(table, "no failures in this window");
    $("errors-note").textContent = "";
    return;
  }
  $("errors-note").textContent = `${d.by_kind.length} kind(s)`;
  const tbody = header(table, ["status", "provider", "model", "error type", "calls", "last seen"]);
  for (const row of d.by_kind) {
    const tr = document.createElement("tr");
    tr.append(pill(row.status), cell(row.provider), cell(row.model), cell(row.error_type ?? "—"),
              cell(fmt.int(row.calls), "num strong"), cell(fmt.time(row.last_seen)));
    tbody.append(tr);
  }
}

async function loadCost() {
  const d = await api(`/api/cost?window=${state.window}&group_by=model`);
  series(state.charts.cost, d.rows.map((r) => r.key), [
    { label: "USD", data: d.rows.map((r) => r.cost_usd), backgroundColor: color("--warn"), borderRadius: 2 },
  ]);
}

async function loadModels() {
  const d = await api(`/api/models?window=${state.window}`);
  const table = $("table-models");
  if (!d.rows.length) return empty(table, "no calls in this window");

  const tbody = header(table, ["model", "calls", "avg", "p95", "fail"]);
  for (const row of d.rows) {
    const tr = document.createElement("tr");
    tr.append(cell(row.model, "strong"), cell(fmt.int(row.calls), "num"),
              cell(fmt.ms(row.avg_ms), "num"), cell(fmt.ms(row.p95_ms), "num"),
              cell(fmt.int(row.failures), "num"));
    tbody.append(tr);
  }
}

async function loadRecent() {
  const d = await api("/api/recent?limit=25");
  const table = $("table-recent");
  if (!d.rows.length) return empty(table, "nothing ingested yet — send a message in the chat app");

  const tbody = header(table, ["time", "provider", "model", "status", "latency", "ttft", "tokens", "cost", "prompt"]);
  for (const row of d.rows) {
    const tr = document.createElement("tr");
    tr.append(cell(fmt.time(row.started_at)), cell(row.provider), cell(row.model, "strong"),
              pill(row.status), cell(fmt.ms(row.latency_ms), "num"), cell(fmt.ms(row.ttft_ms), "num"),
              cell(fmt.int(row.total_tokens), "num"), cell(fmt.usd(row.cost_usd), "num"),
              cell(row.input_preview ?? "", "preview"));
    tbody.append(tr);
  }
}

async function loadPipeline() {
  const d = await api("/api/pipeline");
  const sdk = d.sdk || {};
  const stream = d.stream || {};

  // Anything non-zero here is data that did not arrive. Coloured so it cannot
  // be mistaken for a healthy figure.
  const stats = [
    ["emitted", sdk.emitted ?? "—", ""],
    ["sent", sdk.sent ?? "—", ""],
    ["dropped", sdk.dropped ?? "—", (sdk.dropped || 0) > 0 ? "bad" : "ok"],
    ["spilled", sdk.spilled ?? "—", (sdk.spilled || 0) > 0 ? "warn" : "ok"],
    ["send failures", sdk.send_failures ?? "—", (sdk.send_failures || 0) > 0 ? "warn" : "ok"],
    ["stream depth", stream.length ?? "—", ""],
    ["consumer lag", stream.lag ?? "—", (stream.lag || 0) > 100 ? "warn" : "ok"],
    ["pending", stream.pending ?? "—", (stream.pending || 0) > 0 ? "warn" : "ok"],
    ["consumers", stream.consumers ?? "—", (stream.consumers || 0) > 0 ? "ok" : "bad"],
    ["dead letters", d.dead_letters?.total ?? "—", (d.dead_letters?.total || 0) > 0 ? "warn" : "ok"],
  ];

  const host = $("pipeline");
  host.replaceChildren();
  for (const [label, value, cls] of stats) {
    const box = document.createElement("div");
    box.className = "stat";
    const l = document.createElement("span");
    l.className = "stat-label";
    l.textContent = label;
    const v = document.createElement("span");
    v.className = `stat-value ${cls}`;
    v.textContent = typeof value === "number" ? fmt.int(value) : value;
    box.append(l, v);
    host.append(box);
  }

  if (!d.sdk_reachable || !d.ingest_reachable) {
    const box = document.createElement("div");
    box.className = "stat";
    const l = document.createElement("span");
    l.className = "stat-label";
    l.textContent = "unreachable";
    const v = document.createElement("span");
    v.className = "stat-value bad";
    v.textContent = [!d.sdk_reachable && "chat", !d.ingest_reachable && "ingestion"]
      .filter(Boolean)
      .join(" · ");
    box.append(l, v);
    host.append(box);
  }
}

/* ---------- refresh ---------- */

async function refreshAll() {
  // Panels load concurrently and fail independently — one broken query must not
  // blank the whole page.
  const results = await Promise.allSettled([
    loadOverview(), loadLatency(), loadThroughput(),
    loadErrors(), loadCost(), loadModels(), loadRecent(), loadPipeline(),
  ]);
  const failed = results.filter((r) => r.status === "rejected");
  $("updated").textContent = failed.length
    ? `${failed.length} panel(s) failed · ${new Date().toLocaleTimeString()}`
    : `updated ${new Date().toLocaleTimeString()}`;
  if (failed.length) console.warn("panel errors", failed.map((f) => f.reason));
}

function setWindow(minutes, button) {
  state.window = minutes;
  for (const b of $("window-picker").children) b.classList.toggle("active", b === button);
  refreshAll();
}

$("window-picker").addEventListener("click", (e) => {
  const button = e.target.closest("button[data-window]");
  if (button) setWindow(Number(button.dataset.window), button);
});

// Stop polling while the tab is hidden — no point querying Postgres every five
// seconds for a page nobody is looking at.
document.addEventListener("visibilitychange", () => {
  if (document.hidden) {
    clearInterval(state.timer);
  } else {
    refreshAll();
    state.timer = setInterval(refreshAll, REFRESH_MS);
  }
});

makeCharts();
refreshAll();
state.timer = setInterval(refreshAll, REFRESH_MS);
