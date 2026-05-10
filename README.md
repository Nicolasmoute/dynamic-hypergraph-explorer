# Dynamic Hypergraph Explorer

A browser-based simulator for **dynamic hypergraph rewriting** — the
discrete computational model at the heart of the
[Wolfram Physics Project](https://www.wolframphysics.org/).

Define a rewrite rule like `{{x,y},{x,z}} → {{x,z},{x,w},{y,w},{z,w}}`,
watch the hypergraph evolve step by step, and inspect the emergent
causal graph, lineage, and estimated dimension.

---

## Architecture

```
server/
├── engine.py          # Pure-Python hypergraph rewriting engine
└── main.py            # FastAPI server — exposes /api/* and serves client/
client/
├── index.html         # Shell HTML (loads app.js)
├── app.js             # All rendering + interaction; calls /api/* only
├── canvas-renderer.js # Canvas rendering path for the spatial view
└── layout-worker.js   # Web Worker: off-thread D3 force layout (protocol v2)
Dockerfile             # python:3.11-slim, EXPOSE 8080
zeabur.json            # Zeabur deployment config (Dockerfile build type)
```

The **server** is the only engine: it computes evolutions on demand,
persists results to `data/cache/`, and serves the client over HTTP.
The **client** is a pure display layer — no rewriting logic lives in
the browser.

### Rendering paths

| View | Renderer | Notes |
|------|----------|-------|
| Spatial (main graph) | Canvas by default, SVG via `?renderer=svg` | Canvas uses an off-thread Web Worker for D3 force layout |
| Causal graph | SVG, static step-layered layout | O(N); no force sim; capped at 8,000 visible events |
| Multiway system | SVG, static step-layered layout | Bounded at 300 states by the engine |
| Multiway Causal | SVG, lazy-loaded on tab open | Static step-layered DAG with cross-branch causal edges |

---

## Quick Start

### Docker (recommended)

```bash
docker build -t dhexplorer .
docker run --rm -p 8080:8080 -v "$(pwd)/data:/data" dhexplorer
# open http://localhost:8080
```

Verify the server is up:
```bash
curl http://localhost:8080/health
# {"status":"ok","uptime_s":3,"version":"3d48ba4"}
```

The `-v "$(pwd)/data:/data"` flag mounts the cache directory so
computed results survive container restarts.

### Local dev (no Docker)

```bash
pip install -r server/requirements.txt
python -m uvicorn server.main:app --reload --port 8080
# open http://localhost:8080
```

---

## Tests

```bash
pip install -r server/requirements.txt -r requirements-dev.txt
pytest
```

| File | What it covers | Count |
|------|---------------|-------|
| `tests/test_engine.py`       | Engine unit tests (parse, match, evolve, causal index, lineage, hash, dimension) | 98 |
| `tests/test_api.py`          | FastAPI endpoint integration tests (all routes, job polling, extend, abort, stale-but-done, multiway-causal) | 80 |
| `tests/test_smoke.py`        | Serving-layer smoke tests (MIME types, /health, rules list) | 12 |
| `tests/test_dimension.py`    | Dimension estimation correctness (incidence-BFS metric) | 6 |
| `tests/test_browser_smoke.py`| Playwright E2E browser tests — skipped by default; `--run-slow` to enable | — |

---

## API

| Method | Path | Description |
|--------|------|-------------|
| `GET`    | `/health` | Liveness probe → `{"status":"ok","uptime_s":N,"version":"sha"}` |
| `GET`    | `/api/rules` | List all built-in rules |
| `GET`    | `/api/rules/{id}` | Full evolution data (states, events, causal graph) |
| `GET`    | `/api/rules/{id}/multiway` | Multiway system for a rule |
| `GET`    | `/api/rules/{id}/multiway-causal` | Multiway causal graph for a built-in rule |
| `GET`    | `/api/rules/{id}/descendants` | Trace edge descendants across steps |
| `POST`   | `/api/custom` | Start async evolution (`notation`, `init`, `steps`) → `{job_id, status}` |
| `POST`   | `/api/custom/multiway-causal` | Compute a custom multiway causal graph synchronously |
| `GET`    | `/api/jobs/{job_id}` | Poll job status (`running` / `done` / `stale`) |
| `DELETE` | `/api/jobs/{job_id}` | Abort a running job cooperatively |
| `POST`   | `/api/extend` | Extend a cached result by more steps (`key`, `extra_steps`) |
| `GET`    | `/api/custom/{key}` | Recall a previously computed result by cache key |
| `GET`    | `/api/cache/custom` | List cached custom rules (notation, init, max_steps) |

Long-running evolutions are fully async: `POST /api/custom` or `POST /api/extend` return
immediately with a `job_id`; poll `GET /api/jobs/{job_id}` every 2 s until `status=done`.

Interactive docs: `http://localhost:8080/docs`

---

## Custom rules

In the UI, open the **Custom** panel, enter a rewrite notation
(e.g. `{{x,y}} → {{x,z},{z,y}}`), set an initial condition and step
count, and click **Run**. Results are cached server-side and survive
restarts — use **Recent rules** to recall without recomputation.

---

## Canvas rendering

Canvas is the default renderer for the spatial (main) view. Append `?renderer=svg`
to force the legacy SVG path. In this mode the D3 force simulation runs in a
dedicated Web Worker (`layout-worker.js`) — the main thread never blocks during
layout:

```
http://localhost:8080/?renderer=svg
```

The worker uses protocol v2 (graph-scoped `graph_id`, partial `update_options`, chain-only
hyperedge physics). The SVG path remains available as a fallback.

## Multiway Causal

The **Multiway Causal** tab is live in the deployed app. It loads lazily when the tab
is opened and fetches `/api/rules/{id}/multiway-causal` on demand, so the main page
stays responsive until the view is actually requested.

The red overlay is the Single-History greedy parallel event set embedded in the
multiway occurrence DAG via `default_path_event_ids`; green nodes are the
remaining multiway occurrences. The API does not return separate
`realized_events` or `realized_causal_edges` arrays.

---

## Performance notes

### Engine

| Scenario | Before | After | Speedup |
|----------|--------|-------|---------|
| Rule 1, step 12→13 (7,114 edges) | 117.6 s | 1.05 s | **112×** |
| Rule 1, step 13→14 (14,052 edges) | ~470 s est. | 2.3 s live | **>100×** |
| Rule 4, step 10 (59,049 edges) | 2.7 s | 0.8 s | **3.3×** |

Key techniques: node-to-edges index (O(E × avg_degree) matching replaces O(E²)),
produced-edge index for causal attribution (O(k) per event replaces O(N²) scan).

### Causal view

The causal view uses a static step-layered layout (O(N)) and caps display at 8,000
events (most recent shown). This avoids the D3 force simulation that froze the browser
at rule 1 step 13 (7,129 events), rule 4 step 9 (9,841 events), and rule 3 step 15
(32,767 events).

### Multiway view

The multiway view is engine-bounded at `max_states=300` and uses static layout —
no force simulation at any scale.

---

## Deploy (Zeabur)

Push to `main`. Zeabur auto-builds from `Dockerfile` and injects `$PORT`
at runtime (defaults to 8080 if unset).

### Persistent cache volume (required)

The server caches computed results under `/data/cache/v13/`. Without a
persistent volume this directory is lost on every deploy restart. Configure it
once in the Zeabur dashboard:

1. Open your service → **Storage** tab → **Add Volume**.
2. Set **Mount path** to `/data`.
3. Zeabur assigns a volume ID automatically — save it.

From that point on, the cache survives container restarts and new deploys.

### Optional environment variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `DH_CACHE_DIR` | `/data/cache` (set in Dockerfile) | Override cache root path |
| `DH_CORS_ORIGINS` | `*` | Comma-separated allowed origins (set to your Zeabur domain in production, e.g. `https://your-app.zeabur.app`) |
| `DH_MULTIWAY_MAX_STEPS` | `4` | Multiway computation step limit |
| `DH_MULTIWAY_MAX_STATES` | `300` | Multiway state count limit |
| `DH_MULTIWAY_MAX_TIME_MS` | `3000` | Multiway time limit (ms) |

Full deploy verification checklist: `knowledge/devx/deploy-verification-plan.md`

---

## Development Workflow

This repo uses a **single shared clone**. The `main` branch is protected
by a pre-commit hook — direct commits are blocked. Emergency bypass
(`git commit --no-verify`) requires **Coach pre-authorization** — message
Coach first, then announce on broadcast with the approval reference.

```bash
git pull --rebase
git checkout -b feat/<your-name>-<slug>
# edit, test
pytest
git add <files>
git commit -m "feat(scope): summary"
git push -u origin feat/<your-name>-<slug>
# fast-forward merge to main after review
```

---

## Built-in Rules

| ID | Name | Notation | Emergent property |
|----|------|----------|------------------|
| rule1 | Signature Rule | `{{x,y},{x,z}} → {{x,z},{x,w},{y,w},{z,w}}` | ~2D space |
| rule2 | Path Subdivision | `{{x,y},{y,z}} → {{x,y},{y,w},{w,z}}` | 1D ring |
| rule3 | Binary Tree Growth | `{{x,y}} → {{x,y},{y,z}}` | Hyperbolic / exponential |
| rule4 | Sierpiński Hyperedge | `{{x,y,z}} → {{x,u,w},{y,v,u},{z,w,v}}` | Fractal dim ≈ 1.585 |
| rule5 | Chain Rewrite | `{{x,y,z},{z,u,v}} → {{y,z,u},{v,w,x},{w,y,v}}` | Partial / mixed |

---

## References

- Wolfram Physics Project — <https://www.wolframphysics.org/>
- Gorard, J. (2020). "Some Relativistic and Gravitational Properties of the Wolfram Model."
- Wolfram, S. (2020). "A Class of Models with the Potential to Represent Fundamental Physics."

---

## License

MIT
