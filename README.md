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
├── engine.py      # Pure-Python hypergraph rewriting engine
└── main.py        # FastAPI server — exposes /api/* and serves client/
client/
├── index.html     # Shell HTML (loads app.js)
└── app.js         # All rendering + interaction; calls /api/* only
Dockerfile         # python:3.11-slim, EXPOSE 8080
zeabur.json        # Zeabur deployment config (Dockerfile build type)
```

The **server** is the only engine: it computes evolutions on demand,
persists results to `data/cache/`, and serves the client over HTTP.
The **client** is a pure display layer — no rewriting logic lives in
the browser.

---

## Quick Start

### Docker (recommended)

```bash
docker build -t dhexplorer .
docker run --rm -p 8080:8080 -v "$(pwd)/data:/app/data" dhexplorer
# open http://localhost:8080
```

Verify the server is up:
```bash
curl http://localhost:8080/health
# {"status":"ok","uptime_s":3,"version":"3d48ba4"}
```

The `-v "$(pwd)/data:/app/data"` flag mounts the cache directory so
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
| `tests/test_engine.py` | Engine unit tests (parse, match, evolve, lineage, hash, dimension) | 35 |
| `tests/test_api.py`    | FastAPI endpoint integration tests (all routes + error paths) | 21 |
| `tests/test_smoke.py`  | Serving-layer smoke tests (MIME types, /health, rules list) | 12 |

---

## API

| Method | Path | Description |
|--------|------|-------------|
| `GET`  | `/health` | Liveness probe → `{"status":"ok","uptime_s":N,"version":"sha"}` |
| `GET`  | `/api/rules` | List all built-in rules |
| `GET`  | `/api/rules/{id}` | Full evolution data (states, events, causal graph) |
| `GET`  | `/api/rules/{id}/multiway` | Multiway system for a rule |
| `GET`  | `/api/rules/{id}/descendants` | Trace edge descendants across steps |
| `POST` | `/api/custom` | Run a custom rule (`notation`, `init`, `steps`) |
| `GET`  | `/api/custom/{key}` | Recall a previously computed custom rule |
| `GET`  | `/api/cache/custom` | List cached custom rules (notation, init, max_steps) |

Interactive docs: `http://localhost:8080/docs`

---

## Custom rules

In the UI, open the **Custom** panel, enter a rewrite notation
(e.g. `{{x,y}} → {{x,z},{z,y}}`), set an initial condition and step
count, and click **Run**. Results are cached server-side and survive
restarts — use **Recent rules** to recall without recomputation.

---

## Deploy (Zeabur)

Push to `main`. Zeabur auto-builds from `Dockerfile` and injects `$PORT`
at runtime (defaults to 8080 if unset). Configure a persistent volume
on `data/` in the Zeabur service settings so the cache survives deploys.

Full deploy checklist: `knowledge/devx/deploy-verification-plan.md`

---

## Development Workflow

This repo uses a **single shared clone**. The `main` branch is protected
by a pre-commit hook — direct commits are blocked.

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
