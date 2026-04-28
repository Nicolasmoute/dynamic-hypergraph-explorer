"""FastAPI server for Dynamic Hypergraph Explorer."""
from __future__ import annotations
import hashlib
import json
import os
import subprocess
import threading
import time as _time
from collections import deque
from contextlib import asynccontextmanager
from pathlib import Path
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import Optional
from server import engine

# ── Process metadata (captured once at import time) ───────────────────
_START_TIME: float = _time.time()

def _git_sha() -> str:
    """Return short git SHA of HEAD, or 'dev' if git is unavailable."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, timeout=2, cwd=Path(__file__).parent.parent,
        )
        return result.stdout.strip() or "dev"
    except Exception:
        return "dev"

_VERSION: str = _git_sha()

# ── Persistent cache configuration ───────────────────────────────────
# Cache root is configurable via env var DH_CACHE_DIR (default ./data/cache).
# The actual cache directory includes the engine CACHE_VERSION so that bumping
# the version in engine.py automatically routes new writes to a fresh directory
# while old files remain intact under the previous version path.
_CACHE_ROOT: Path = Path(os.environ.get("DH_CACHE_DIR", "./data/cache"))
CACHE_DIR: Path = _CACHE_ROOT / engine.CACHE_VERSION

# ── Preloaded rules ───────────────────────────────────────────────────
RULES = [
    {
        "id": "rule1", "name": "Signature Rule",
        "notation": "{{x,y},{x,z}} → {{x,z},{x,w},{y,w},{z,w}}",
        "desc": "Two edges sharing a source become four, creating a new node. Generates emergent 2D space.",
        "blurb": "The classic Wolfram-physics signature rule. Two edges sharing a node fan out into four edges and a new node, iteratively tiling a flat 2-dimensional space. Dimension converges to ~2.",
        "tag": "2D Space", "tagClass": "tag-2d",
        "init": [[0, 0], [0, 0]],
        "steps": 12,
    },
    {
        "id": "rule2", "name": "Path Subdivision",
        "notation": "{{x,y},{y,z}} → {{x,y},{y,w},{w,z}}",
        "desc": "Finds a 2-edge path and inserts a new node in the middle. Refines 1D ring structure.",
        "blurb": "Inserts a midpoint into every consecutive pair of edges. Starting from a 5-node ring the graph grows while keeping its 1-dimensional loop topology. Dimension converges to ~1.",
        "tag": "1D Ring", "tagClass": "tag-1d",
        "init": [[0, 1], [1, 2], [2, 3], [3, 4], [4, 0]],
        "steps": 17,
    },
    {
        "id": "rule3", "name": "Binary Tree Growth",
        "notation": "{{x,y}} → {{x,y},{y,z}}",
        "desc": "Every edge spawns a new edge from its target. Exponential doubling every step.",
        "blurb": "Each edge grows a child edge, producing a binary tree that doubles in size every step. Edge count grows as 2^n, yielding fractal-like exponential expansion rather than a smooth manifold.",
        "tag": "Exponential", "tagClass": "tag-tree",
        "init": [[0, 1]],
        "steps": 12,
    },
    {
        "id": "rule4", "name": "Sierpinski Hyperedge",
        "notation": "{{x,y,z}} → {{x,u,w},{y,v,u},{z,w,v}}",
        "desc": "Ternary hyperedges split into 3 new ones. Fractal structure, dimension ≈ 1.4.",
        "blurb": "A single ternary hyperedge self-similarly subdivides into three, recreating the Sierpinski gasket. Each step triples the hyperedge count. Hausdorff dimension ≈ log(3)/log(2) ≈ 1.585.",
        "tag": "Fractal", "tagClass": "tag-fractal",
        "init": [[0, 1, 2]],
        "steps": 7,
    },
    {
        "id": "rule5", "name": "Chain Rewrite",
        "notation": "{{x,y,z},{z,u,v}} → {{y,z,u},{v,w,x},{w,y,v}}",
        "desc": "Two chain-linked ternary hyperedges fuse into three. Unchained edges survive across steps.",
        "blurb": "Pairs of ternary hyperedges that share a node get rewritten as three new ones, while unpaired edges carry over unchanged. Produces a complex partially-rewritten structure across steps.",
        "tag": "Partial", "tagClass": "tag-mixed",
        "init": [[0,1,2],[2,3,4],[4,5,6],[6,7,8],[8,9,0],[1,3,5],[5,7,9],[9,1,3]],
        "steps": 14,
    },
]

# ── In-memory cache + per-key locks ──────────────────────────────────
CACHE: dict[str, dict] = {}
_compute_locks: dict[str, threading.Lock] = {}
_locks_lock = threading.Lock()

def _get_lock(key: str) -> threading.Lock:
    with _locks_lock:
        if key not in _compute_locks:
            _compute_locks[key] = threading.Lock()
        return _compute_locks[key]


# ── Disk cache helpers ────────────────────────────────────────────────

def _disk_path(key: str) -> Path:
    """Return the JSON cache file path for the given key."""
    return CACHE_DIR / f"{key}.json"


def _disk_write(key: str, data: dict) -> None:
    """Atomically write data to the disk cache for key.

    Writes to a temp file first then os.replace() so a partial write never
    leaves a corrupt cache entry visible to concurrent readers.
    """
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    p = _disk_path(key)
    tmp = p.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(data, separators=(",", ":")))
    os.replace(tmp, p)


def _disk_read(key: str) -> dict | None:
    """Return cached data from disk, or None if the entry does not exist."""
    p = _disk_path(key)
    if p.exists():
        try:
            return json.loads(p.read_text())
        except (json.JSONDecodeError, OSError):
            return None
    return None


def _custom_cache_key(notation: str, init: list, steps: int, time_limit_ms: int) -> str:
    """Derive a stable cache key for a custom rule invocation."""
    raw = notation + json.dumps(init, sort_keys=True) + str(steps) + str(time_limit_ms)
    h = hashlib.sha256(raw.encode()).hexdigest()[:16]
    return f"custom-{h}"


# ── Core data accessors (memory → disk → compute) ─────────────────────

def get_rule_data(rule_id: str) -> dict:
    """Return evolution data for a built-in rule.

    Read order: in-memory CACHE → disk JSON → recompute (then persist).
    """
    if rule_id in CACHE:
        return CACHE[rule_id]

    lock = _get_lock(rule_id)
    with lock:
        if rule_id in CACHE:          # double-checked
            return CACHE[rule_id]

        # Disk cache hit
        cached = _disk_read(rule_id)
        if cached is not None:
            CACHE[rule_id] = cached
            return cached

        # Compute
        rule = next((r for r in RULES if r["id"] == rule_id), None)
        if not rule:
            raise HTTPException(404, f"Rule {rule_id} not found")

        parsed = engine.parse_notation(rule["notation"])
        if not parsed:
            raise HTTPException(400, f"Cannot parse notation for {rule_id}")

        result = engine.evolve(rule["init"], parsed["lhs"], parsed["rhs"], rule["steps"], time_limit_ms=30000)
        lineage, birth_steps = engine.build_lineage(result["states"], result["events"])

        data = {
            "states": result["states"],
            "events": result["events"],
            "causal_edges": result["causal_edges"],
            "stats": result["stats"],
            "lineage": lineage,
            "birthSteps": birth_steps,
        }
        _disk_write(rule_id, data)
        CACHE[rule_id] = data
        return data


def get_multiway(rule_id: str) -> dict:
    """Return multiway data for a built-in rule.

    Read order: in-memory CACHE → disk JSON → recompute (then persist).
    """
    cache_key = f"{rule_id}_multiway"
    if cache_key in CACHE:
        return CACHE[cache_key]

    lock = _get_lock(cache_key)
    with lock:
        if cache_key in CACHE:        # double-checked
            return CACHE[cache_key]

        # Disk cache hit
        cached = _disk_read(cache_key)
        if cached is not None:
            CACHE[cache_key] = cached
            return cached

        # Compute
        rule = next((r for r in RULES if r["id"] == rule_id), None)
        if not rule:
            raise HTTPException(404, f"Rule {rule_id} not found")

        parsed = engine.parse_notation(rule["notation"])
        result = engine.compute_multiway(rule["init"], parsed["lhs"], parsed["rhs"])
        _disk_write(cache_key, result)
        CACHE[cache_key] = result
        return result


def _preload_disk_cache() -> int:
    """Scan CACHE_DIR and pre-populate in-memory CACHE from disk.

    Returns the number of entries loaded.
    """
    if not CACHE_DIR.exists():
        return 0
    count = 0
    for p in CACHE_DIR.glob("*.json"):
        key = p.stem
        if key not in CACHE:
            try:
                CACHE[key] = json.loads(p.read_text())
                count += 1
            except (json.JSONDecodeError, OSError):
                pass
    return count


# ── App + lifespan ────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Pre-load disk cache then fill any missing entries in a background thread."""
    loaded = _preload_disk_cache()
    print(f"Loaded {loaded} rule(s) from disk cache (CACHE_DIR={CACHE_DIR})")

    def _precompute():
        for r in RULES:
            print(f"Precomputing {r['id']}...")
            try:
                get_rule_data(r["id"])
                print(f"  {r['id']} done: {len(CACHE[r['id']]['states'])} states")
            except Exception as e:
                print(f"  {r['id']} failed: {e}")
        for r in RULES:
            print(f"Computing multiway for {r['id']}...")
            try:
                get_multiway(r["id"])
                print(f"  {r['id']} multiway done")
            except Exception as e:
                print(f"  {r['id']} multiway failed: {e}")
        print("All rules precomputed!")

    threading.Thread(target=_precompute, daemon=True).start()
    yield


app = FastAPI(title="Dynamic Hypergraph Explorer", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ── API endpoints ─────────────────────────────────────────────────────

@app.get("/health")
def health():
    """Liveness probe / readiness check.

    Response fields:
    - status        : always "ok" while the server is alive.
    - uptime_s      : integer seconds since the uvicorn process started.
    - version       : short git SHA of the deployed commit, or "dev".
    - cache_version : engine.CACHE_VERSION, e.g. "v1".
    """
    return {
        "status": "ok",
        "uptime_s": int(_time.time() - _START_TIME),
        "version": _VERSION,
        "cache_version": engine.CACHE_VERSION,
    }


@app.get("/api/rules")
def list_rules():
    """List all available rules."""
    return [
        {
            "id": r["id"], "name": r["name"], "notation": r["notation"],
            "desc": r["desc"], "blurb": r["blurb"], "tag": r["tag"], "tagClass": r["tagClass"],
        }
        for r in RULES
    ]


@app.get("/api/rules/{rule_id}")
def get_rule(rule_id: str):
    """Get full evolution data for a rule."""
    data = get_rule_data(rule_id)
    return {
        "states": data["states"],
        "events": data["events"],
        "causalEdges": data["causal_edges"],
        "stats": data["stats"],
        "lineage": data["lineage"],
        "birthSteps": data["birthSteps"],
    }


@app.get("/api/rules/{rule_id}/multiway")
def get_rule_multiway(rule_id: str):
    """Get multiway system for a rule."""
    return get_multiway(rule_id)


@app.get("/api/rules/{rule_id}/descendants")
def get_descendants(rule_id: str, viewing_step: int, edge_idx: int, origin_step: int):
    """Trace edge descendants from origin to viewing step."""
    data = get_rule_data(rule_id)
    lineage = data["lineage"]

    result = set()
    queue: deque[str] = deque([f"{origin_step}:{edge_idx}"])
    visited = set(queue)

    while queue:
        current = queue.popleft()
        for child in lineage.get(current, []):
            if child in visited:
                continue
            visited.add(child)
            c_step, c_idx = map(int, child.split(":"))
            if c_step == viewing_step:
                result.add(c_idx)
            elif c_step < viewing_step:
                queue.append(child)

    return {"descendants": sorted(result)}


# ── Custom rule endpoints ─────────────────────────────────────────────

_HARD_MAX_TIME_MS = 30_000  # hard cap regardless of user-supplied value

class CustomRuleRequest(BaseModel):
    notation: str
    init: list[list[int]]
    steps: int = 8


@app.post("/api/custom")
def run_custom_rule(req: CustomRuleRequest):
    """Run a custom rule and return evolution data plus a persistent cache key.

    The returned `key` can be passed to GET /api/custom/{key} on future
    requests (including after a server restart) to retrieve the same result
    without recomputation.
    """
    if req.steps < 1 or req.steps > 20:
        raise HTTPException(400, "Steps must be 1-20")

    parsed = engine.parse_notation(req.notation)
    if not parsed:
        raise HTTPException(400, "Cannot parse notation")
    if not parsed["lhs"] or not parsed["rhs"]:
        raise HTTPException(400, "LHS and RHS must be non-empty")
    if not req.init:
        raise HTTPException(400, "Initial state must be non-empty")

    time_limit_ms = min(15_000, _HARD_MAX_TIME_MS)
    key = _custom_cache_key(req.notation, req.init, req.steps, time_limit_ms)

    # Serve from memory or disk if already computed
    if key in CACHE:
        return {"key": key, **CACHE[key]}
    disk = _disk_read(key)
    if disk is not None:
        CACHE[key] = disk
        return {"key": key, **disk}

    # Compute
    result = engine.evolve(req.init, parsed["lhs"], parsed["rhs"], req.steps, time_limit_ms=time_limit_ms)
    lineage, birth_steps = engine.build_lineage(result["states"], result["events"])
    multiway = engine.compute_multiway(req.init, parsed["lhs"], parsed["rhs"])

    payload = {
        "states": result["states"],
        "events": result["events"],
        "causalEdges": result["causal_edges"],
        "stats": result["stats"],
        "lineage": lineage,
        "birthSteps": birth_steps,
        "multiway": multiway,
        # metadata stored in the cache file so GET /api/cache/custom can list it
        "_meta": {
            "notation": req.notation,
            "init": req.init,
            "steps": req.steps,
            "computed_at": _time.strftime("%Y-%m-%dT%H:%M:%SZ", _time.gmtime()),
        },
    }
    _disk_write(key, payload)
    CACHE[key] = payload

    return {"key": key, **payload}


@app.get("/api/custom/{key}")
def recall_custom_rule(key: str):
    """Return previously computed custom rule data by cache key.

    Does not recompute; 404 if the key is not in the cache.
    """
    if key in CACHE:
        return {"key": key, **CACHE[key]}
    disk = _disk_read(key)
    if disk is not None:
        CACHE[key] = disk
        return {"key": key, **disk}
    raise HTTPException(404, f"Cache key '{key}' not found")


@app.get("/api/cache/custom")
def list_custom_cache():
    """List all cached custom rules with their metadata.

    Used by the client to render a 'Recent custom rules' panel.
    Returns: [{key, notation, init, steps, computed_at}, ...]
    """
    if not CACHE_DIR.exists():
        return []

    result = []
    for p in sorted(CACHE_DIR.glob("custom-*.json"), key=lambda f: f.stat().st_mtime, reverse=True):
        key = p.stem
        try:
            data = json.loads(p.read_text())
            meta = data.get("_meta", {})
            result.append({
                "key": key,
                "notation": meta.get("notation", ""),
                "init": meta.get("init", []),
                "steps": meta.get("steps", 0),
                "computed_at": meta.get("computed_at", ""),
            })
        except (json.JSONDecodeError, OSError):
            continue
    return result


# ── Serve client ──────────────────────────────────────────────────────
CLIENT_DIR = Path(__file__).parent.parent / "client"

@app.get("/")
def serve_index():
    return FileResponse(CLIENT_DIR / "index.html")

@app.get("/app.js")
def serve_app_js():
    return FileResponse(CLIENT_DIR / "app.js", media_type="application/javascript")

# Mount static files (JS, CSS, images) after API routes
app.mount("/static", StaticFiles(directory=CLIENT_DIR), name="static")
