"""FastAPI server for Dynamic Hypergraph Explorer."""
from __future__ import annotations
import hashlib
import json
import logging
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

# ── Logging setup ─────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("dh.server")

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
# Cache root: $DH_CACHE_DIR/v<CACHE_VERSION>/ (default /data/cache/v2/ in
# Docker via Dockerfile ENV; ./data/cache/v2/ in bare-Python local dev).
# Bump engine.CACHE_VERSION when engine output semantics change; the new
# directory is created automatically; old data stays under the old path.
_CACHE_ROOT: Path = Path(os.environ.get("DH_CACHE_DIR", "./data/cache"))
CACHE_DIR: Path = _CACHE_ROOT / engine.CACHE_VERSION

# ── Multiway limits (§5.1) ────────────────────────────────────────────
# Override via env vars; documented defaults match the original hard-codes.
_MW_MAX_STEPS: int = int(os.environ.get("DH_MULTIWAY_MAX_STEPS", "4"))
_MW_MAX_STATES: int = int(os.environ.get("DH_MULTIWAY_MAX_STATES", "300"))
_MW_MAX_TIME_MS: int = int(os.environ.get("DH_MULTIWAY_MAX_TIME_MS", "3000"))

# ── Multiway-causal limits (Phase B3) ────────────────────────────────
_MWCAUSAL_MAX_STEPS: int = int(os.environ.get("DH_MULTIWAY_CAUSAL_MAX_STEPS", "4"))
_MWCAUSAL_MAX_OCCURRENCES: int = int(os.environ.get("DH_MULTIWAY_CAUSAL_MAX_OCCURRENCES", "5000"))
_MWCAUSAL_MAX_TIME_MS: int = int(os.environ.get("DH_MULTIWAY_CAUSAL_MAX_TIME_MS", "5000"))
_PRECOMPUTE_MULTIWAY_CAUSAL: bool = os.environ.get("DH_PRECOMPUTE_MULTIWAY_CAUSAL", "1").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)

# ── CORS (§5.5) ───────────────────────────────────────────────────────
# Production recommendation: set DH_CORS_ORIGINS to your Zeabur domain,
# e.g. "https://dynamic-hypergraph.zeabur.app".  Defaults to "*" for dev.
_CORS_ORIGINS: list[str] = [
    o.strip() for o in os.environ.get("DH_CORS_ORIGINS", "*").split(",") if o.strip()
] or ["*"]

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
    logger.debug("cache write key=%s path=%s", key, p)


def _disk_read(key: str) -> dict | None:
    """Return cached data from disk, or None if the entry does not exist."""
    p = _disk_path(key)
    if p.exists():
        try:
            data = json.loads(p.read_text())
            logger.debug("cache hit (disk) key=%s", key)
            return data
        except (json.JSONDecodeError, OSError):
            logger.warning("cache corrupt key=%s path=%s — will recompute", key, p)
            return None
    logger.debug("cache miss key=%s", key)
    return None


def _custom_cache_key(notation: str, init: list, steps: int, time_limit_ms: int) -> str:
    """Derive a stable cache key for a custom rule invocation."""
    raw = notation + json.dumps(init, sort_keys=True) + str(steps) + str(time_limit_ms)
    h = hashlib.sha256(raw.encode()).hexdigest()[:16]
    return f"custom-{h}"


def _strip_meta(payload: dict) -> dict:
    """Return payload without the internal _meta key."""
    return {k: v for k, v in payload.items() if k != "_meta"}


# ── Job tracker ───────────────────────────────────────────────────────
# Tracks in-flight and recently-completed custom rule computations so the
# client can poll for progress without blocking on a long HTTP request.
#
# Job lifecycle:  running → done | failed | cancelled
# Stale detection: if status is "running" and heartbeat_at is older than
#   _JOB_STALE_S seconds, the server almost certainly restarted mid-compute.
#   The client should show an actionable error and let the user retry.
#
# job_id == cache key (sha256 hash of inputs) so GET /api/custom/{key}
# continues to work as a recall path after a job completes.
#
# Cancellation: each running job stores a threading.Event in _jobs[key].
#   DELETE /api/jobs/{job_id} sets the event; engine.evolve() checks it at
#   the top of each step and exits the loop cooperatively.

_JOB_STALE_S = 45  # seconds without a heartbeat → stale

_jobs: dict[str, dict] = {}
_jobs_lock = threading.Lock()


def _update_job(job_id: str, **kwargs) -> None:
    with _jobs_lock:
        if job_id in _jobs:
            _jobs[job_id].update(kwargs)


def _job_response(job_id: str) -> dict:
    """Build the canonical job-status response for a given job_id.

    Checks _jobs first, then falls back to the disk/memory cache so that
    jobs which completed before a server restart are still retrievable.
    """
    with _jobs_lock:
        job = dict(_jobs[job_id]) if job_id in _jobs else None

    now = _time.time()

    if job is None:
        # Not in in-process _jobs — check persistent cache (post-restart recall)
        if job_id in CACHE:
            return {"job_id": job_id, "status": "done", "key": job_id,
                    **_strip_meta(CACHE[job_id])}
        disk = _disk_read(job_id)
        if disk is not None:
            CACHE[job_id] = disk
            return {"job_id": job_id, "status": "done", "key": job_id,
                    **_strip_meta(disk)}
        raise HTTPException(404, f"Job '{job_id}' not found")

    status = job["status"]
    elapsed = round(now - job["started_at"], 1)

    # Stale detection: running job with no heartbeat update.
    # Before declaring stale, check the persistent cache — a long step (>45s)
    # can complete and write its result while the heartbeat appears expired.
    # Without this check a legitimate "done" job would be reported as "stale".
    if status == "running":
        age = now - job.get("heartbeat_at", job["started_at"])
        if age > _JOB_STALE_S:
            cached = CACHE.get(job_id)
            if cached is None:
                cached = _disk_read(job_id)
            if cached is not None:
                # Computation finished — reconcile in-memory job entry and
                # return done so the client can render the result immediately.
                CACHE[job_id] = cached
                _update_job(job_id, status="done", result=cached,
                            heartbeat_at=now)
                status = "done"
                logger.info("job stale-but-done (cache hit) job_id=%s", job_id)
            else:
                status = "stale"
                logger.warning("job stale job_id=%s heartbeat_age=%.1fs", job_id, age)

    resp: dict = {
        "job_id": job_id,
        "status": status,
        "step": job.get("step", 0),
        "total_steps": job.get("total_steps", 0),
        "elapsed_s": elapsed,
    }
    if status == "done":
        resp["key"] = job_id
        resp.update(_strip_meta(job.get("result", {})))
    elif status in ("failed", "stale", "cancelled"):
        if "error" in job:
            resp["error"] = job["error"]
    return resp


# ── Core data accessors (memory → disk → compute) ─────────────────────

def get_rule_data(rule_id: str) -> dict:
    """Return evolution data for a built-in rule.

    Read order: in-memory CACHE → disk JSON → recompute (then persist).
    """
    if rule_id in CACHE:
        logger.debug("cache hit (memory) key=%s", rule_id)
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

        logger.info("computing rule_id=%s steps=%d", rule_id, rule["steps"])
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
        logger.info("computed rule_id=%s states=%d", rule_id, len(data["states"]))
        return data


def get_multiway(rule_id: str) -> dict:
    """Return multiway data for a built-in rule.

    Read order: in-memory CACHE → disk JSON → recompute (then persist).
    """
    cache_key = f"{rule_id}_multiway"
    if cache_key in CACHE:
        logger.debug("cache hit (memory) key=%s", cache_key)
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

        logger.info("computing multiway rule_id=%s", rule_id)
        parsed = engine.parse_notation(rule["notation"])
        result = engine.compute_multiway(
            rule["init"], parsed["lhs"], parsed["rhs"],
            max_steps=_MW_MAX_STEPS,
            max_states=_MW_MAX_STATES,
            max_time_ms=_MW_MAX_TIME_MS,
        )
        _disk_write(cache_key, result)
        CACHE[cache_key] = result
        logger.info("computed multiway rule_id=%s states=%d", rule_id, len(result.get("states", {})))
        return result


def get_multiway_causal(
    rule_id: str,
    max_steps: int,
    max_occurrences: int,
    max_time_ms: int,
) -> dict:
    """Return multiway-causal-graph data for a built-in rule.

    Cache key encodes all cap parameters so different cap settings produce
    independent cache entries.  Read order: in-memory CACHE → disk JSON →
    recompute (then persist).
    """
    # Hard caps — callers should validate first, but clamp defensively here too
    max_steps = max(1, min(max_steps, 8))
    max_occurrences = max(1, min(max_occurrences, 20_000))
    max_time_ms = max(1, min(max_time_ms, 30_000))

    cache_key = f"{rule_id}_mwcausal_{max_steps}_{max_occurrences}_{max_time_ms}"
    if cache_key in CACHE:
        logger.debug("cache hit (memory) key=%s", cache_key)
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

        logger.info(
            "computing multiway-causal rule_id=%s max_steps=%d max_occurrences=%d",
            rule_id, max_steps, max_occurrences,
        )
        parsed = engine.parse_notation(rule["notation"])
        if not parsed:
            raise HTTPException(400, f"Cannot parse notation for {rule_id}")

        result = engine.multiway_causal_graph(
            rule["init"], parsed["lhs"], parsed["rhs"],
            max_steps=max_steps,
            max_occurrences=max_occurrences,
            max_time_ms=max_time_ms,
        )
        # Attach rule metadata for client display
        result["meta"] = {
            "rule_id": rule_id,
            "rule_notation": rule["notation"],
            "init_state": rule["init"],
        }
        _disk_write(cache_key, result)
        CACHE[cache_key] = result
        logger.info(
            "computed multiway-causal rule_id=%s events=%d",
            rule_id, len(result.get("events", [])),
        )
        return result


def _precompute_builtin_multiway_causal() -> None:
    """Warm built-in multiway-causal cache entries in the background.

    This is intentionally isolated from lifespan() so tests can exercise the
    warm path deterministically without waiting on server startup timing.
    """
    if not _PRECOMPUTE_MULTIWAY_CAUSAL:
        logger.info("precompute multiway-causal disabled")
        return

    for r in RULES:
        logger.info("precompute multiway-causal start rule_id=%s", r["id"])
        started_at = _time.time()
        try:
            result = get_multiway_causal(
                r["id"],
                _MWCAUSAL_MAX_STEPS,
                _MWCAUSAL_MAX_OCCURRENCES,
                _MWCAUSAL_MAX_TIME_MS,
            )
            elapsed_ms = int((_time.time() - started_at) * 1000)
            logger.info(
                "precompute multiway-causal done rule_id=%s events=%d edges=%d truncated=%s reason=%s elapsed_ms=%d",
                r["id"],
                len(result.get("events", [])),
                len(result.get("causal_edges", [])),
                result.get("truncated"),
                result.get("truncation_reason"),
                elapsed_ms,
            )
        except Exception as e:
            logger.error(
                "precompute multiway-causal failed rule_id=%s error=%s",
                r["id"],
                e,
            )


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
                logger.warning("skipping corrupt cache file path=%s", p)
    return count


# ── App + lifespan ────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Pre-load disk cache then fill any missing entries in a background thread."""
    loaded = _preload_disk_cache()
    logger.info("startup: loaded %d rule(s) from disk cache (CACHE_DIR=%s)", loaded, CACHE_DIR)

    def _precompute():
        for r in RULES:
            logger.info("precompute start rule_id=%s", r["id"])
            try:
                get_rule_data(r["id"])
                logger.info("precompute done rule_id=%s states=%d", r["id"], len(CACHE[r["id"]]["states"]))
            except Exception as e:
                logger.error("precompute failed rule_id=%s error=%s", r["id"], e)
        for r in RULES:
            logger.info("precompute multiway start rule_id=%s", r["id"])
            try:
                get_multiway(r["id"])
                logger.info("precompute multiway done rule_id=%s", r["id"])
            except Exception as e:
                logger.error("precompute multiway failed rule_id=%s error=%s", r["id"], e)
        _precompute_builtin_multiway_causal()
        logger.info("precompute complete: all rules ready")

    threading.Thread(target=_precompute, daemon=True).start()
    yield


app = FastAPI(title="Dynamic Hypergraph Explorer", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=_CORS_ORIGINS,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── API endpoints ─────────────────────────────────────────────────────

@app.get("/health")
def health():
    """Liveness probe / readiness check.

    Response fields:
    - status        : always "ok" while the server is alive.
    - uptime_s      : integer seconds since the uvicorn process started.
    - version       : short git SHA of the deployed commit, or "dev".
    - cache_version : engine.CACHE_VERSION, e.g. "v2".
    - active_jobs   : number of custom rules currently being computed.
    """
    with _jobs_lock:
        active = sum(1 for j in _jobs.values() if j["status"] == "running")
    return {
        "status": "ok",
        "uptime_s": int(_time.time() - _START_TIME),
        "version": _VERSION,
        "cache_version": engine.CACHE_VERSION,
        "active_jobs": active,
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


@app.get("/api/rules/{rule_id}/multiway-causal")
def get_rule_multiway_causal(
    rule_id: str,
    max_steps: int = _MWCAUSAL_MAX_STEPS,
    max_occurrences: int = _MWCAUSAL_MAX_OCCURRENCES,
    max_time_ms: int = _MWCAUSAL_MAX_TIME_MS,
):
    """Get the multiway causal graph for a built-in rule.

    Returns all update events across all branches of the multiway system
    with cross-branch causal edges, per the multiway-causal-graph contract
    (Phase B).

    Query params:
    - max_steps        : BFS depth cap (default 4, must be 1–8).
    - max_occurrences  : total occurrence cap (default 5000, must be 1–20000).
    - max_time_ms      : wall-clock cap in ms (default 5000, must be 1–30000).

    Response shape: see engine.multiway_causal_graph() docstring, plus:
    - meta.rule_id, meta.rule_notation, meta.init_state
    """
    if max_steps < 1 or max_steps > 8:
        raise HTTPException(400, "max_steps must be 1-8")
    if max_occurrences < 1 or max_occurrences > 20_000:
        raise HTTPException(400, "max_occurrences must be 1-20000")
    if max_time_ms < 1 or max_time_ms > 30_000:
        raise HTTPException(400, "max_time_ms must be 1-30000")
    return get_multiway_causal(rule_id, max_steps, max_occurrences, max_time_ms)


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
    """Submit a custom rule for computation.

    Returns immediately.  If the result is already cached, status is "done"
    and the full payload is included.  If computation is needed, status is
    "running" and the client should poll GET /api/jobs/{job_id} for progress.

    The job_id is the same as the persistent cache key, so
    GET /api/custom/{key} also works once status is "done".

    Response shape:
    - Always: {job_id, status, step, total_steps, elapsed_s}
    - When done: also includes {key, states, events, causalEdges, stats,
                                lineage, birthSteps, multiway}
    - When failed: also includes {error}
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

    # Cached — return immediately with full payload
    if key in CACHE:
        logger.debug("cache hit (memory) key=%s", key)
        return {"job_id": key, "status": "done", "key": key,
                "step": req.steps, "total_steps": req.steps, "elapsed_s": 0.0,
                **_strip_meta(CACHE[key])}
    disk = _disk_read(key)
    if disk is not None:
        CACHE[key] = disk
        return {"job_id": key, "status": "done", "key": key,
                "step": req.steps, "total_steps": req.steps, "elapsed_s": 0.0,
                **_strip_meta(disk)}

    # Already computing — return current job status (deduplicates concurrent requests)
    with _jobs_lock:
        existing = _jobs.get(key)
        if existing and existing["status"] in ("running", "queued"):
            logger.debug("dedup job request key=%s", key)
            return _job_response(key)

    # New job — register and start background thread
    now = _time.time()
    cancel_event = threading.Event()
    with _jobs_lock:
        _jobs[key] = {
            "status": "running",
            "step": 0,
            "total_steps": req.steps,
            "started_at": now,
            "heartbeat_at": now,
            "key": key,
            "cancel_event": cancel_event,
        }

    notation = req.notation
    init = req.init
    steps = req.steps
    parsed_lhs = parsed["lhs"]
    parsed_rhs = parsed["rhs"]

    def _run():
        def progress_cb(completed: int, total: int) -> None:
            _update_job(key, step=completed, heartbeat_at=_time.time())

        try:
            logger.info("job start key=%s notation=%r steps=%d", key, notation, steps)
            result = engine.evolve(
                init, parsed_lhs, parsed_rhs, steps,
                time_limit_ms=time_limit_ms,
                progress_cb=progress_cb,
                cancel_event=cancel_event,
            )
            # Cooperative cancellation: engine exited loop early
            if cancel_event.is_set():
                logger.info("job cancelled key=%s", key)
                _update_job(key, status="cancelled", heartbeat_at=_time.time())
                return
            lineage, birth_steps = engine.build_lineage(result["states"], result["events"])
            multiway = engine.compute_multiway(
                init, parsed_lhs, parsed_rhs,
                max_steps=_MW_MAX_STEPS,
                max_states=_MW_MAX_STATES,
                max_time_ms=_MW_MAX_TIME_MS,
            )
            payload = {
                "states": result["states"],
                "events": result["events"],
                "causalEdges": result["causal_edges"],
                "stats": result["stats"],
                "lineage": lineage,
                "birthSteps": birth_steps,
                "multiway": multiway,
                "_meta": {
                    "notation": notation,
                    "init": init,
                    "steps": steps,
                    "computed_at": _time.strftime("%Y-%m-%dT%H:%M:%SZ", _time.gmtime()),
                },
            }
            _disk_write(key, payload)
            CACHE[key] = payload
            _update_job(key, status="done", step=steps, heartbeat_at=_time.time(),
                        result=payload)
            logger.info("job done key=%s states=%d", key, len(payload["states"]))
        except Exception as e:
            logger.error("job failed key=%s error=%s", key, e, exc_info=True)
            _update_job(key, status="failed", error=str(e), heartbeat_at=_time.time())

    threading.Thread(target=_run, daemon=True).start()

    return {
        "job_id": key,
        "status": "running",
        "step": 0,
        "total_steps": req.steps,
        "elapsed_s": 0.0,
    }


class MultiwayCausalRequest(BaseModel):
    notation: str
    init: list[list[int]]
    max_steps: int = 4
    max_occurrences: int = 5000
    max_time_ms: int = 5000


@app.post("/api/custom/multiway-causal")
def run_custom_multiway_causal(req: MultiwayCausalRequest):
    """Compute the multiway causal graph for a custom rule (synchronous).

    Unlike POST /api/custom which dispatches a background job, this
    endpoint is synchronous — computation is bounded by req.max_time_ms
    (hard-capped at _MWCAUSAL_MAX_TIME_MS) so it always returns within
    the time budget.

    Request body:
    - notation       : rule in {{...}} → {{...}} notation (required).
    - init           : initial hypergraph state (required).
    - max_steps      : BFS depth cap (default 4, must be 1–8).
    - max_occurrences: total occurrence cap (default 5000, must be 1–20000).
    - max_time_ms    : wall-clock cap in ms (default 5000, must be 1–30000).

    Response shape: same as GET /api/rules/{id}/multiway-causal, plus
    meta.rule_notation and meta.init_state.  Computation is never
    cached on disk; successive calls with identical parameters recompute.
    """
    if req.max_steps < 1 or req.max_steps > 8:
        raise HTTPException(400, "max_steps must be 1-8")
    if req.max_occurrences < 1 or req.max_occurrences > 20_000:
        raise HTTPException(400, "max_occurrences must be 1-20000")
    if req.max_time_ms < 1 or req.max_time_ms > 30_000:
        raise HTTPException(400, "max_time_ms must be 1-30000")

    parsed = engine.parse_notation(req.notation)
    if not parsed:
        raise HTTPException(400, "Cannot parse notation")
    if not parsed["lhs"] or not parsed["rhs"]:
        raise HTTPException(400, "LHS and RHS must be non-empty")
    if not req.init:
        raise HTTPException(400, "Initial state must be non-empty")

    effective_time_ms = min(req.max_time_ms, _MWCAUSAL_MAX_TIME_MS)

    logger.info(
        "custom multiway-causal notation=%r max_steps=%d max_occurrences=%d",
        req.notation, req.max_steps, req.max_occurrences,
    )
    try:
        result = engine.multiway_causal_graph(
            req.init, parsed["lhs"], parsed["rhs"],
            max_steps=req.max_steps,
            max_occurrences=req.max_occurrences,
            max_time_ms=effective_time_ms,
        )
    except Exception as e:
        logger.error("custom multiway-causal failed notation=%r error=%s", req.notation, e, exc_info=True)
        raise HTTPException(500, f"Computation failed: {e}")

    result["meta"] = {
        "rule_notation": req.notation,
        "init_state": req.init,
    }
    return result


@app.get("/api/jobs/{job_id}")
def get_job_status(job_id: str):
    """Poll the status of a custom rule computation job.

    Response shape (all states):
      {job_id, status, step, total_steps, elapsed_s}

    Status values:
    - "running"   : actively computing; step/total_steps show progress.
    - "done"      : complete; key + full payload fields are included.
    - "failed"    : computation error; error field is included.
    - "stale"     : no heartbeat for >45s — server likely restarted
                    mid-compute.  Client should show a retry prompt.
    - "cancelled" : job was cancelled via DELETE /api/jobs/{job_id}.

    A completed job can also be retrieved via GET /api/custom/{key}.
    """
    return _job_response(job_id)


@app.delete("/api/jobs/{job_id}", status_code=200)
def cancel_job(job_id: str):
    """Request cooperative cancellation of a running job.

    Sets the job's cancel_event so the engine exits after the current step.
    The job status transitions to "cancelled" once the thread observes the
    event (usually within one engine step, <1s for most rules).

    Response: {job_id, status}
    - 200 + {"job_id": ..., "status": "cancelling"} if the job was running.
    - 404 if the job_id is not known to this server process.
    - 409 if the job has already reached a terminal state (done/failed/cancelled/stale).
    """
    with _jobs_lock:
        job = _jobs.get(job_id)
        if job is None:
            # Not in active tracker — check cache (completed before cancel reached it)
            if job_id in CACHE or _disk_read(job_id) is not None:
                raise HTTPException(409, f"Job '{job_id}' is already done")
            raise HTTPException(404, f"Job '{job_id}' not found")
        if job["status"] != "running":
            raise HTTPException(409, f"Job '{job_id}' is already in terminal state: {job['status']}")
        cancel_ev = job.get("cancel_event")

    if cancel_ev is not None:
        cancel_ev.set()
    logger.info("cancel requested job_id=%s", job_id)
    return {"job_id": job_id, "status": "cancelling"}


# ── Extend endpoint ───────────────────────────────────────────────────

class ExtendRequest(BaseModel):
    key: str
    extra_steps: int = 1


def _merge_evolution(old: dict, ext: dict, old_steps: int, extra_steps: int) -> dict:
    """Merge an extension result into an existing cached evolution dict.

    Args:
        old:         Full cached payload (states, events, causalEdges, stats,
                     lineage, birthSteps, _meta).
        ext:         Result from engine.evolve() starting at old's last state,
                     with event IDs already offset via initial_ev_id.
        old_steps:   Number of steps in the old result (= len(old["states"])-1).
        extra_steps: Number of new steps requested.

    Returns a merged dict ready to be persisted (without lineage/birthSteps —
    those are rebuilt from the merged states+events by the caller).
    """
    # states: drop ext[0] (== old[-1], the shared boundary state)
    merged_states = old["states"] + ext["states"][1:]

    # events: straightforward append (IDs are already globally-offset)
    merged_events = old["events"] + ext["events"]

    # causal edges: append (ext edges already reference global IDs)
    old_cedges = old.get("causalEdges", old.get("causal_edges", []))
    merged_cedges = old_cedges + ext["causal_edges"]

    # stats: drop ext[0] (re-stat of the boundary state already in old),
    #        offset step numbers by old_steps
    new_stats = []
    for stat in ext["stats"][1:]:
        new_stats.append({**stat, "step": stat["step"] + old_steps})
    merged_stats = old["stats"] + new_stats

    return {
        "states": merged_states,
        "events": merged_events,
        "causal_edges": merged_cedges,
        "stats": merged_stats,
    }


@app.post("/api/extend")
def extend_cached_evolution(req: ExtendRequest):
    """Extend a cached custom-rule evolution by additional steps.

    Takes the last state of a cached result and continues the rewrite from
    there, producing a new cached result with (old_steps + extra_steps) total
    steps.  The new result is stored under a fresh cache key derived from
    the same notation/init/new_total_steps triple — identical to what
    POST /api/custom would produce if called with the larger step count,
    so the caches are interchangeable.

    Request body: {key: str, extra_steps: int = 1}
      key         — cache key of an existing custom rule result (custom-...).
                    Must have been stored with _meta (i.e. created via
                    POST /api/custom or POST /api/extend).
      extra_steps — number of additional rewrite steps to compute (1-10).

    Response: same shape as POST /api/custom.
      If the extended result is already cached: status "done" + full payload.
      Otherwise: status "running"; poll GET /api/jobs/{job_id} for progress.
    """
    if req.extra_steps < 1 or req.extra_steps > 10:
        raise HTTPException(400, "extra_steps must be 1-10")

    # Resolve source from cache
    src = CACHE.get(req.key)
    if src is None:
        src = _disk_read(req.key)
        if src is not None:
            CACHE[req.key] = src
    if src is None:
        raise HTTPException(404, f"Cache key '{req.key}' not found")

    meta = src.get("_meta")
    if not meta or not meta.get("notation"):
        # Built-in rules lack _meta; reconstruct it from the RULES list.
        builtin = next((r for r in RULES if r["id"] == req.key), None)
        if builtin:
            meta = {
                "notation": builtin["notation"],
                "init": builtin["init"],
                "steps": builtin["steps"],
            }
        else:
            raise HTTPException(422, "Source result has no _meta — cannot extend this rule via this endpoint")

    notation = meta["notation"]
    orig_init = meta["init"]
    old_steps = meta["steps"]
    new_total = old_steps + req.extra_steps

    if new_total > 30:
        raise HTTPException(400, f"Total steps after extension ({new_total}) would exceed 30")

    parsed = engine.parse_notation(notation)
    if not parsed:
        raise HTTPException(400, "Cannot re-parse notation from cached _meta")

    time_limit_ms = min(15_000, _HARD_MAX_TIME_MS)
    new_key = _custom_cache_key(notation, orig_init, new_total, time_limit_ms)

    # Already cached at the extended size
    ext_cached = CACHE.get(new_key) or _disk_read(new_key)
    if ext_cached is not None:
        CACHE[new_key] = ext_cached
        logger.debug("extend cache hit key=%s", new_key)
        return {"job_id": new_key, "status": "done", "key": new_key,
                "step": new_total, "total_steps": new_total, "elapsed_s": 0.0,
                **_strip_meta(ext_cached)}

    # Dedup: already extending
    with _jobs_lock:
        existing = _jobs.get(new_key)
        if existing and existing["status"] in ("running", "queued"):
            return _job_response(new_key)

    # Register new job
    now = _time.time()
    cancel_event = threading.Event()
    with _jobs_lock:
        _jobs[new_key] = {
            "status": "running",
            "step": 0,
            "total_steps": req.extra_steps,
            "started_at": now,
            "heartbeat_at": now,
            "key": new_key,
            "cancel_event": cancel_event,
        }

    # Capture locals for thread closure
    src_snapshot = src          # full old payload (already loaded above)
    extra = req.extra_steps
    parsed_lhs = parsed["lhs"]
    parsed_rhs = parsed["rhs"]

    def _run_extend():
        def progress_cb(completed: int, total: int) -> None:
            _update_job(new_key, step=completed, heartbeat_at=_time.time())

        try:
            last_state = src_snapshot["states"][-1]

            # Build flat_events list from old serialised events for causal linkage
            old_flat: list[dict] = [
                ev for step_evts in src_snapshot.get("events", [])
                for ev in step_evts
            ]
            initial_ev_id = len(old_flat)

            logger.info(
                "extend job start key=%s base=%s old_steps=%d extra=%d",
                new_key, req.key, old_steps, extra,
            )
            ext_result = engine.evolve(
                last_state, parsed_lhs, parsed_rhs, extra,
                time_limit_ms=time_limit_ms,
                progress_cb=progress_cb,
                cancel_event=cancel_event,
                initial_ev_id=initial_ev_id,
                initial_flat_events=old_flat,
            )

            if cancel_event.is_set():
                logger.info("extend job cancelled key=%s", new_key)
                _update_job(new_key, status="cancelled", heartbeat_at=_time.time())
                return

            merged = _merge_evolution(src_snapshot, ext_result, old_steps, extra)
            lineage, birth_steps = engine.build_lineage(merged["states"], merged["events"])
            multiway = engine.compute_multiway(
                orig_init, parsed_lhs, parsed_rhs,
                max_steps=_MW_MAX_STEPS,
                max_states=_MW_MAX_STATES,
                max_time_ms=_MW_MAX_TIME_MS,
            )
            payload = {
                "states": merged["states"],
                "events": merged["events"],
                "causalEdges": merged["causal_edges"],
                "stats": merged["stats"],
                "lineage": lineage,
                "birthSteps": birth_steps,
                "multiway": multiway,
                "_meta": {
                    "notation": notation,
                    "init": orig_init,
                    "steps": new_total,
                    "computed_at": _time.strftime("%Y-%m-%dT%H:%M:%SZ", _time.gmtime()),
                    "extended_from": req.key,
                },
            }
            _disk_write(new_key, payload)
            CACHE[new_key] = payload
            _update_job(new_key, status="done", step=extra, heartbeat_at=_time.time(),
                        result=payload)
            logger.info("extend job done key=%s states=%d", new_key, len(payload["states"]))
        except Exception as e:
            logger.error("extend job failed key=%s error=%s", new_key, e, exc_info=True)
            _update_job(new_key, status="failed", error=str(e), heartbeat_at=_time.time())

    threading.Thread(target=_run_extend, daemon=True).start()

    return {
        "job_id": new_key,
        "status": "running",
        "step": 0,
        "total_steps": req.extra_steps,
        "elapsed_s": 0.0,
    }


@app.get("/api/custom/{key}")
def recall_custom_rule(key: str):
    """Return previously computed custom rule data by cache key.

    Does not recompute; 404 if the key is not in the cache.
    """
    if key in CACHE:
        logger.debug("cache hit (memory) key=%s", key)
        return {"key": key, **_strip_meta(CACHE[key])}
    disk = _disk_read(key)
    if disk is not None:
        CACHE[key] = disk
        return {"key": key, **_strip_meta(disk)}
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
