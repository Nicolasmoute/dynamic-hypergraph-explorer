"""FastAPI server for Dynamic Hypergraph Explorer."""
from __future__ import annotations
import json
import threading
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

# ── Preloaded rules ───────────────────────────────────────────────────
RULES = [
    {
        "id": "rule1", "name": "Signature Rule",
        "notation": "{{x,y},{x,z}} \u2192 {{x,z},{x,w},{y,w},{z,w}}",
        "desc": "Two edges sharing a source become four, creating a new node. Generates emergent 2D space.",
        "tag": "2D Space", "tagClass": "tag-2d",
        "init": [[0, 0], [0, 0]],
        "steps": 12,
    },
    {
        "id": "rule2", "name": "Path Subdivision",
        "notation": "{{x,y},{y,z}} \u2192 {{x,y},{y,w},{w,z}}",
        "desc": "Finds a 2-edge path and inserts a new node in the middle. Refines 1D ring structure.",
        "tag": "1D Ring", "tagClass": "tag-1d",
        "init": [[0, 1], [1, 2], [2, 3], [3, 4], [4, 0]],
        "steps": 17,
    },
    {
        "id": "rule3", "name": "Binary Tree Growth",
        "notation": "{{x,y}} \u2192 {{x,y},{y,z}}",
        "desc": "Every edge spawns a new edge from its target. Exponential doubling every step.",
        "tag": "Exponential", "tagClass": "tag-tree",
        "init": [[0, 1]],
        "steps": 12,
    },
    {
        "id": "rule4", "name": "Sierpinski Hyperedge",
        "notation": "{{x,y,z}} \u2192 {{x,u,w},{y,v,u},{z,w,v}}",
        "desc": "Ternary hyperedges split into 3 new ones. Fractal structure, dimension \u2248 1.4.",
        "tag": "Fractal", "tagClass": "tag-fractal",
        "init": [[0, 1, 2]],
        "steps": 7,
    },
    {
        "id": "rule5", "name": "Chain Rewrite",
        "notation": "{{x,y,z},{z,u,v}} \u2192 {{y,z,u},{v,w,x},{w,y,v}}",
        "desc": "Two chain-linked ternary hyperedges fuse into three. Unchained edges survive across steps.",
        "tag": "Partial", "tagClass": "tag-mixed",
        "init": [[0,1,2],[2,3,4],[4,5,6],[6,7,8],[8,9,0],[1,3,5],[5,7,9],[9,1,3]],
        "steps": 14,
    },
]

# Cache for computed data
CACHE: dict[str, dict] = {}
_compute_locks: dict[str, threading.Lock] = {}
_locks_lock = threading.Lock()

def _get_lock(key: str) -> threading.Lock:
    with _locks_lock:
        if key not in _compute_locks:
            _compute_locks[key] = threading.Lock()
        return _compute_locks[key]

def get_rule_data(rule_id: str) -> dict:
    """Get or compute cached rule data."""
    if rule_id in CACHE:
        return CACHE[rule_id]

    lock = _get_lock(rule_id)
    with lock:
        if rule_id in CACHE:
            return CACHE[rule_id]

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
        CACHE[rule_id] = data
        return data

# ── App + lifespan ────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Precompute all rule data in a background thread at startup."""
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

@app.get("/api/rules")
def list_rules():
    """List all available rules."""
    return [{"id": r["id"], "name": r["name"], "notation": r["notation"],
             "desc": r["desc"], "tag": r["tag"], "tagClass": r["tagClass"]}
            for r in RULES]

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
def get_multiway(rule_id: str):
    """Get multiway system for a rule."""
    cache_key = f"{rule_id}_multiway"
    if cache_key in CACHE:
        return CACHE[cache_key]

    lock = _get_lock(cache_key)
    with lock:
        if cache_key in CACHE:
            return CACHE[cache_key]

        rule = next((r for r in RULES if r["id"] == rule_id), None)
        if not rule:
            raise HTTPException(404, f"Rule {rule_id} not found")

        parsed = engine.parse_notation(rule["notation"])
        result = engine.compute_multiway(rule["init"], parsed["lhs"], parsed["rhs"])
        CACHE[cache_key] = result
        return result

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


class CustomRuleRequest(BaseModel):
    notation: str
    init: list[list[int]]
    steps: int = 8

@app.post("/api/custom")
def run_custom_rule(req: CustomRuleRequest):
    """Run a custom rule and return evolution data."""
    if req.steps < 1 or req.steps > 20:
        raise HTTPException(400, "Steps must be 1-20")

    parsed = engine.parse_notation(req.notation)
    if not parsed:
        raise HTTPException(400, "Cannot parse notation")
    if not parsed["lhs"] or not parsed["rhs"]:
        raise HTTPException(400, "LHS and RHS must be non-empty")
    if not req.init:
        raise HTTPException(400, "Initial state must be non-empty")

    result = engine.evolve(req.init, parsed["lhs"], parsed["rhs"], req.steps, time_limit_ms=15000)
    lineage, birth_steps = engine.build_lineage(result["states"], result["events"])

    # multiway
    multiway = engine.compute_multiway(req.init, parsed["lhs"], parsed["rhs"])

    return {
        "states": result["states"],
        "events": result["events"],
        "causalEdges": result["causal_edges"],
        "stats": result["stats"],
        "lineage": lineage,
        "birthSteps": birth_steps,
        "multiway": multiway,
    }


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

