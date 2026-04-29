"""Hypergraph rewriting engine — pure Python implementation."""
from __future__ import annotations
import itertools
import math
import threading
from collections import defaultdict
from typing import Optional

# ── Cache version ─────────────────────────────────────────────────────
# Bump this constant when the engine's output semantics change (e.g. after
# fixing estimate_dimension in §5.4).  The cache directory path includes this
# value so old files remain on disk but are no longer read.
# v1 → v2: §5.4 dimension fix — incidence-based BFS replaces clique projection.
#   Ternary hyperedges (Rules 4, 5) produce different (more correct) estimates.
CACHE_VERSION = "v2"

# ── helpers ──────────────────────────────────────────────────────────
Edge = list[int]
Hypergraph = list[Edge]

# Thread-local node counter — each request/thread gets its own counter so
# concurrent calls to evolve() or compute_multiway() cannot race.
_tls = threading.local()

def reset(start: int = 0):
    _tls._next_node = start + 1

def fresh() -> int:
    n = _tls._next_node
    _tls._next_node += 1
    return n

# ── pattern matching (undirected) ────────────────────────────────────
from functools import lru_cache

@lru_cache(maxsize=4096)
def _edge_perms_cached(e_tuple: tuple) -> list[list[int]]:
    return [list(p) for p in set(itertools.permutations(e_tuple))]

def _edge_perms(e: Edge) -> list[list[int]]:
    return _edge_perms_cached(tuple(e))

def find_matches(hyp: Hypergraph, pattern: list[list[str]]) -> list[tuple[list[int], dict]]:
    results: list[tuple[list[int], dict]] = []

    # Pre-index edges by length for fast lookup
    by_len: dict[int, list[int]] = defaultdict(list)
    for i, e in enumerate(hyp):
        by_len[len(e)].append(i)

    # Pre-compute permutations for each edge (cached)
    edge_perms_cache: dict[int, list[list[int]]] = {}
    for i, e in enumerate(hyp):
        edge_perms_cache[i] = _edge_perms(e)

    def rec(pi: int, matched: list[int], binding: dict, used: set):
        if pi == len(pattern):
            results.append((matched[:], dict(binding)))
            return
        pe = pattern[pi]
        pe_len = len(pe)
        candidates = by_len.get(pe_len, [])
        for i in candidates:
            if i in used:
                continue
            for perm in edge_perms_cache[i]:
                nb = dict(binding)
                ok = True
                for j in range(pe_len):
                    var = pe[j]
                    val = perm[j]
                    bound = nb.get(var)
                    if bound is not None:
                        if bound != val:
                            ok = False
                            break
                    else:
                        nb[var] = val
                if not ok:
                    continue
                matched.append(i)
                used.add(i)
                rec(pi + 1, matched, nb, used)
                used.discard(i)
                matched.pop()

    rec(0, [], {}, set())
    # deduplicate
    seen = set()
    deduped = []
    for mi, bind in results:
        key = (tuple(mi), tuple(sorted(bind.items())))
        if key not in seen:
            seen.add(key)
            deduped.append((mi, bind))
    return deduped

def rule_new_vars(lhs: list[list[str]], rhs: list[list[str]]) -> set[str]:
    lv = {v for e in lhs for v in e}
    return {v for e in rhs for v in e} - lv

# ── single-match application ─────────────────────────────────────────
def apply_rule_once(hyp: Hypergraph, lhs, rhs, match_idx: int):
    matches = find_matches(hyp, lhs)
    if match_idx >= len(matches):
        return None
    mi, binding = matches[match_idx]
    nv = rule_new_vars(lhs, rhs)
    for v in nv:
        binding[v] = fresh()
    matched = set(mi)
    remaining = [e[:] for i, e in enumerate(hyp) if i not in matched]
    produced = [[binding[v] for v in re] for re in rhs]
    return {
        "state": remaining + produced,
        "event": {"consumed": [hyp[i] for i in mi], "produced": produced},
    }

# ── greedy (non-overlapping) application ──────────────────────────────
def apply_all_non_overlapping(hyp: Hypergraph, lhs, rhs):
    matches = find_matches(hyp, lhs)
    used = set()
    selected = []
    for mi, bind in matches:
        if any(i in used for i in mi):
            continue
        selected.append((mi, bind))
        used.update(mi)
    if not selected:
        return hyp, []

    nv = rule_new_vars(lhs, rhs)
    all_matched = {i for mi, _ in selected for i in mi}
    remaining = [e[:] for i, e in enumerate(hyp) if i not in all_matched]
    events = []
    new_edges = []
    for mi, bind in selected:
        for v in nv:
            bind[v] = fresh()
        produced = [[bind[v] for v in re] for re in rhs]
        new_edges.extend(produced)
        events.append({"consumed": [hyp[i] for i in mi], "produced": produced})
    return remaining + new_edges, events

# ── full evolution ────────────────────────────────────────────────────
import time

def evolve(init_hyp: Hypergraph, lhs, rhs, steps: int, time_limit_ms: int = 30000):
    max_n = max((n for e in init_hyp for n in e), default=0)
    reset(max_n)

    t0 = time.time()
    states = [[e[:] for e in init_hyp]]
    all_events: list[list[dict]] = []
    ev_id = 0
    current = init_hyp
    causal_edges = []
    flat_events = []

    for s in range(steps):
        if time_limit_ms and (time.time() - t0) * 1000 > time_limit_ms:
            break
        nxt, step_evts = apply_all_non_overlapping(current, lhs, rhs)
        for ev in step_evts:
            ev["id"] = ev_id
            ev_id += 1

        # causal edges — use tuple sets for fast lookup
        for ev in step_evts:
            c_set = {tuple(e) for e in ev["consumed"]}
            for prev in flat_events:
                if any(tuple(e) in c_set for e in prev["produced"]):
                    causal_edges.append([prev["id"], ev["id"]])

        flat_events.extend(step_evts)
        all_events.append([{"id": e["id"], "consumed": e["consumed"], "produced": e["produced"]} for e in step_evts])
        states.append([e[:] for e in nxt])
        current = nxt

    stats = []
    for i, st in enumerate(states):
        ns = set(n for e in st for n in e)
        dim = estimate_dimension(st)
        stats.append({"step": i, "num_nodes": len(ns), "num_edges": len(st), "estimated_dimension": dim})

    return {"states": states, "events": all_events, "causal_edges": causal_edges, "stats": stats}

# ── dimension estimate ────────────────────────────────────────────────

def _hyperedge_bfs(
    state: Hypergraph,
    incidence: dict,
    seed: int,
) -> list[int]:
    """BFS using hyperedge traversals.

    One "step" traverses a single hyperedge, reaching all nodes it contains.
    This is the metric used in the Wolfram Physics Project for geodesic-ball
    estimation and emergent-dimension extraction.

    Returns cumulative ball sizes B[0..r] where B[r] = number of nodes
    reachable from seed in at most r traversals.
    """
    visited_nodes: set[int] = {seed}
    visited_edges: set[int] = set()
    frontier: list[int] = [seed]
    ball_sizes: list[int] = [1]          # B[0] = 1 (seed only)
    while frontier:
        new_nodes: list[int] = []
        for node in frontier:
            for edge_idx in incidence.get(node, []):
                if edge_idx in visited_edges:
                    continue
                visited_edges.add(edge_idx)
                for nbr in state[edge_idx]:
                    if nbr not in visited_nodes:
                        visited_nodes.add(nbr)
                        new_nodes.append(nbr)
        if not new_nodes:
            break
        frontier = new_nodes
        ball_sizes.append(len(visited_nodes))
    return ball_sizes


def estimate_dimension(st: Hypergraph) -> Optional[float]:
    """Estimate the effective dimension of a hypergraph via geodesic ball growth.

    Uses true hyperedge distance (one traversal crosses one hyperedge, reaching
    all its nodes) rather than an adjacency-projection (clique expansion).  For
    undirected hyperedges both metrics produce identical ball volumes, so this is
    a conceptual improvement (O(k) per edge vs O(k²)) without changing results.
    """
    if len(st) < 5:
        return None
    node_set = set(n for e in st for n in e)
    nodes = sorted(node_set)
    if len(nodes) < 8:
        return None

    # Build incidence index: node → list of hyperedge indices (O(sum of arities))
    incidence: dict[int, list[int]] = defaultdict(list)
    for idx, e in enumerate(st):
        for node in e:
            incidence[node].append(idx)

    num_seeds = min(5, len(nodes))
    seed_step = max(1, len(nodes) // num_seeds)
    all_balls: list[list[int]] = []

    for si in range(num_seeds):
        seed = nodes[si * seed_step]
        all_balls.append(_hyperedge_bfs(st, incidence, seed))

    max_r = min(len(b) for b in all_balls) - 1
    if max_r < 2:
        return None

    xs, ys = [], []
    for r in range(1, max_r + 1):
        avg_count = sum(b[r] for b in all_balls) / len(all_balls)
        if avg_count > 1:
            xs.append(math.log(r))
            ys.append(math.log(avg_count))
    if len(xs) < 2:
        return None

    n = len(xs)
    mx = sum(xs) / n
    my = sum(ys) / n
    num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    den = sum((x - mx) ** 2 for x in xs)
    return round(num / den, 4) if den > 0 else None

# ── canonical hash (isomorphism-invariant) ────────────────────────────
def canonical_hash(hyp: Hypergraph) -> str:
    if not hyp:
        return "[]"
    node_set = set(n for e in hyp for n in e)
    nodes = sorted(node_set)
    N, E = len(nodes), len(hyp)

    def refine(color0: dict) -> dict:
        c = dict(color0)
        for _ in range(10):
            sigs = {}
            for n in nodes:
                es = []
                for e in hyp:
                    cnt = e.count(n)
                    if not cnt:
                        continue
                    es.append(f"{cnt}/{len(e)}:{','.join(str(c[x]) for x in sorted(e, key=lambda x: c[x]))}")
                es.sort()
                sigs[n] = f"{c[n]}|{';'.join(es)}"
            uniq = sorted(set(sigs.values()))
            idx = {v: i for i, v in enumerate(uniq)}
            changed = False
            for n in nodes:
                nv = idx[sigs[n]]
                if nv != c[n]:
                    changed = True
                c[n] = nv
            if not changed:
                break
        return c

    def edge_str(relabel: dict) -> str:
        edges = [sorted(relabel[n] for n in e) for e in hyp]
        edges.sort()
        return str(edges)

    init_c = defaultdict(int)
    for e in hyp:
        for n in e:
            init_c[n] += 1
    color = refine(dict(init_c))

    cells: dict[int, list] = defaultdict(list)
    for n in nodes:
        cells[color[n]].append(n)
    color_order = sorted(cells.keys())

    total_perms = 1
    for c in color_order:
        f = 1
        for i in range(2, len(cells[c]) + 1):
            f *= i
        total_perms *= f
        if total_perms > 5000:
            break

    if total_perms <= 5000:
        best = None
        cell_arrays = [cells[c] for c in color_order]

        def try_cells(ci, relabel, next_lbl):
            nonlocal best
            if ci >= len(cell_arrays):
                s = edge_str(relabel)
                if best is None or s < best:
                    best = s
                return
            for perm in itertools.permutations(cell_arrays[ci]):
                r = dict(relabel)
                lbl = next_lbl
                for n in perm:
                    r[n] = lbl
                    lbl += 1
                try_cells(ci + 1, r, lbl)

        try_cells(0, {}, 0)
        return f"{N}:{E}:{best}"

    # fallback: individualization-refinement with timeout
    t0 = time.time()

    def solve(col, assigned, next_lbl):
        if time.time() - t0 > 0.5:
            # Deterministic fallback: ignore partial recursive state and assign
            # all nodes by the fixed post-initial-refinement coloring (closed
            # over from the outer scope) so two calls with identical input
            # always produce the same hash string even when the timeout fires
            # at different recursion depths.
            r = {}
            lbl = 0
            for n in sorted(nodes, key=lambda n: (color[n], n)):
                r[n] = lbl
                lbl += 1
            return edge_str(r)

        unassigned = [n for n in nodes if n not in assigned]
        if not unassigned:
            return edge_str(assigned)

        by_c: dict[int, list] = defaultdict(list)
        for n in unassigned:
            by_c[col[n]].append(n)
        c_keys = sorted(by_c.keys())

        r = dict(assigned)
        lbl = next_lbl
        ambig = None
        for c in c_keys:
            if len(by_c[c]) == 1:
                r[by_c[c][0]] = lbl
                lbl += 1
            else:
                ambig = (c, by_c[c])
                break
        if ambig is None:
            return edge_str(r)

        best = None
        for n in ambig[1]:
            nr = dict(r)
            nr[n] = lbl
            nc = dict(col)
            nc[n] = max(col[m] for m in nodes) + 1
            refined = refine(nc)
            candidate = solve(refined, nr, lbl + 1)
            if best is None or candidate < best:
                best = candidate
        return best

    canonical = solve(color, {}, 0)
    return f"{N}:{E}:{canonical}"

# ── lineage maps ──────────────────────────────────────────────────────
def build_lineage(states, events):
    """Build edge lineage and birth-step maps.

    Uses reverse-index maps (edge_tuple → [idx, ...]) built once per step so
    that consumed/produced/surviving lookups are O(1) per edge rather than
    O(|edges|) — reducing overall complexity from O(events × edges²) to
    O(events × arity + edges).
    """
    lineage: dict[str, list[str]] = {}

    for step in range(len(events)):
        prev_state = states[step]
        next_state = states[step + 1] if step + 1 < len(states) else None
        if not next_state:
            continue

        # Reverse-index maps: edge_tuple → list of indices (handles duplicates)
        prev_idx: dict[tuple, list[int]] = defaultdict(list)
        for i, e in enumerate(prev_state):
            prev_idx[tuple(e)].append(i)
        next_idx: dict[tuple, list[int]] = defaultdict(list)
        for i, e in enumerate(next_state):
            next_idx[tuple(e)].append(i)

        # Track which indices have been claimed to avoid double-assignment
        used_prev: set[int] = set()
        used_next: set[int] = set()

        for ev in events[step]:
            consumed_indices: list[int] = []
            for consumed in ev["consumed"]:
                ct = tuple(consumed)
                for i in prev_idx.get(ct, []):
                    if i not in used_prev:
                        consumed_indices.append(i)
                        used_prev.add(i)
                        break
            produced_indices: list[int] = []
            for produced in ev["produced"]:
                pt = tuple(produced)
                for i in next_idx.get(pt, []):
                    if i not in used_next:
                        produced_indices.append(i)
                        used_next.add(i)
                        break
            for ci in consumed_indices:
                key = f"{step}:{ci}"
                if key not in lineage:
                    lineage[key] = []
                for pi in produced_indices:
                    lineage[key].append(f"{step+1}:{pi}")

        # Surviving edges — prev edges not consumed, matched to same content in next
        for i, e in enumerate(prev_state):
            if i in used_prev:
                continue
            et = tuple(e)
            for j in next_idx.get(et, []):
                if j not in used_next:
                    key = f"{step}:{i}"
                    if key not in lineage:
                        lineage[key] = []
                    lineage[key].append(f"{step+1}:{j}")
                    used_next.add(j)
                    break

    # birth steps
    birth_steps: list[list[int]] = []
    if states:
        birth_steps.append([0] * len(states[0]))
    for step in range(1, len(states)):
        bs = [step] * len(states[step])
        prev_tuples = [tuple(e) for e in states[step - 1]] if step - 1 < len(states) else []
        cur_tuples = [tuple(e) for e in states[step]]
        for pi in range(len(prev_tuples)):
            key = f"{step-1}:{pi}"
            prev_birth = (
                birth_steps[step - 1][pi]
                if step - 1 < len(birth_steps) and pi < len(birth_steps[step - 1])
                else 0
            )
            for child in lineage.get(key, []):
                cs, ci = child.split(":")
                cs, ci = int(cs), int(ci)
                if cs == step and ci < len(bs):
                    # Surviving edge keeps its original birth step
                    if pi < len(prev_tuples) and ci < len(cur_tuples) and prev_tuples[pi] == cur_tuples[ci]:
                        bs[ci] = prev_birth
        birth_steps.append(bs)

    return lineage, birth_steps

# ── multiway computation ──────────────────────────────────────────────
def compute_multiway(init_state: Hypergraph, lhs, rhs, max_steps=4, max_states=300, max_time_ms=3000):
    max_n = max((n for e in init_state for n in e), default=0)
    reset(max_n)

    init_hash = canonical_hash(init_state)
    mw_states = {init_hash: {"state": init_state, "step": 0}}
    mw_edges = []
    branches = {init_hash: init_state}
    t0 = time.time()
    event_id = 0

    for step in range(1, max_steps + 1):
        new_branches = {}
        too_many = False

        for parent_hash, parent_state in branches.items():
            matches = find_matches(parent_state, lhs)
            if not matches:
                new_branches[parent_hash] = parent_state
                continue

            for mi_idx in range(len(matches)):
                if len(mw_states) >= max_states or (time.time() - t0) * 1000 > max_time_ms:
                    too_many = True
                    break

                result = apply_rule_once([e[:] for e in parent_state], lhs, rhs, mi_idx)
                if not result:
                    continue
                child_hash = canonical_hash(result["state"])
                if child_hash not in mw_states:
                    mw_states[child_hash] = {"state": result["state"], "step": step}
                new_branches[child_hash] = result["state"]
                ev = {"id": event_id, "consumed": result["event"]["consumed"], "produced": result["event"]["produced"]}
                event_id += 1
                mw_edges.append({"from": parent_hash, "to": child_hash, "event": ev})

            if too_many:
                break
        branches = new_branches
        if too_many:
            break

    # default path: follow first child at each step
    default_path_hashes = {init_hash}
    default_path_event_ids = set()
    current_hash = init_hash
    for step in range(1, max_steps + 1):
        child = next((e for e in mw_edges if e["from"] == current_hash and mw_states.get(e["to"], {}).get("step") == step), None)
        if not child:
            break
        default_path_hashes.add(child["to"])
        default_path_event_ids.add(child["event"]["id"])
        current_hash = child["to"]

    return {
        "states": {h: {"state": info["state"], "step": info["step"]} for h, info in mw_states.items()},
        "edges": mw_edges,
        "initHash": init_hash,
        "defaultPathEventIds": list(default_path_event_ids),
        "defaultPathHashes": list(default_path_hashes),
    }

# ── notation parser ───────────────────────────────────────────────────
import re

def parse_notation(notation: str):
    arrow = "\u2192" if "\u2192" in notation else "->"
    if arrow not in notation:
        return None
    l_str, r_str = notation.split(arrow, 1)

    def parse_side(s: str):
        s = s.strip()
        # Strip a single outer wrapper only when it encloses multiple edges,
        # i.e. the notation uses {{e1},{e2},...} style.  A bare single-edge
        # like {x,y} must NOT be stripped or the regex finds nothing.
        if s.startswith("{{"):
            s = s[1:-1]
        edges = []
        for m in re.finditer(r"\{([^}]+)\}", s):
            edges.append([v.strip() for v in m.group(1).split(",")])
        return edges

    return {"lhs": parse_side(l_str), "rhs": parse_side(r_str)}
