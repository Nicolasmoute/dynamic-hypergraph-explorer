"""Hypergraph rewriting engine — pure Python implementation."""
from __future__ import annotations
import itertools
import math
import threading
from collections import Counter, defaultdict
from typing import Optional

# ── Cache version ─────────────────────────────────────────────────────
# Bump this constant when the engine's output semantics change (e.g. after
# fixing estimate_dimension in §5.4).  The cache directory path includes this
# value so old files remain on disk but are no longer read.
# v1 → v2: §5.4 dimension fix — incidence-based BFS replaces clique projection.
#   Ternary hyperedges (Rules 4, 5) produce different (more correct) estimates.
# v2 → v3: Phase B1/B2 — occurrence-based multiway BFS adds match_idx /
#   branch_path fields; multiway_causal_graph() payload uses occ_id as event id.
#   Old multiway cache entries lack these fields; must be invalidated.
# v3 → v4: multiway_causal_graph() adds a realized greedy-parallel causal
#   slice for red rendering while keeping occurrence-level events as green
#   multiway alternatives.
# v4 → v5: multiway_causal_graph() embeds red as the serial occurrence path
#   (`default_path_event_ids`) instead of emitting separate `r*` red records.
# v5 → v6: clarify that embedded red is the Single-History greedy path in
#   multiway occurrence-ID space.
# v6 → v7: default_path_event_ids now embeds the full Single-History greedy
#   parallel event set, not one serial child per depth.
# v7 → v8: the greedy Single-History serial occurrence path is guaranteed to be
#   present in the multiway occurrence set before event/causal-edge construction.
# v8 → v9: multiway_causal_graph() adds explicit serial/layout metadata and
#   embeds red by private edge-instance identity instead of local shape fallback.
# v9 → v10: compute_multiway() adds aggregatedEdges, a canonical-labeled
#   event-role quotient with multiplicity over the existing concrete edges.
CACHE_VERSION = "v10"

# ── helpers ──────────────────────────────────────────────────────────
Edge = list[int]
Hypergraph = list[Edge]

# Thread-local storage — node counter + cancel signal.
# Using thread-local for the cancel signal lets find_matches.rec() observe
# it without requiring signature changes on every inner function.
_tls = threading.local()

def reset(start: int = 0):
    _tls._next_node = start + 1

def fresh() -> int:
    n = _tls._next_node
    _tls._next_node += 1
    return n

def _is_cancelled() -> bool:
    """Return True if the current thread's cancel event has been set."""
    ev = getattr(_tls, "_cancel_ev", None)
    return ev is not None and ev.is_set()

# ── pattern matching (undirected) ────────────────────────────────────
from functools import lru_cache

@lru_cache(maxsize=131072)
def _edge_perms_cached(e_tuple: tuple) -> list[list[int]]:
    return [list(p) for p in set(itertools.permutations(e_tuple))]

def _edge_perms(e: Edge) -> list[list[int]]:
    return _edge_perms_cached(tuple(e))

def find_matches(hyp: Hypergraph, pattern: list[list[str]]) -> list[tuple[list[int], dict]]:
    results: list[tuple[list[int], dict]] = []

    # Pre-index edges by length — used when no variable is bound yet.
    by_len: dict[int, list[int]] = defaultdict(list)
    for i, e in enumerate(hyp):
        by_len[len(e)].append(i)

    # Node-to-edge-indices index — used for constrained pattern edges.
    # Mirrors the same optimisation in _find_matches_gen: reduces candidates
    # from O(E) to O(degree) once any variable in the pattern edge is bound.
    node_to_edges: dict[int, list[int]] = defaultdict(list)
    for i, e in enumerate(hyp):
        for n in e:
            node_to_edges[n].append(i)

    # Pre-compute permutations for each edge (cached)
    edge_perms_cache: dict[int, list[list[int]]] = {}
    for i, e in enumerate(hyp):
        edge_perms_cache[i] = _edge_perms(e)

    def rec(pi: int, matched: list[int], binding: dict, used: set):
        # Check cancel at every recursion level so a mid-step abort request
        # is observed promptly even when the graph is large (thousands of edges).
        if _is_cancelled():
            return
        if pi == len(pattern):
            results.append((matched[:], dict(binding)))
            return
        pe = pattern[pi]
        pe_len = len(pe)

        # Use node_to_edges for any bound variable — O(degree) candidates.
        best_candidates = None
        for var in pe:
            if var in binding:
                nc = node_to_edges.get(binding[var], [])
                if best_candidates is None or len(nc) < len(best_candidates):
                    best_candidates = nc
        candidates = best_candidates if best_candidates is not None else by_len.get(pe_len, [])

        for i in candidates:
            if i in used:
                continue
            if len(hyp[i]) != pe_len:
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

# ── Rec-1: lazy match generator ───────────────────────────────────────
def _find_matches_gen(hyp: Hypergraph, pattern: list[list[str]],
                       committed: "set[int] | None" = None):
    """Lazy generator variant of find_matches with node-index acceleration.

    Two key optimisations over the original eager find_matches:

    1. **Node→edges index** (primary speedup): for any pattern edge whose
       variables are already partially bound, we look up only edges that
       contain the required node values instead of scanning all edges of the
       right length.  This reduces candidate evaluation from O(E) to O(degree)
       per bound variable, typically cutting work by 100-1000× for densely
       connected hypergraphs.

    2. **Committed-set pruning** (secondary speedup for greedy caller): the
       caller (apply_all_non_overlapping) passes a *mutable* set of already-
       accepted edge indices.  The generator checks this set dynamically — so
       once an edge is committed, every future branch that would use it is
       pruned immediately, without requiring a generator restart.

    committed: optional mutable set of edge indices.  Updated externally by
    the caller between successive next() calls; changes are visible here
    because Python passes mutable objects by reference.

    Deduplication is tracked via a seen-set so no duplicate match is yielded.
    Cancel-check is inherited from the shared _is_cancelled() thread-local.
    """
    if committed is None:
        committed = set()

    # Pre-index edges by length — used for unconstrained first pattern edge.
    by_len: dict[int, list[int]] = defaultdict(list)
    for i, e in enumerate(hyp):
        by_len[len(e)].append(i)

    # Node-to-edge-indices index — used for constrained subsequent edges.
    # Maps node value → list of edge indices that contain that node.
    node_to_edges: dict[int, list[int]] = defaultdict(list)
    for i, e in enumerate(hyp):
        for n in e:
            node_to_edges[n].append(i)

    # Pre-compute permutations for each edge (cached)
    edge_perms_cache: dict[int, list[list[int]]] = {}
    for i, e in enumerate(hyp):
        edge_perms_cache[i] = _edge_perms(e)

    seen: set = set()

    def rec(pi: int, matched: list[int], binding: dict, used: set):
        if _is_cancelled():
            return
        if pi == len(pattern):
            key = (tuple(matched), tuple(sorted(binding.items())))
            if key not in seen:
                seen.add(key)
                yield (matched[:], dict(binding))
            return
        pe = pattern[pi]
        pe_len = len(pe)

        # Choose the tightest candidate set available.
        # If any variable in the current pattern edge is already bound, use
        # node_to_edges[binding[var]] to restrict to edges containing that node
        # (O(degree) candidates) instead of all edges of the right length (O(E)).
        # Pick the bound variable whose node has the smallest edge list — that
        # gives the tightest filter.
        best_candidates: "list[int] | None" = None
        for var in pe:
            if var in binding:
                node_val = binding[var]
                node_cands = node_to_edges.get(node_val, [])
                if best_candidates is None or len(node_cands) < len(best_candidates):
                    best_candidates = node_cands
        candidates = best_candidates if best_candidates is not None else by_len.get(pe_len, [])

        for i in candidates:
            # Skip edges already consumed by this partial match (backtracking
            # guard) OR committed to a previously accepted non-overlapping match.
            if i in used or i in committed:
                continue
            # Length filter needed when coming from node_to_edges (mixed lengths).
            if len(hyp[i]) != pe_len:
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
                yield from rec(pi + 1, matched, nb, used)
                used.discard(i)
                matched.pop()

    yield from rec(0, [], {}, set())

# ── single-match application ─────────────────────────────────────────
def _apply_match(hyp: Hypergraph, lhs, rhs, mi: list[int], binding: dict):
    """Apply one preselected match without re-running match discovery."""
    nv = rule_new_vars(lhs, rhs)
    binding = dict(binding)
    for v in nv:
        binding[v] = fresh()
    matched = set(mi)
    remaining = [e[:] for i, e in enumerate(hyp) if i not in matched]
    produced = [[binding[v] for v in re] for re in rhs]
    return {
        "state": remaining + produced,
        "event": {"consumed": [hyp[i] for i in mi], "produced": produced},
    }


def apply_rule_once(hyp: Hypergraph, lhs, rhs, match_idx: int):
    matches = find_matches(hyp, lhs)
    if match_idx >= len(matches):
        return None
    mi, binding = matches[match_idx]
    return _apply_match(hyp, lhs, rhs, mi, binding)

# ── greedy (non-overlapping) application — Rec-1 lazy early-exit ─────
def apply_all_non_overlapping(hyp: Hypergraph, lhs, rhs):
    """Greedily select non-overlapping matches and apply the rule.

    Two code paths:

    **Single-edge LHS fast path** (rules 3, 4 and any other 1-edge pattern):
    Every hyperedge matches independently — they can never overlap.  We iterate
    directly, picking the first permutation per edge, avoiding generator
    overhead and the 5 wasted permutation checks per ternary edge.  Cancel is
    checked every 1 024 edges.

    **Multi-edge LHS general path** (rules 1, 2, 5, custom rules):
    Rec-1 lazy generator with node-index acceleration + committed-set pruning.
    See _find_matches_gen for details.
    """
    nv = rule_new_vars(lhs, rhs)
    pe_len = len(lhs[0]) if lhs else 0

    # ── Single-edge fast path ─────────────────────────────────────────
    if len(lhs) == 1:
        pe = lhs[0]
        pe_len_1 = len(pe)
        events: list = []
        new_edges: list = []
        consumed_indices: set[int] = set()
        for i, e in enumerate(hyp):
            if len(e) != pe_len_1:
                continue
            # Cooperative cancellation — check every 1024 edges.
            if i & 1023 == 0 and _is_cancelled():
                break
            perms = _edge_perms(e)
            # Use the first permutation to form the binding.
            bind = dict(zip(pe, perms[0]))
            for v in nv:
                bind[v] = fresh()
            produced = [[bind[v] for v in re] for re in rhs]
            new_edges.extend(produced)
            events.append({"consumed": [e[:]], "produced": produced})
            consumed_indices.add(i)
        if not events:
            return hyp, []
        remaining = [e[:] for i, e in enumerate(hyp) if i not in consumed_indices]
        return remaining + new_edges, events

    # ── Multi-edge general path ───────────────────────────────────────
    # Count free edges by length; updated as matches are accepted.
    free_by_len: Counter = Counter(len(e) for e in hyp)
    # Per-length demand for one complete LHS match.
    lhs_needs: Counter = Counter(len(pe) for pe in lhs)

    # Quick feasibility check before entering the generator at all.
    if any(free_by_len[L] < lhs_needs[L] for L in lhs_needs):
        return hyp, []

    # committed is shared with the generator by reference — updates here are
    # visible inside the generator's next() call, so it skips committed edges
    # when resuming.  This is the key mechanism behind the early-exit speedup:
    # once an edge is committed, all branches exploring it are pruned.
    committed: set[int] = set()
    selected: list = []

    for mi, bind in _find_matches_gen(hyp, lhs, committed=committed):
        # Safety guard: generator may still yield a match whose first pattern
        # edge was committed AFTER the generator started the inner loop for it.
        if any(i in committed for i in mi):
            continue
        selected.append((mi, bind))
        committed.update(mi)
        # Deduct consumed edges from the free pool.
        for i in mi:
            free_by_len[len(hyp[i])] -= 1
        # Early-exit: if any required length is exhausted, no more full matches.
        if any(free_by_len[L] < lhs_needs[L] for L in lhs_needs):
            break

    if not selected:
        return hyp, []

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

def evolve(
    init_hyp: Hypergraph,
    lhs,
    rhs,
    steps: int,
    time_limit_ms: int = 30000,
    progress_cb=None,
    cancel_event=None,
    initial_ev_id: int = 0,
    initial_flat_events: Optional[list] = None,
):
    """Evolve init_hyp for up to *steps* rewrite steps.

    Args:
        init_hyp:             Initial hypergraph state.
        lhs:                  Parsed left-hand-side pattern.
        rhs:                  Parsed right-hand-side pattern.
        steps:                Maximum number of steps to run.
        time_limit_ms:        Wall-clock cutoff in milliseconds (0 = unlimited).
        progress_cb:          Optional callable(completed_steps: int, total_steps: int).
                              Called after each step completes. Exceptions are silently
                              swallowed so a buggy callback never crashes the engine.
        cancel_event:         Optional threading.Event.  When set, the loop exits cleanly
                              within the current step — the signal is propagated into
                              find_matches via a thread-local so backtracking search
                              yields promptly rather than waiting for the whole step.
        initial_ev_id:        Event ID counter starting value (use total events from a prior
                              run when extending a cached result so IDs stay globally unique).
        initial_flat_events:  Flat list of prior-run events to seed the causal-edge lookup.
                              Enables correct cross-boundary causal edges when extending.
    """
    max_n = max((n for e in init_hyp for n in e), default=0)
    reset(max_n)

    # Expose cancel signal via thread-local so find_matches.rec() can observe
    # it without needing a signature change.  Always clear in finally.
    _tls._cancel_ev = cancel_event

    t0 = time.time()
    states = [[e[:] for e in init_hyp]]
    all_events: list[list[dict]] = []
    ev_id = initial_ev_id
    current = init_hyp
    causal_edges = []

    # Rec-2: causal index — O(1) lookup replaces O(N) linear scan over flat_events.
    # Maps edge_tuple → ordered list of event IDs that produced instances of that
    # edge content (FIFO queue).  A list is required — not a plain dict — because
    # multiple events can produce edges with identical node-ID content (duplicate
    # hyperedge instances).  Using a single int would overwrite earlier producers,
    # mis-attributing every consumer of those duplicates to the last writer.
    # On consume we pop the front (FIFO); on produce we append to the back.
    # Seeded from initial_flat_events so cross-boundary causal links are correct
    # when extending a cached evolution (extend endpoint path).
    produced_index: dict[tuple, list[int]] = defaultdict(list)
    if initial_flat_events:
        for pev in initial_flat_events:
            for edge in pev["produced"]:
                produced_index[tuple(edge)].append(pev["id"])

    try:
        for s in range(steps):
            # Cooperative cancellation — check before each step
            if _is_cancelled():
                break
            if time_limit_ms and (time.time() - t0) * 1000 > time_limit_ms:
                break
            nxt, step_evts = apply_all_non_overlapping(current, lhs, rhs)

            # If cancel fired mid-step (inside find_matches), discard the
            # partial result and exit without appending a corrupted state.
            if _is_cancelled():
                break

            for ev in step_evts:
                ev["id"] = ev_id
                ev_id += 1

            # Causal edges — O(k) per event via produced_index.
            # First pass: look up consumed edges → cause event IDs.
            #   For each consumed edge, pop the front of its queue (FIFO).
            #   Popping ensures each duplicate instance is attributed to its own
            #   distinct producer rather than the most-recent one.
            # Second pass: register produced edges in the index.
            # Keeping passes separate avoids intra-step self-causality.
            for ev in step_evts:
                seen_cause_ids: set[int] = set()
                for consumed_edge in ev["consumed"]:
                    q = produced_index[tuple(consumed_edge)]
                    if q:
                        cause_id = q.pop(0)
                        if cause_id not in seen_cause_ids:
                            seen_cause_ids.add(cause_id)
                            causal_edges.append([cause_id, ev["id"]])
            for ev in step_evts:
                for produced_edge in ev["produced"]:
                    produced_index[tuple(produced_edge)].append(ev["id"])

            all_events.append([{"id": e["id"], "consumed": e["consumed"], "produced": e["produced"]} for e in step_evts])
            states.append([e[:] for e in nxt])
            current = nxt

            if progress_cb is not None:
                try:
                    progress_cb(s + 1, steps)
                except Exception:
                    pass  # never let a callback crash the engine
    finally:
        _tls._cancel_ev = None  # always clear so other uses of this thread aren't affected

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

    Size limits: BFS over large hypergraphs is expensive.
      >20 000 edges → skip (return None) — graph too large for useful estimate.
      >5 000 edges  → use 1 seed instead of 5 (reduces BFS cost by 5×).
    The dimension estimate is most useful for early/intermediate steps when the
    emergent geometry is still forming; at very large step counts the manifold
    structure is already well-established and the exact number matters less.
    """
    if len(st) < 5:
        return None
    # Skip BFS entirely for very large states — cost is O(E × seeds) and the
    # estimate adds no value that the trend from smaller states didn't already.
    if len(st) > 20000:
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

    # Use fewer seeds for large graphs (5 000 < edges ≤ 20 000) to keep BFS cheap.
    if len(st) > 5000:
        num_seeds = min(2, len(nodes))
    else:
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
    edge_nodes: list[tuple[int, ...]] = [tuple(e) for e in hyp]
    edge_lens: list[int] = [len(e) for e in hyp]

    # Build a per-call node→incident-edge index so each refinement round only
    # inspects the edges that actually touch a node, instead of scanning all
    # edges for every node.
    incident: dict[int, list[tuple[int, int]]] = defaultdict(list)
    for edge_idx, edge in enumerate(edge_nodes):
        counts: dict[int, int] = defaultdict(int)
        for n in edge:
            counts[n] += 1
        for n, cnt in counts.items():
            incident[n].append((edge_idx, cnt))

    def refine(color0: dict) -> dict:
        c = dict(color0)
        for _ in range(10):
            sigs = {}
            for n in nodes:
                es = []
                for edge_idx, cnt in incident.get(n, []):
                    e = edge_nodes[edge_idx]
                    es.append(
                        f"{cnt}/{edge_lens[edge_idx]}:{','.join(str(c[x]) for x in sorted(e, key=lambda x: c[x]))}"
                    )
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

# ── canonical-labeled multiway event aggregation ─────────────────────

def _canonical_label_maps(hyp: Hypergraph, max_perms: int = 5000) -> list[dict[int, int]]:
    """Return deterministic canonical node label maps for a state.

    For tractable states this enumerates all color-cell permutations that
    achieve the same canonical edge string used by ``canonical_hash``.  Those
    all-optimal maps let event aggregation minimize over automorphisms instead
    of accidentally splitting symmetric rewrite events.  For larger ambiguous
    states, fall back to one deterministic refined-color ordering; this keeps
    MWC responsive while preserving stable output.
    """
    if not hyp:
        return [{}]

    node_set = set(n for e in hyp for n in e)
    nodes = sorted(node_set)
    edge_nodes: list[tuple[int, ...]] = [tuple(e) for e in hyp]
    edge_lens: list[int] = [len(e) for e in hyp]

    incident: dict[int, list[tuple[int, int]]] = defaultdict(list)
    for edge_idx, edge in enumerate(edge_nodes):
        counts: dict[int, int] = defaultdict(int)
        for n in edge:
            counts[n] += 1
        for n, cnt in counts.items():
            incident[n].append((edge_idx, cnt))

    def refine(color0: dict) -> dict:
        c = dict(color0)
        for _ in range(10):
            sigs = {}
            for n in nodes:
                es = []
                for edge_idx, cnt in incident.get(n, []):
                    e = edge_nodes[edge_idx]
                    es.append(
                        f"{cnt}/{edge_lens[edge_idx]}:{','.join(str(c[x]) for x in sorted(e, key=lambda x: c[x]))}"
                    )
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

    def edge_str(relabel: dict[int, int]) -> str:
        edges = [sorted(relabel[n] for n in e) for e in hyp]
        edges.sort()
        return str(edges)

    init_c = defaultdict(int)
    for e in hyp:
        for n in e:
            init_c[n] += 1
    color = refine(dict(init_c))

    cells: dict[int, list[int]] = defaultdict(list)
    for n in nodes:
        cells[color[n]].append(n)
    color_order = sorted(cells.keys())

    total_perms = 1
    for c in color_order:
        f = 1
        for i in range(2, len(cells[c]) + 1):
            f *= i
        total_perms *= f
        if total_perms > max_perms:
            break

    if total_perms > max_perms:
        return [{n: i for i, n in enumerate(sorted(nodes, key=lambda n: (color[n], n)))}]

    best: str | None = None
    best_maps: list[dict[int, int]] = []
    cell_arrays = [cells[c] for c in color_order]

    def try_cells(ci: int, relabel: dict[int, int], next_lbl: int) -> None:
        nonlocal best, best_maps
        if ci >= len(cell_arrays):
            s = edge_str(relabel)
            if best is None or s < best:
                best = s
                best_maps = [dict(relabel)]
            elif s == best:
                best_maps.append(dict(relabel))
            return
        for perm in itertools.permutations(cell_arrays[ci]):
            r = dict(relabel)
            lbl = next_lbl
            for n in perm:
                r[n] = lbl
                lbl += 1
            try_cells(ci + 1, r, lbl)

    try_cells(0, {}, 0)
    return best_maps or [{n: i for i, n in enumerate(nodes)}]


def _canonical_labeled_edge_multiset(edges: list[list[int]], label_map: dict[int, int]) -> list[list[int]]:
    """Convert edges to a sorted multiset under a canonical label map."""
    labeled = [sorted(label_map[n] for n in e) for e in edges]
    labeled.sort()
    return labeled


def _canonical_event_signature(source_state: Hypergraph, event: dict) -> tuple:
    """Return canonical consumed/produced multisets for event-role grouping.

    This uses the source-tied variant from the task spec: nodes already present
    in the source state keep source canonical labels, while RHS-new nodes get
    deterministic fresh labels after the source label range.  That avoids
    depending on whichever representative state first occupied the target
    canonical hash when several concrete events land in the same quotient node.
    """
    source_nodes = {n for e in source_state for n in e}
    best: tuple | None = None

    for label_map in _canonical_label_maps(source_state):
        produced_new: dict[int, int] = {}
        next_label = len(source_nodes)

        def label(n: int) -> int:
            nonlocal next_label
            if n in label_map:
                return label_map[n]
            if n not in produced_new:
                produced_new[n] = next_label
                next_label += 1
            return produced_new[n]

        consumed = _canonical_labeled_edge_multiset(event["consumed"], label_map)
        produced = [sorted(label(n) for n in e) for e in event["produced"]]
        produced.sort()
        candidate = (tuple(tuple(e) for e in consumed), tuple(tuple(e) for e in produced))
        if best is None or candidate < best:
            best = candidate

    return best or ((), ())


def _aggregate_multiway_edges(mw_edges: list[dict], mw_states: dict) -> list[dict]:
    """Group concrete multiway edges by canonical-labeled event role."""
    groups: dict[tuple, dict] = {}
    order: list[tuple] = []

    for edge in mw_edges:
        source_state = edge.get("_sourceState", mw_states[edge["from"]]["state"])
        consumed_sig, produced_sig = _canonical_event_signature(source_state, edge["event"])
        key = (edge["from"], edge["to"], consumed_sig, produced_sig)
        if key not in groups:
            groups[key] = {
                "from": edge["from"],
                "to": edge["to"],
                "signature": repr(key),
                "multiplicity": 0,
                "eventIds": [],
                "representativeEvent": edge["event"],
                "canonicalConsumed": [list(e) for e in consumed_sig],
                "canonicalProduced": [list(e) for e in produced_sig],
            }
            order.append(key)
        groups[key]["multiplicity"] += 1
        groups[key]["eventIds"].append(edge["event"]["id"])

    return [groups[key] for key in order]

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
                mw_edges.append({
                    "from": parent_hash,
                    "to": child_hash,
                    "event": ev,
                    "_sourceState": [e[:] for e in parent_state],
                })

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

    public_edges = [
        {k: v for k, v in edge.items() if not k.startswith("_")}
        for edge in mw_edges
    ]

    return {
        "states": {h: {"state": info["state"], "step": info["step"]} for h, info in mw_states.items()},
        "edges": public_edges,
        "aggregatedEdges": _aggregate_multiway_edges(mw_edges, mw_states),
        "initHash": init_hash,
        "defaultPathEventIds": list(default_path_event_ids),
        "defaultPathHashes": list(default_path_hashes),
    }

# ── per-occurrence multiway BFS (Phase B1) ───────────────────────────

def _normalize_new_nodes(
    parent_nodes: set[int],
    result_state: Hypergraph,
    produced: list[list[int]],
) -> tuple[Hypergraph, list[list[int]]]:
    """Remap fresh nodes to sequential IDs starting from max(parent_nodes)+1.

    ``apply_rule_once`` uses thread-local ``fresh()`` to allocate IDs for new
    variables.  The specific IDs depend on how many earlier expansions ran in
    the same BFS pass, so two structurally identical application paths end up
    with different node labels.  This breaks match-index stability across paths.

    By remapping every node that was NOT in the parent state to sequential IDs
    (max_parent+1, max_parent+2, …) in order of first appearance in the result
    state, we make the child state deterministic given only (parent_state,
    match_idx).  Both the BFS expansion and the sequential replay in
    ``causal_graph_for_path`` produce the SAME state — so ``branch_path``
    indices reliably select the same structural matches on replay.

    Args:
        parent_nodes: Set of node IDs present in the parent state.
        result_state: New state from ``apply_rule_once`` (remaining + produced).
        produced:     Produced edges from the rewrite event (subset of result_state).

    Returns:
        ``(normalized_state, normalized_produced)`` where every fresh node has
        been remapped to a canonical sequential ID.
    """
    max_parent = max(parent_nodes, default=-1)
    mapping: dict[int, int] = {}
    next_id = max_parent + 1

    norm_state: Hypergraph = []
    for edge in result_state:
        new_edge: list[int] = []
        for n in edge:
            if n not in parent_nodes:
                if n not in mapping:
                    mapping[n] = next_id
                    next_id += 1
                new_edge.append(mapping[n])
            else:
                new_edge.append(n)
        norm_state.append(new_edge)

    # Apply the same mapping to the event's produced list.
    # Remaining edges only contain parent nodes (no remapping needed), so
    # ``mapping.get(n, n)`` is an identity for them.
    norm_produced = [[mapping.get(n, n) for n in edge] for edge in produced]
    return norm_state, norm_produced


def compute_multiway_occurrences(
    init_state: Hypergraph,
    lhs: list[list[str]],
    rhs: list[list[str]],
    max_steps: int = 4,
    max_occurrences: int = 5000,
    max_time_ms: int = 5000,
) -> dict:
    """BFS over the multiway system tracking every history occurrence separately.

    Unlike ``compute_multiway()`` which merges isomorphic states by canonical
    hash, this function tracks each distinct *history path* as its own
    occurrence.  Every occurrence carries a ``branch_path`` — the exact
    sequence of match indices applied to the freshly-replayed state at each
    step — which is a replay-stable path token.

    **Why branch_path is replay-stable (fixes Phase A)**

    Phase A exposed ``match_idx`` from the *quotient* multiway edges: the index
    was relative to whichever representative state happened to be stored for a
    canonical hash, not to the actual replayed state.  After an isomorphic
    merge, re-playing that index from the initial state could select a different
    match and land in a different canonical state (empirically: 70/108 BFS paths
    mismatched for rule1, 227/299 for rule5).

    Here each occurrence records the match index applied to its *own* parent's
    exact state.  ``branch_path = parent.branch_path + [match_idx]``, built
    incrementally during the BFS.  Replaying ``branch_path`` from ``init_state``
    via ``causal_graph_for_path()`` always reproduces the same canonical hash as
    ``occurrence["canonical_hash"]`` — by construction.

    **Caps**

    Occurrence count can grow much faster than state count (up to
    ``product of match-count-per-step`` vs ``number of distinct canonical
    hashes``).  Two caps prevent unbounded growth:

    * ``max_occurrences`` (default 5 000): stop expanding once the total
      occurrence count reaches this limit; set ``truncated=True`` with
      ``truncation_reason="max_occurrences"``.
    * ``max_time_ms`` (default 5 000 ms): wall-clock cap; set
      ``truncation_reason="max_time_ms"`` when exceeded.

    When truncated, the returned occurrences are a valid prefix of the full
    BFS tree (all provenance links are consistent within the returned set).

    **Memory note**

    Each occurrence stores its full state internally during the BFS (needed to
    compute matches for the next level).  States are stripped from the returned
    payload to keep it compact.  To replay a specific path use
    ``causal_graph_for_path(init_state, lhs, rhs, occ["branch_path"])``.

    Args:
        init_state:       Initial hypergraph.
        lhs:              Parsed LHS pattern (from ``parse_notation``).
        rhs:              Parsed RHS pattern.
        max_steps:        Maximum BFS depth (rewrite steps from root).
        max_occurrences:  Total occurrence cap including the root.
        max_time_ms:      Wall-clock cap in milliseconds.

    Returns::

        {
          "occurrences": [
            {
              "occ_id":         int,            # unique; 0 = root
              "step":           int,            # depth from root (0 for root)
              "canonical_hash": str,            # canonical hash of state
              "parent_occ_id":  int | None,     # None for root
              "match_idx":      int | None,     # match applied to parent state
              "branch_path":    list[int],      # replay-stable path from root
              "consumed":       list[list[int]],# edges consumed ([] for root)
              "produced":       list[list[int]],# edges produced ([] for root)
            },
            ...
          ],
          "root_hash":         str,    # canonical hash of init_state
          "truncated":         bool,
          "truncation_reason": str | None,  # "max_occurrences"|"max_time_ms"|None
        }
    """
    max_n = max((n for e in init_state for n in e), default=0)
    reset(max_n)

    t0 = time.time()
    root_hash = canonical_hash(init_state)

    # Root occurrence — no parent, no rewrite event.
    root: dict = {
        "occ_id": 0,
        "step": 0,
        "canonical_hash": root_hash,
        "parent_occ_id": None,
        "match_idx": None,
        "branch_path": [],
        "consumed": [],
        "produced": [],
        "_state": [e[:] for e in init_state],  # internal; stripped before return
    }

    occurrences: list[dict] = [root]
    frontier: list[dict] = [root]
    next_occ_id: int = 1
    truncated: bool = False
    truncation_reason: Optional[str] = None

    for _step in range(1, max_steps + 1):
        if not frontier:
            break
        if (time.time() - t0) * 1000 > max_time_ms:
            truncated = True
            truncation_reason = "max_time_ms"
            break

        new_frontier: list[dict] = []
        for parent in frontier:
            parent_state: Hypergraph = parent["_state"]
            matches = find_matches(parent_state, lhs)
            if not matches:
                continue

            for mi_idx in range(len(matches)):
                if len(occurrences) >= max_occurrences:
                    truncated = True
                    truncation_reason = "max_occurrences"
                    break
                if (time.time() - t0) * 1000 > max_time_ms:
                    truncated = True
                    truncation_reason = "max_time_ms"
                    break

                mi, bind = matches[mi_idx]
                result = _apply_match(parent_state, lhs, rhs, mi, bind)

                # Normalize fresh node IDs so branch_path indices are replay-
                # stable regardless of BFS expansion order.  See _normalize_new_nodes.
                parent_nodes: set[int] = set(n for e in parent_state for n in e)
                norm_state, norm_produced = _normalize_new_nodes(
                    parent_nodes, result["state"], result["event"]["produced"]
                )

                child_hash = canonical_hash(norm_state)
                occ: dict = {
                    "occ_id": next_occ_id,
                    "step": _step,
                    "canonical_hash": child_hash,
                    "parent_occ_id": parent["occ_id"],
                    "match_idx": mi_idx,
                    "branch_path": parent["branch_path"] + [mi_idx],
                    "consumed": result["event"]["consumed"],
                    "produced": norm_produced,
                    "_state": norm_state,
                }
                next_occ_id += 1
                occurrences.append(occ)
                new_frontier.append(occ)

            if truncated:
                break

        if truncated:
            break
        frontier = new_frontier

    # Rin audit: determine whether the frontier at max_steps has further matches.
    # multiway_causal_graph() uses this to distinguish "BFS stopped at the
    # requested depth with work remaining" (→ max_depth truncation) from
    # "system naturally ran out of matches" (→ NOT truncated).
    # We check now while _state is still present; it will be stripped below.
    frontier_can_extend: bool = False
    if not truncated and max_steps > 0 and frontier:
        frontier_can_extend = any(
            bool(find_matches(occ["_state"], lhs)) for occ in frontier
        )

    # Strip internal _state field before returning (keep payload compact).
    public_occs = [{k: v for k, v in occ.items() if k != "_state"} for occ in occurrences]

    return {
        "occurrences": public_occs,
        "root_hash": root_hash,
        "truncated": truncated,
        "truncation_reason": truncation_reason,
        "frontier_can_extend": frontier_can_extend,
    }


# ── selected-path causal replay (Phase A helper — reused by B) ────────

def causal_graph_for_path(
    init_state: Hypergraph,
    lhs: list[list[str]],
    rhs: list[list[str]],
    match_indices: list[int],
) -> dict:
    """Compute the causal graph for a specific history through the multiway system.

    Each ``match_indices[k]`` selects which match to apply at step k, starting
    from ``init_state``.  This replays a concrete linear history — one valid
    branch of the multiway exploration — and returns its causal graph.

    The ``match_indices`` here should come from ``occurrence["branch_path"]``
    returned by ``compute_multiway_occurrences()``, NOT from the quotient
    ``match_idx`` on ``compute_multiway()`` edges.  ``branch_path`` values are
    replay-stable (they were recorded relative to the exact replayed state at
    each step); quotient-edge match indices are not (they are relative to an
    arbitrary isomorphic representative after canonical merging).

    Causal attribution uses ``defaultdict(list)`` FIFO queues — same approach
    as ``evolve()`` — so duplicate hyperedge instances trace to their own
    distinct producers.

    Args:
        init_state:    Initial hypergraph.
        lhs:           Parsed LHS pattern.
        rhs:           Parsed RHS pattern.
        match_indices: Replay-stable path token (use ``occ["branch_path"]``).

    Returns::

        {
          "events":       [{id, consumed, produced}, ...],  # one per step
          "causal_edges": [[src_id, dst_id], ...],
          "states":       [state_0, state_after_step_0, ...],  # len = steps+1
        }

    Raises:
        ValueError: if ``match_indices[k]`` is out of range at step k.
    """
    max_n = max((n for e in init_state for n in e), default=0)
    reset(max_n)

    state: Hypergraph = [e[:] for e in init_state]
    events: list[dict] = []
    causal_edges: list[list[int]] = []
    states: list[Hypergraph] = [[e[:] for e in state]]
    produced_index: dict[tuple, list[int]] = defaultdict(list)

    for step, match_idx in enumerate(match_indices):
        result = apply_rule_once(state, lhs, rhs, match_idx)
        if result is None:
            raise ValueError(
                f"step {step}: match_idx {match_idx} is out of range "
                f"(only {len(find_matches(state, lhs))} matches available)"
            )

        # Apply the same normalization used in compute_multiway_occurrences so
        # that node IDs are identical between BFS expansion and replay.
        state_nodes: set[int] = set(n for e in state for n in e)
        norm_state, norm_produced = _normalize_new_nodes(
            state_nodes, result["state"], result["event"]["produced"]
        )

        ev_id = len(events)
        ev = {
            "id": ev_id,
            "consumed": result["event"]["consumed"],
            "produced": norm_produced,
        }

        # FIFO pop — each duplicate instance traces to its own distinct producer.
        seen_cause_ids: set[int] = set()
        for consumed_edge in ev["consumed"]:
            q = produced_index[tuple(consumed_edge)]
            if q:
                cause_id = q.pop(0)
                if cause_id not in seen_cause_ids:
                    seen_cause_ids.add(cause_id)
                    causal_edges.append([cause_id, ev_id])

        for produced_edge in norm_produced:
            produced_index[tuple(produced_edge)].append(ev_id)

        events.append(ev)
        state = norm_state
        states.append([e[:] for e in state])

    return {"events": events, "causal_edges": causal_edges, "states": states}


def _causal_edges_from_event_stream(events: list[dict]) -> list[list[int]]:
    """Compute live causal edges for an already-ordered event stream.

    Each event must provide ``consumed`` and ``produced`` lists.  This mirrors
    the provenance bookkeeping in ``causal_graph_for_path()`` but skips rule
    replay entirely, which is useful when the event stream is already known.
    """
    produced_index: dict[tuple, list[int]] = defaultdict(list)
    causal_edges: list[list[int]] = []
    for ev_id, ev in enumerate(events):
        seen_cause_ids: set[int] = set()
        for consumed_edge in ev["consumed"]:
            q = produced_index[tuple(consumed_edge)]
            if q:
                cause_id = q.pop(0)
                if cause_id not in seen_cause_ids:
                    seen_cause_ids.add(cause_id)
                    causal_edges.append([cause_id, ev_id])

        for produced_edge in ev["produced"]:
            produced_index[tuple(produced_edge)].append(ev_id)

    return causal_edges


def _event_shape_signature(ev: dict) -> tuple:
    """Return an alpha-normalized consumed/produced event signature.

    Single-History greedy evolution and occurrence replay allocate fresh node
    IDs in different contexts: parallel evolution allocates across every event
    in the step, while each occurrence normalizes fresh IDs relative to its own
    parent state.  For embedding the Single-History red set into occurrence
    space, event identity is therefore compared up to a local node renaming
    that preserves the consumed/produced incidence pattern and edge order.
    """
    mapping: dict[int, int] = {}
    next_id = 0

    def encode_edge(edge: list[int]) -> tuple[int, ...]:
        nonlocal next_id
        encoded: list[int] = []
        for node in edge:
            if node not in mapping:
                mapping[node] = next_id
                next_id += 1
            encoded.append(mapping[node])
        return tuple(encoded)

    return (
        tuple(encode_edge(edge) for edge in ev["consumed"]),
        tuple(encode_edge(edge) for edge in ev["produced"]),
    )


def _edge_token_state(init_state: Hypergraph) -> list[dict]:
    """Return tokenized live edge instances for a private replay helper."""
    return [
        {"edge": edge[:], "token": ("init", idx)}
        for idx, edge in enumerate(init_state)
    ]


def _public_state(token_state: list[dict]) -> Hypergraph:
    return [item["edge"][:] for item in token_state]


def _apply_greedy_token_step(
    token_state: list[dict],
    lhs: list[list[str]],
    rhs: list[list[str]],
    next_greedy_index: int,
) -> tuple[list[dict], list[dict]]:
    """Mirror apply_all_non_overlapping while preserving edge-instance tokens."""
    public_state = _public_state(token_state)
    nv = rule_new_vars(lhs, rhs)
    pe_len = len(lhs[0]) if lhs else 0
    selected: list[tuple[list[int], dict]] = []

    if len(lhs) == 1:
        pe = lhs[0]
        for i, edge in enumerate(public_state):
            if len(edge) != pe_len:
                continue
            if i & 1023 == 0 and _is_cancelled():
                break
            perms = _edge_perms(edge)
            bind = dict(zip(pe, perms[0]))
            selected.append(([i], bind))
    else:
        free_by_len: Counter = Counter(len(edge) for edge in public_state)
        lhs_needs: Counter = Counter(len(pe) for pe in lhs)
        if any(free_by_len[L] < lhs_needs[L] for L in lhs_needs):
            return token_state, []

        committed: set[int] = set()
        for mi, bind in _find_matches_gen(public_state, lhs, committed=committed):
            if any(i in committed for i in mi):
                continue
            selected.append((mi, bind))
            committed.update(mi)
            for i in mi:
                free_by_len[len(public_state[i])] -= 1
            if any(free_by_len[L] < lhs_needs[L] for L in lhs_needs):
                break

    if not selected:
        return token_state, []

    matched = {idx for mi, _ in selected for idx in mi}
    remaining = [
        {"edge": item["edge"][:], "token": item["token"]}
        for idx, item in enumerate(token_state)
        if idx not in matched
    ]
    events: list[dict] = []
    produced_items: list[dict] = []

    for batch_index, (mi, bind) in enumerate(selected):
        bind = dict(bind)
        for v in nv:
            bind[v] = fresh()
        greedy_index = next_greedy_index + batch_index
        produced = [[bind[v] for v in re] for re in rhs]
        produced_tokens = [
            ("greedy", greedy_index, rhs_idx)
            for rhs_idx in range(len(produced))
        ]
        produced_items.extend(
            {"edge": edge[:], "token": token}
            for edge, token in zip(produced, produced_tokens)
        )
        events.append({
            "greedy_index": greedy_index,
            "single_history_batch_index": batch_index,
            "consumed": [public_state[i][:] for i in mi],
            "produced": [edge[:] for edge in produced],
            "consumed_tokens": tuple(token_state[i]["token"] for i in mi),
            "produced_tokens": tuple(produced_tokens),
        })

    return remaining + produced_items, events


def _single_history_greedy_token_trace(
    init_state: Hypergraph,
    lhs: list[list[str]],
    rhs: list[list[str]],
    max_steps: int,
    time_limit_ms: int = 5000,
) -> dict:
    """Build the flattened Single-History greedy event trace with private tokens.

    Contract §5: red event identity is edge-instance identity, not local shape.
    """
    max_n = max((n for e in init_state for n in e), default=0)
    reset(max_n)

    t0 = time.time()
    token_state = _edge_token_state(init_state)
    events: list[dict] = []
    batch_counts: list[int] = []

    for single_history_step in range(1, max_steps + 1):
        if time_limit_ms and (time.time() - t0) * 1000 > time_limit_ms:
            break
        token_state, step_events = _apply_greedy_token_step(
            token_state, lhs, rhs, len(events)
        )
        if not step_events:
            break
        for ev in step_events:
            ev["single_history_step"] = single_history_step
        batch_counts.append(len(step_events))
        events.extend(step_events)

    return {"events": events, "batch_counts": batch_counts}


def _single_history_greedy_occurrence_path(
    init_state: Hypergraph,
    lhs: list[list[str]],
    rhs: list[list[str]],
    max_steps: int,
    time_limit_ms: int = 5000,
    max_path_events: Optional[int] = None,
) -> dict:
    """Build the exact Single-History greedy path as occurrence records.

    The multiway occurrence BFS branches one serial rewrite at a time.  A
    Single-History step, however, fires all non-overlapping matches in parallel;
    this path serializes those greedy events in the same deterministic order as
    ``evolve()`` and records the replay-stable match index at each serial event.
    Missing records from this path can then be merged into the ordinary
    occurrence set before causal edges are built, so red IDs remain normal
    occurrence IDs with normal branch paths.

    Contract §6: serial branch_path depth is provenance only; red layout depth
    is the Single-History greedy step recorded on each path record.
    """
    greedy = _single_history_greedy_token_trace(
        init_state, lhs, rhs, max_steps, time_limit_ms=time_limit_ms
    )

    max_n = max((n for e in init_state for n in e), default=0)
    reset(max_n)
    state: Hypergraph = [e[:] for e in init_state]
    token_state: list[dict] = _edge_token_state(init_state)
    path_records: list[dict] = []
    branch_path: list[int] = []
    parent_branch_path: tuple[int, ...] = ()

    for greedy_event in greedy["events"]:
        if max_path_events is not None and len(path_records) >= max_path_events:
            break
        selected_match: Optional[tuple[int, list[int], dict]] = None
        matches = find_matches(state, lhs)
        for match_idx, (mi, bind) in enumerate(matches):
            consumed_tokens = tuple(token_state[i]["token"] for i in mi)
            if consumed_tokens == greedy_event["consumed_tokens"]:
                selected_match = (match_idx, mi, bind)
                break

        if selected_match is None:
            break

        match_idx, mi, bind = selected_match
        result = _apply_match(state, lhs, rhs, mi, bind)
        parent_nodes: set[int] = set(n for e in state for n in e)
        norm_state, norm_produced = _normalize_new_nodes(
            parent_nodes, result["state"], result["event"]["produced"]
        )
        matched = set(mi)
        remaining_tokens = [
            item["token"] for idx, item in enumerate(token_state)
            if idx not in matched
        ]
        token_state = (
            [
                {"edge": edge[:], "token": token}
                for edge, token in zip(
                    [edge for idx, edge in enumerate(norm_state) if idx < len(remaining_tokens)],
                    remaining_tokens,
                )
            ]
            + [
                {"edge": edge[:], "token": token}
                for edge, token in zip(norm_produced, greedy_event["produced_tokens"])
            ]
        )
        state = norm_state
        branch_path = branch_path + [match_idx]
        serial_depth = len(branch_path)
        path_records.append({
            "step": serial_depth,
            "serial_depth": serial_depth,
            "single_history_step": greedy_event["single_history_step"],
            "single_history_batch_index": greedy_event["single_history_batch_index"],
            "greedy_index": greedy_event["greedy_index"],
            "layout": {
                "depth": greedy_event["single_history_step"],
                "branch_key": [
                    greedy_event["single_history_step"],
                    greedy_event["single_history_batch_index"],
                    greedy_event["greedy_index"],
                ],
                "order": greedy_event["single_history_batch_index"],
            },
            "canonical_hash": canonical_hash(state),
            "_parent_branch_path": parent_branch_path,
            "match_idx": match_idx,
            "branch_path": branch_path[:],
            "consumed": result["event"]["consumed"],
            "produced": norm_produced,
        })
        parent_branch_path = tuple(branch_path)

    return {
        "occurrences": path_records,
        "branch_paths": [tuple(rec["branch_path"]) for rec in path_records],
        "greedy_event_count": len(greedy["events"]),
        "batch_counts": greedy["batch_counts"],
    }


# ── multiway causal graph (Phase B2) ─────────────────────────────────

def multiway_causal_graph(
    init_state: Hypergraph,
    lhs: list[list[str]],
    rhs: list[list[str]],
    max_steps: int = 4,
    max_occurrences: int = 5000,
    max_time_ms: int = 5000,
) -> dict:
    """Compute the multiway causal graph: events DAG across all branches.

    Builds the cross-branch causal DAG by running the occurrence BFS
    (``compute_multiway_occurrences``) and then — for each non-root
    occurrence — constructing its ancestry-produced index to find which
    earlier event produced each consumed edge.

    **Co-historical guarantee (Sofia's requirement)**

    Causal edge ``A → B`` exists iff:

    - B's ``consumed`` set contains edge ``h``, AND
    - the ancestry chain from root to B's *parent* contains an occurrence
      whose ``produced`` list includes ``h`` and whose ``occ_id == A``.

    Causal edges from events outside B's ancestry are never added.
    Cross-branch causal edges arise naturally when a shared canonical state
    is reached from two different rule applications; descendant occurrences
    from one branch will have ancestors produced by the other branch only if
    those ancestors appear literally in their ancestry chain.

    **Embedded Single-History greedy event set**

    ``default_path_event_ids`` contains the ``occ_id``\\s of explicit
    occurrence events for deterministic Single-History greedy evolution
    (``evolve()``, via ``apply_all_non_overlapping()``).  These occurrence IDs
    are the red embedded event set in the client; all other occurrence events
    remain green multiway alternatives.

    **Truncation**

    Three stop conditions, all surfaced as ``truncated=True``:

    - ``"max_occurrences"`` — BFS reached ``max_occurrences`` total.
    - ``"max_time_ms"``     — wall-clock cap exceeded.
    - ``"max_depth"``       — BFS stopped at ``max_steps`` depth (Rin
      audit: honest about depth-cap even when other caps don't fire).

    **CACHE_VERSION** is bumped when this payload shape changes so stale
    multiway-causal cache entries are not reused.

    Args:
        init_state:      Initial hypergraph.
        lhs:             Parsed LHS pattern (from ``parse_notation``).
        rhs:             Parsed RHS pattern.
        max_steps:       Maximum BFS depth (rewrite steps from root).
        max_occurrences: Total occurrence cap (default 5 000).
        max_time_ms:     Wall-clock cap in milliseconds (default 5 000).

    Returns::

        {
          "events": [
            {
              "id":             int,           # == occ_id (>0)
              "step":           int,           # depth from root
              "occ_id":         int,           # same as id
              "parent_occ_id":  int | None,
              "match_idx":      int,
              "consumed":       list[list[int]],
              "produced":       list[list[int]],
              "branch_path":    list[int],     # replay-stable path token
            }, ...
          ],
          "causal_edges":            [[from_id, to_id], ...],
          "default_path_event_ids":  [int, ...],
          "stats":                   dict,
          "truncated":               bool,
          "truncation_reason":       str | None,
        }
    """
    # ── 1. Occurrence BFS ─────────────────────────────────────────────
    bfs = compute_multiway_occurrences(
        init_state, lhs, rhs,
        max_steps=max_steps,
        max_occurrences=max_occurrences,
        max_time_ms=max_time_ms,
    )

    occs: list[dict] = bfs["occurrences"]
    truncated: bool = bfs["truncated"]
    truncation_reason: Optional[str] = bfs["truncation_reason"]

    # Rin audit (B2.1 fix): only report max_depth when the frontier actually has
    # more matches available — not when the system terminated naturally at depth.
    # compute_multiway_occurrences() checks this while _state is still live.
    if not truncated and max_steps > 0 and bfs.get("frontier_can_extend", False):
        truncated = True
        truncation_reason = "max_depth"

    greedy_path = _single_history_greedy_occurrence_path(
        init_state,
        lhs,
        rhs,
        max_steps,
        time_limit_ms=max_time_ms,
        max_path_events=max(0, max_occurrences - 1),
    )
    by_branch_path: dict[tuple[int, ...], dict] = {
        tuple(occ["branch_path"]): occ for occ in occs
    }
    next_occ_id = max((occ["occ_id"] for occ in occs), default=0) + 1
    default_path_branch_paths: list[tuple[int, ...]] = []

    for record in greedy_path["occurrences"]:
        branch_path_key = tuple(record["branch_path"])
        existing = by_branch_path.get(branch_path_key)
        if existing is not None:
            existing.update({
                "serial_depth": record["serial_depth"],
                "single_history_step": record["single_history_step"],
                "single_history_batch_index": record["single_history_batch_index"],
                "greedy_index": record["greedy_index"],
                "layout": record["layout"],
            })
            default_path_branch_paths.append(branch_path_key)
            continue

        parent = by_branch_path.get(record["_parent_branch_path"])
        if parent is None:
            break

        occ = {
            "occ_id": next_occ_id,
            "step": record["step"],
            "canonical_hash": record["canonical_hash"],
            "parent_occ_id": parent["occ_id"],
            "match_idx": record["match_idx"],
            "branch_path": record["branch_path"],
            "serial_depth": record["serial_depth"],
            "single_history_step": record["single_history_step"],
            "single_history_batch_index": record["single_history_batch_index"],
            "greedy_index": record["greedy_index"],
            "layout": record["layout"],
            "consumed": record["consumed"],
            "produced": record["produced"],
        }
        next_occ_id += 1
        occs.append(occ)
        by_branch_path[branch_path_key] = occ
        default_path_branch_paths.append(branch_path_key)

    if len(occs) > max_occurrences:
        red_branch_path_set = set(default_path_branch_paths)
        required = [
            occ for occ in occs
            if occ["occ_id"] == 0 or tuple(occ["branch_path"]) in red_branch_path_set
        ]
        green_budget = max(0, max_occurrences - len(required))
        kept: list[dict] = []
        kept_ids: set[int] = set()
        for occ in occs:
            is_required = (
                occ["occ_id"] == 0
                or tuple(occ["branch_path"]) in red_branch_path_set
            )
            if is_required or green_budget > 0:
                kept.append(occ)
                kept_ids.add(occ["occ_id"])
                if not is_required:
                    green_budget -= 1
        occs = kept
        by_branch_path = {tuple(occ["branch_path"]): occ for occ in occs}
        default_path_branch_paths = [
            bp for bp in default_path_branch_paths if bp in by_branch_path
        ]
        truncated = True
        truncation_reason = truncation_reason or "max_occurrences"

    # ── 2. Index occurrences and build events list ────────────────────
    by_id: dict[int, dict] = {o["occ_id"]: o for o in occs}

    events: list[dict] = [
        {
            "id": occ["occ_id"],
            "step": occ["step"],
            "serial_depth": occ.get("serial_depth", len(occ["branch_path"])),
            "single_history_step": occ.get("single_history_step"),
            "single_history_batch_index": occ.get("single_history_batch_index"),
            "greedy_index": occ.get("greedy_index"),
            "layout": occ.get("layout", {
                "depth": occ["step"],
                "branch_key": occ["branch_path"],
                "order": occ["occ_id"],
            }),
            "occ_id": occ["occ_id"],
            "parent_occ_id": occ["parent_occ_id"],
            "match_idx": occ["match_idx"],
            "consumed": occ["consumed"],
            "produced": occ["produced"],
            "branch_path": occ["branch_path"],
        }
        for occ in occs
        if occ["occ_id"] != 0  # root has no rewrite event
    ]

    # ── 3. Cross-branch causal edges via per-occurrence replay ────────
    #
    # Sofia audit (B2.1 blocker): the previous ancestry-produced-index approach
    # indexed every ancestor's produced edges but never drained edges consumed
    # by intermediate ancestors.  When identical edge content is produced and
    # re-consumed along one branch (e.g. rule3: [0,1] is consumed and re-produced
    # at every step), FIFO-pop incorrectly attributed the later consumer to the
    # oldest stale producer instead of the live, immediately-preceding one.
    #
    # Fix: for each occurrence B, replay its branch_path using
    # causal_graph_for_path(), which maintains a live produced_index (popping
    # consumed instances before adding new produced instances — exactly like
    # evolve()).  The causal edges targeting B's own event (local index d-1)
    # give the correct co-historical causal parents.  Mapping local event index
    # i → chain[i].occ_id converts replay-local IDs to occurrence IDs.
    #
    # Cost: O(total_occurrences × depth × apply_rule_once).  Bounded by caps
    # (default 5 000 occurrences × 4 depth = 20 000 apply_rule_once calls).
    causal_edges: list[list[int]] = []
    seen_pairs: set[tuple[int, int]] = set()

    for occ in occs:
        if occ["occ_id"] == 0:
            continue

        d = len(occ["branch_path"])
        if d == 0:
            continue  # root (defensive; already handled above)

        # Build ancestry chain ordered oldest→newest:
        # chain[0] = step-1 ancestor, ..., chain[d-1] = occ itself.
        chain_rev: list[dict] = []
        cur: Optional[dict] = occ
        while cur is not None and cur["occ_id"] != 0:
            chain_rev.append(cur)
            cur = by_id.get(cur["parent_occ_id"])
        chain: list[dict] = list(reversed(chain_rev))

        # Reuse the already-recorded occurrence stream for this ancestry chain
        # instead of replaying the path from scratch.
        path_edges = _causal_edges_from_event_stream(chain)

        # Add only causal edges whose target is B's event (local index d-1).
        # Edges targeting earlier ancestors are emitted when those ancestor
        # occurrences are processed — no duplication, full coverage.
        for src_local, dst_local in path_edges:
            if dst_local == d - 1:
                src_occ_id = chain[src_local]["occ_id"]
                pair = (src_occ_id, occ["occ_id"])
                if pair not in seen_pairs:
                    seen_pairs.add(pair)
                    causal_edges.append(list(pair))

    # ── 4. Embedded Single-History greedy event set ──────────────────
    #
    # Red is the Single-History greedy serial occurrence path inside the same
    # multiway occurrence event set.  No separate red event or edge namespace is
    # emitted; the client derives red edges as the causal_edges induced by these
    # existing occurrence IDs.
    default_path_event_ids: list[int] = [
        by_branch_path[bp]["occ_id"]
        for bp in default_path_branch_paths
        if bp in by_branch_path and by_branch_path[bp]["occ_id"] != 0
    ]
    greedy_flat_event_count = greedy_path["greedy_event_count"]

    embedded_red_count = len(default_path_event_ids)
    stats = {
        "event_count": len(events),
        "embedded_red_event_count": embedded_red_count,
        "green_event_count": max(0, len(events) - embedded_red_count),
        "single_history_greedy_event_count": greedy_flat_event_count,
        "serial_default_path_event_count": embedded_red_count,
        "single_history_batch_counts": greedy_path.get("batch_counts", []),
        "max_steps": max_steps,
        "max_occurrences": max_occurrences,
        "max_time_ms": max_time_ms,
        "truncation_reason": truncation_reason,
    }

    return {
        "events": events,
        "causal_edges": causal_edges,
        "default_path_event_ids": default_path_event_ids,
        "stats": stats,
        "truncated": truncated,
        "truncation_reason": truncation_reason,
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
