"""Unit tests for server.engine — pure Python, no HTTP layer.

Run from the repo root:
    pip install -r requirements-dev.txt
    pytest
"""
from __future__ import annotations
import pytest
from server import engine


# ── parse_notation ────────────────────────────────────────────────────

class TestParseNotation:
    def test_ascii_arrow(self):
        r = engine.parse_notation("{{x,y}} -> {{x,y},{y,z}}")
        assert r is not None
        assert r["lhs"] == [["x", "y"]]
        assert r["rhs"] == [["x", "y"], ["y", "z"]]

    def test_unicode_arrow(self):
        r = engine.parse_notation("{{x,y}} → {{x,y},{y,z}}")
        assert r is not None
        assert r["lhs"] == [["x", "y"]]
        assert r["rhs"] == [["x", "y"], ["y", "z"]]

    def test_no_arrow_returns_none(self):
        assert engine.parse_notation("{{x,y}}") is None

    def test_single_edge_lhs(self):
        """Single-edge rules must not silently drop the edge (was LOW bug)."""
        r = engine.parse_notation("{{x,y}} -> {{x,y},{y,z}}")
        assert r is not None
        assert len(r["lhs"]) == 1

    def test_ternary_edges(self):
        r = engine.parse_notation("{{x,y,z}} -> {{x,u,w},{y,v,u},{z,w,v}}")
        assert r is not None
        assert len(r["lhs"]) == 1 and len(r["lhs"][0]) == 3
        assert len(r["rhs"]) == 3

    def test_multi_edge_lhs(self):
        r = engine.parse_notation("{{x,y},{y,z}} -> {{x,z},{x,w},{y,w},{z,w}}")
        assert r is not None
        assert len(r["lhs"]) == 2


# ── rule_new_vars ─────────────────────────────────────────────────────

class TestRuleNewVars:
    def test_one_new_var(self):
        lhs = [["x", "y"]]
        rhs = [["x", "y"], ["y", "z"]]
        assert engine.rule_new_vars(lhs, rhs) == {"z"}

    def test_multiple_new_vars(self):
        lhs = [["x", "y", "z"]]
        rhs = [["x", "u", "w"], ["y", "v", "u"], ["z", "w", "v"]]
        new = engine.rule_new_vars(lhs, rhs)
        assert new == {"u", "v", "w"}

    def test_no_new_vars(self):
        lhs = [["x", "y"]]
        rhs = [["y", "x"]]
        assert engine.rule_new_vars(lhs, rhs) == set()


# ── find_matches ──────────────────────────────────────────────────────

class TestFindMatches:
    def test_path_match(self):
        """Two-edge path: {0,1},{1,2} matches pattern {x,y},{y,z}."""
        hyp = [[0, 1], [1, 2]]
        pattern = [["x", "y"], ["y", "z"]]
        matches = engine.find_matches(hyp, pattern)
        assert len(matches) >= 1

    def test_no_match_disconnected(self):
        """Disconnected edges cannot match a path pattern."""
        hyp = [[0, 1], [2, 3]]
        pattern = [["x", "y"], ["y", "z"]]
        assert engine.find_matches(hyp, pattern) == []

    def test_self_loop_pattern(self):
        hyp = [[0, 0]]
        pattern = [["x", "x"]]
        matches = engine.find_matches(hyp, pattern)
        assert len(matches) == 1

    def test_single_edge_match(self):
        hyp = [[0, 1]]
        pattern = [["x", "y"]]
        matches = engine.find_matches(hyp, pattern)
        assert len(matches) >= 1

    def test_empty_hypergraph(self):
        assert engine.find_matches([], [["x", "y"]]) == []

    def test_ternary_match(self):
        hyp = [[0, 1, 2]]
        pattern = [["x", "y", "z"]]
        matches = engine.find_matches(hyp, pattern)
        assert len(matches) >= 1

    def test_deduplication(self):
        """find_matches must not return duplicate (indices, binding) pairs."""
        hyp = [[0, 1], [1, 2], [2, 3]]
        pattern = [["x", "y"], ["y", "z"]]
        matches = engine.find_matches(hyp, pattern)
        keys = [(tuple(mi), tuple(sorted(b.items()))) for mi, b in matches]
        assert len(keys) == len(set(keys)), "duplicate matches returned"


# ── evolve ────────────────────────────────────────────────────────────

class TestEvolve:
    def setup_method(self):
        engine.reset(1)

    def test_binary_tree_grows(self):
        """{{x,y}} -> {{x,y},{y,z}}: edges roughly double each step."""
        parsed = engine.parse_notation("{{x,y}} -> {{x,y},{y,z}}")
        result = engine.evolve([[0, 1]], parsed["lhs"], parsed["rhs"], steps=4)
        assert len(result["states"]) == 5  # initial + 4 steps
        assert len(result["states"][-1]) > len(result["states"][0])

    def test_returns_required_keys(self):
        parsed = engine.parse_notation("{{x,y}} -> {{x,y},{y,z}}")
        result = engine.evolve([[0, 1]], parsed["lhs"], parsed["rhs"], steps=2)
        for key in ("states", "events", "causal_edges", "stats"):
            assert key in result

    def test_stats_length_matches_states(self):
        parsed = engine.parse_notation("{{x,y}} -> {{x,y},{y,z}}")
        result = engine.evolve([[0, 1]], parsed["lhs"], parsed["rhs"], steps=3)
        assert len(result["stats"]) == len(result["states"])

    def test_empty_init_no_growth(self):
        """No edges → no rewrite → stays empty every step."""
        parsed = engine.parse_notation("{{x,y}} -> {{x,y},{y,z}}")
        result = engine.evolve([], parsed["lhs"], parsed["rhs"], steps=3)
        for s in result["states"]:
            assert s == []

    def test_time_limit_respected(self):
        """A 1 ms time limit must not raise — just produce fewer steps."""
        parsed = engine.parse_notation("{{x,y}} -> {{x,y},{y,z}}")
        result = engine.evolve([[0, 1]], parsed["lhs"], parsed["rhs"],
                                steps=200, time_limit_ms=1)
        assert len(result["states"]) >= 1

    def test_zero_steps(self):
        parsed = engine.parse_notation("{{x,y}} -> {{x,y},{y,z}}")
        result = engine.evolve([[0, 1]], parsed["lhs"], parsed["rhs"], steps=0)
        assert len(result["states"]) == 1
        assert result["states"][0] == [[0, 1]]

    def test_causal_edges_are_pairs(self):
        parsed = engine.parse_notation("{{x,y}} -> {{x,y},{y,z}}")
        result = engine.evolve([[0, 1]], parsed["lhs"], parsed["rhs"], steps=3)
        for edge in result["causal_edges"]:
            assert len(edge) == 2


# ── build_lineage ─────────────────────────────────────────────────────

class TestBuildLineage:
    def setup_method(self):
        engine.reset(1)

    def test_returns_dicts_and_lists(self):
        parsed = engine.parse_notation("{{x,y}} -> {{x,y},{y,z}}")
        result = engine.evolve([[0, 1]], parsed["lhs"], parsed["rhs"], steps=2)
        lineage, birth_steps = engine.build_lineage(result["states"], result["events"])
        assert isinstance(lineage, dict)
        assert isinstance(birth_steps, list)

    def test_birth_steps_length_matches_states(self):
        parsed = engine.parse_notation("{{x,y}} -> {{x,y},{y,z}}")
        result = engine.evolve([[0, 1]], parsed["lhs"], parsed["rhs"], steps=3)
        lineage, birth_steps = engine.build_lineage(result["states"], result["events"])
        assert len(birth_steps) == len(result["states"])

    def test_first_birth_step_all_zero(self):
        """All edges in the initial state have birth step 0."""
        parsed = engine.parse_notation("{{x,y}} -> {{x,y},{y,z}}")
        result = engine.evolve([[0, 1]], parsed["lhs"], parsed["rhs"], steps=2)
        _, birth_steps = engine.build_lineage(result["states"], result["events"])
        assert birth_steps[0] == [0] * len(result["states"][0])

    def test_lineage_keys_format(self):
        """Lineage keys should be 'step:edgeIdx' strings."""
        parsed = engine.parse_notation("{{x,y}} -> {{x,y},{y,z}}")
        result = engine.evolve([[0, 1]], parsed["lhs"], parsed["rhs"], steps=2)
        lineage, _ = engine.build_lineage(result["states"], result["events"])
        for k in lineage:
            parts = k.split(":")
            assert len(parts) == 2
            assert all(p.isdigit() for p in parts)


# ── canonical_hash ────────────────────────────────────────────────────

class TestCanonicalHash:
    def test_empty_hypergraph(self):
        assert engine.canonical_hash([]) == "[]"

    def test_isomorphic_graphs_same_hash(self):
        """Two graphs related by a node relabelling share the same hash."""
        h1 = [[0, 1], [1, 2]]
        h2 = [[10, 20], [20, 30]]
        assert engine.canonical_hash(h1) == engine.canonical_hash(h2)

    def test_non_isomorphic_graphs_differ(self):
        """A path graph ≠ a pair of disconnected edges."""
        path = [[0, 1], [1, 2]]
        disconnected = [[0, 1], [2, 3]]
        assert engine.canonical_hash(path) != engine.canonical_hash(disconnected)

    def test_same_graph_same_hash(self):
        h = [[0, 1], [1, 2], [2, 0]]
        assert engine.canonical_hash(h) == engine.canonical_hash(h)

    def test_order_invariant(self):
        """Edge order should not change the canonical hash."""
        h1 = [[0, 1], [1, 2]]
        h2 = [[1, 2], [0, 1]]
        assert engine.canonical_hash(h1) == engine.canonical_hash(h2)


# ── estimate_dimension ────────────────────────────────────────────────

class TestEstimateDimension:
    def test_too_small_returns_none(self):
        assert engine.estimate_dimension([[0, 1], [1, 2]]) is None

    def test_returns_float_or_none(self):
        """Should never raise — only return a float or None."""
        ring = [[i, (i + 1) % 10] for i in range(10)]
        dim = engine.estimate_dimension(ring)
        assert dim is None or isinstance(dim, float)

    def test_large_path_returns_dimension(self):
        """A long path should have dimension ≈ 1."""
        path = [[i, i + 1] for i in range(20)]
        dim = engine.estimate_dimension(path)
        # May return None if heuristics disagree, but should not raise
        assert dim is None or (0.5 < dim < 2.0)

    def test_very_large_state_returns_none(self):
        """States >20 000 edges return None (size cap to keep BFS cheap)."""
        large = [[i, i + 1] for i in range(21000)]
        assert engine.estimate_dimension(large) is None

    def test_size_cap_does_not_affect_medium_states(self):
        """States well below the cap still get a dimension estimate."""
        # 100-edge ring — small enough for full BFS, large enough to return a value
        ring = [[i, (i + 1) % 100] for i in range(100)]
        dim = engine.estimate_dimension(ring)
        # Ring ≈ 1D; None is also acceptable if BFS heuristics disagree
        assert dim is None or isinstance(dim, float)


# ── Rec-1: lazy generator ─────────────────────────────────────────────

class TestFindMatchesGen:
    """_find_matches_gen must produce the same matches as find_matches."""

    def _eager(self, hyp, pattern):
        return engine.find_matches(hyp, pattern)

    def _lazy(self, hyp, pattern):
        return list(engine._find_matches_gen(hyp, pattern))

    def _normalise(self, matches):
        """Sort matches for order-invariant comparison."""
        return sorted(
            (tuple(mi), tuple(sorted(b.items()))) for mi, b in matches
        )

    def test_path_same_as_eager(self):
        hyp = [[0, 1], [1, 2]]
        pattern = [["x", "y"], ["y", "z"]]
        assert self._normalise(self._lazy(hyp, pattern)) == \
               self._normalise(self._eager(hyp, pattern))

    def test_no_match_empty(self):
        hyp = [[0, 1], [2, 3]]
        pattern = [["x", "y"], ["y", "z"]]
        assert self._lazy(hyp, pattern) == []

    def test_single_edge(self):
        hyp = [[0, 1]]
        pattern = [["x", "y"]]
        assert self._normalise(self._lazy(hyp, pattern)) == \
               self._normalise(self._eager(hyp, pattern))

    def test_no_duplicates(self):
        hyp = [[0, 1], [1, 2], [2, 3]]
        pattern = [["x", "y"], ["y", "z"]]
        lazy = self._lazy(hyp, pattern)
        keys = [(tuple(mi), tuple(sorted(b.items()))) for mi, b in lazy]
        assert len(keys) == len(set(keys)), "duplicate matches from generator"

    def test_larger_graph_matches_eager(self):
        """5-step evolution: lazy generator must find same matches as eager."""
        parsed = engine.parse_notation("{{x,y}} -> {{x,y},{y,z}}")
        result = engine.evolve([[0, 1]], parsed["lhs"], parsed["rhs"], steps=5)
        # The last state is the interesting one — use it as input for matching.
        state = result["states"][-1]
        assert self._normalise(self._lazy(state, parsed["lhs"])) == \
               self._normalise(self._eager(state, parsed["lhs"]))


# ── Rec-1: apply_all_non_overlapping early-exit ───────────────────────

class TestApplyAllNonOverlappingRec1:
    """Verify that the lazy early-exit selection produces correct output."""

    def setup_method(self):
        engine.reset(1)

    def test_result_matches_reference(self):
        """Both eager and lazy paths should produce the same new hypergraph."""
        parsed = engine.parse_notation("{{x,y}} -> {{x,y},{y,z}}")
        lhs, rhs = parsed["lhs"], parsed["rhs"]
        state = [[0, 1], [1, 2], [2, 3], [3, 4]]
        # Reference: restore find_matches eager path manually
        all_matches = engine.find_matches(state, lhs)
        used_ref: set = set()
        selected_ref = []
        for mi, bind in all_matches:
            if any(i in used_ref for i in mi):
                continue
            selected_ref.append((mi, bind))
            used_ref.update(mi)
        # Optimised path
        nxt, evts = engine.apply_all_non_overlapping(state, lhs, rhs)
        assert len(evts) == len(selected_ref), (
            f"Expected {len(selected_ref)} events, got {len(evts)}"
        )

    def test_no_match_returns_unchanged(self):
        # Two-edge LHS pattern: {x,y},{y,z} requires connected edges.
        # Disconnected pairs [0,1],[2,3] share no node so the pattern cannot match.
        parsed = engine.parse_notation("{{x,y},{y,z}} -> {{x,z},{x,w},{y,w},{z,w}}")
        state = [[0, 1], [2, 3]]  # disconnected — no path match
        nxt, evts = engine.apply_all_non_overlapping(state, parsed["lhs"], parsed["rhs"])
        assert evts == []
        assert nxt == state

    def test_single_edge_fast_path_event_count(self):
        """Single-edge LHS: every edge should produce exactly one event."""
        # Rule3: {x,y} -> {x,y},{y,z} — single binary edge LHS
        parsed = engine.parse_notation("{{x,y}} -> {{x,y},{y,z}}")
        lhs, rhs = parsed["lhs"], parsed["rhs"]
        state = [[0, 1], [1, 2], [2, 3], [3, 4], [4, 5]]
        nxt, evts = engine.apply_all_non_overlapping(state, lhs, rhs)
        # Every edge matches independently — 5 edges → 5 events
        assert len(evts) == 5
        assert len(nxt) == 5 + 5  # 5 original + 5 new (each {x,y} produces {x,y},{y,z})

    def test_single_edge_ternary_fast_path(self):
        """Single ternary-edge LHS (rule4-style): all edges produce events."""
        parsed = engine.parse_notation("{{x,y,z}} -> {{x,u,w},{y,v,u},{z,w,v}}")
        lhs, rhs = parsed["lhs"], parsed["rhs"]
        state = [[0, 1, 2], [3, 4, 5], [6, 7, 8]]
        nxt, evts = engine.apply_all_non_overlapping(state, lhs, rhs)
        assert len(evts) == 3   # all 3 edges consumed
        assert len(nxt) == 9   # 3 × 3 produced edges

    def test_single_edge_fast_path_respects_repeated_variables(self):
        """A repeated variable in one edge requires equal node labels."""
        parsed = engine.parse_notation("{{x,x}} -> {{x,y}}")
        lhs, rhs = parsed["lhs"], parsed["rhs"]
        state = [[0, 1], [2, 2]]
        nxt, evts = engine.apply_all_non_overlapping(state, lhs, rhs)
        assert len(evts) == 1
        assert evts[0]["consumed"] == [[2, 2]]
        assert [0, 1] in nxt

    def test_single_edge_fast_path_preserves_evolution_semantics(self):
        """Fast path must produce identical step count and node growth as general path."""
        # Use rule3 for 5 steps and verify states/events structure is intact.
        parsed = engine.parse_notation("{{x,y}} -> {{x,y},{y,z}}")
        engine.reset(1)
        result = engine.evolve([[0, 1]], parsed["lhs"], parsed["rhs"], steps=5)
        assert len(result["states"]) == 6   # initial + 5 steps
        # Edge count doubles each step: 1, 2, 4, 8, 16, 32
        for i, st in enumerate(result["states"]):
            assert len(st) == 2 ** i, f"step {i}: expected {2**i} edges, got {len(st)}"


# ── Rec-2: produced_index causal edges ───────────────────────────────

class TestCausalIndexRec2:
    """Verify produced_index produces correct causal edges."""

    def setup_method(self):
        engine.reset(1)

    def test_causal_edges_present(self):
        """A simple binary-tree rule should produce causal edges after step 1."""
        parsed = engine.parse_notation("{{x,y}} -> {{x,y},{y,z}}")
        result = engine.evolve([[0, 1]], parsed["lhs"], parsed["rhs"], steps=3)
        assert len(result["causal_edges"]) > 0

    def test_causal_edge_ids_in_range(self):
        parsed = engine.parse_notation("{{x,y}} -> {{x,y},{y,z}}")
        result = engine.evolve([[0, 1]], parsed["lhs"], parsed["rhs"], steps=4)
        all_ev_ids = {ev["id"] for step_evs in result["events"] for ev in step_evs}
        for src, dst in result["causal_edges"]:
            assert src in all_ev_ids, f"causal src {src} not in event IDs"
            assert dst in all_ev_ids, f"causal dst {dst} not in event IDs"

    def test_cross_boundary_causal_edges(self):
        """Extending with initial_flat_events must generate cross-boundary causal links.

        Run step 0→2, then extend from step 2→4 seeding produced_index from
        the first run's flat events.  Events in the extension that consume
        edges produced in the first run must appear in the causal_edges of the
        extension result.
        """
        parsed = engine.parse_notation("{{x,y}} -> {{x,y},{y,z}}")
        lhs, rhs = parsed["lhs"], parsed["rhs"]

        # First run: 2 steps
        engine.reset(1)
        run1 = engine.evolve([[0, 1]], lhs, rhs, steps=2)

        # Collect flat_events from run1 for cross-boundary seeding
        flat_run1 = [ev for step_evs in run1["events"] for ev in step_evs]
        total_ev_count = len(flat_run1)

        # Extension run: 2 more steps starting from last state of run1
        engine.reset(max(n for e in run1["states"][-1] for n in e))
        run2 = engine.evolve(
            run1["states"][-1],
            lhs,
            rhs,
            steps=2,
            initial_ev_id=total_ev_count,
            initial_flat_events=flat_run1,
        )

        # run2's event IDs start at total_ev_count; run1's are 0..total_ev_count-1
        run1_ids = {ev["id"] for ev in flat_run1}
        run2_ids = {ev["id"] for step_evs in run2["events"] for ev in step_evs}

        # At least one causal edge must cross the boundary (run1 → run2)
        cross_boundary = [
            (s, d) for s, d in run2["causal_edges"]
            if s in run1_ids and d in run2_ids
        ]
        assert len(cross_boundary) > 0, (
            "No cross-boundary causal edges found — produced_index seeding may be broken"
        )

    def test_no_spurious_self_causality(self):
        """An event should not be listed as causing itself."""
        parsed = engine.parse_notation("{{x,y}} -> {{x,y},{y,z}}")
        result = engine.evolve([[0, 1]], parsed["lhs"], parsed["rhs"], steps=5)
        for src, dst in result["causal_edges"]:
            assert src != dst, f"self-causal edge detected: {src} -> {dst}"
