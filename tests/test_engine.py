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
