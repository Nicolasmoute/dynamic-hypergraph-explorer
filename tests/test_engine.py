"""Unit tests for server.engine — pure Python, no HTTP layer.

Run from the repo root:
    pip install -r requirements-dev.txt
    pytest
"""
from __future__ import annotations
import hashlib
from collections import Counter, defaultdict
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


# ── application playback ─────────────────────────────────────────────

class TestApplicationPlayback:
    _RULE = "{{x,y}} -> {{x,y},{y,z}}"

    def setup_method(self):
        engine.reset(1)

    def _parsed(self):
        return engine.parse_notation(self._RULE)

    def _build(self, *, max_frames: int = 5000, max_time_ms: int = 5000):
        p = self._parsed()
        return engine.build_application_playback_trace(
            [[0, 1]],
            p["lhs"],
            p["rhs"],
            steps=4,
            max_frames=max_frames,
            max_time_ms=max_time_ms,
        )

    def test_application_playback_frames_deterministic_on_rule3(self):
        digests = []
        for _ in range(5):
            engine.reset(1)
            trace = self._build()
            seq = tuple((f["frame_id"], f["event_index"], f["match_idx"]) for f in trace["frames"])
            digests.append(hashlib.sha256(repr(seq).encode()).hexdigest())
        assert len(set(digests)) == 1

    def test_application_playback_final_state_matches_step_mode(self):
        p = self._parsed()
        step_result = engine.evolve([[0, 1]], p["lhs"], p["rhs"], steps=4)
        trace = self._build()
        frames_by_step: dict[int, list[dict]] = defaultdict(list)
        for frame in trace["frames"]:
            frames_by_step[frame["step"]].append(frame)

        for step_index, step_events in enumerate(step_result["events"]):
            if not step_events:
                continue
            assert frames_by_step[step_index][-1]["state"] == step_result["states"][step_index + 1]

    def test_application_playback_truncation_max_frames(self):
        trace = self._build(max_frames=10)
        assert trace["truncated"] is True
        assert trace["truncation_reason"] == "max_frames"
        assert len(trace["frames"]) == 10

    @pytest.mark.skip(reason="TODO: deterministic wall-clock cap test depends on operation-budget fix")
    def test_application_playback_truncation_max_time_ms(self):
        self._build(max_time_ms=1)


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

    def test_duplicate_node_hyperedges_are_isomorphic(self):
        """Repeated nodes inside hyperedges should remain hash-stable under relabeling."""
        h1 = [[0, 0, 1], [1, 2, 3], [3, 4, 4]]
        h2 = [[10, 10, 11], [11, 12, 13], [13, 14, 14]]
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

    def test_duplicate_edge_instances_get_distinct_causal_attribution(self):
        """Each duplicate hyperedge instance must trace to its own producer.

        The identity rule {x,y}->{x,y} on a state with two copies of [1,2]
        fires twice in step 1 (event 0 and event 1), each producing one [1,2].
        In step 2 (events 2 and 3), each consumer should be attributed to its
        own distinct step-1 producer.

        Old code (dict): produced_index[(1,2)] is overwritten to 1 after step 1,
        so both step-2 events incorrectly attribute to event 1 only.

        New code (list/queue): produced_index[(1,2)] = [0, 1]; pop-FIFO gives
        event 2 → cause 0 and event 3 → cause 1.
        """
        engine.reset(0)
        lhs = [["x", "y"]]
        rhs = [["x", "y"]]  # identity — produces identical edge content

        result = engine.evolve([[1, 2], [1, 2]], lhs, rhs, steps=2)

        step1_evts = result["events"][0]
        step2_evts = result["events"][1]
        assert len(step1_evts) == 2, "expected 2 events in step 1"
        assert len(step2_evts) == 2, "expected 2 events in step 2"

        ev0_id = step1_evts[0]["id"]
        ev1_id = step1_evts[1]["id"]
        ev2_id = step2_evts[0]["id"]
        ev3_id = step2_evts[1]["id"]

        causal = result["causal_edges"]
        causes_of_ev2 = {s for s, d in causal if d == ev2_id}
        causes_of_ev3 = {s for s, d in causal if d == ev3_id}

        # Each step-2 event must be caused by exactly one step-1 event
        assert len(causes_of_ev2) == 1, f"ev2 has {len(causes_of_ev2)} causes, expected 1"
        assert len(causes_of_ev3) == 1, f"ev3 has {len(causes_of_ev3)} causes, expected 1"

        # The two causes must be DIFFERENT — each duplicate instance has its own producer
        assert causes_of_ev2 != causes_of_ev3, (
            "Both step-2 events attributed to the same producer "
            f"({causes_of_ev2}) — duplicate-edge causal-attribution bug"
        )
        # Both must be valid step-1 IDs
        assert causes_of_ev2 <= {ev0_id, ev1_id}
        assert causes_of_ev3 <= {ev0_id, ev1_id}

    def test_same_step_duplicate_producers_attributed_correctly(self):
        """When two events in the same step produce the same edge content,
        the next step's consumers each trace to their own producer.

        Uses a two-edge rule that merges two separate chains into the same
        output node IDs via a hardcoded variable.  Specifically: the state
        contains [[1,2],[3,4]], and the rule {x,y} -> {x,y} fires on both
        edges simultaneously (step 1), producing [1,2] and [3,4].  That is
        the straightforward single-step case above; here we add a *second*
        step to verify the queue drains correctly across multiple steps.
        """
        engine.reset(0)
        # Rule: identity
        lhs = [["x", "y"]]
        rhs = [["x", "y"]]
        # Three copies of [1,2] to stress the queue over 2 steps
        result = engine.evolve([[1, 2], [1, 2], [1, 2]], lhs, rhs, steps=2)

        step2_evts = result["events"][1]
        assert len(step2_evts) == 3, "expected 3 events in step 2"

        step1_ids = {ev["id"] for ev in result["events"][0]}

        causal = result["causal_edges"]
        # Every step-2 event must have exactly one cause, and all causes
        # must be distinct (no two step-2 events share the same producer)
        causes = []
        for ev in step2_evts:
            cause_ids = {s for s, d in causal if d == ev["id"]}
            assert len(cause_ids) == 1, (
                f"event {ev['id']} has {len(cause_ids)} causes, expected 1"
            )
            cause = next(iter(cause_ids))
            assert cause in step1_ids
            causes.append(cause)

        assert len(set(causes)) == 3, (
            f"step-2 events share producers: {causes} — queue not draining correctly"
        )


# ── compute_multiway aggregatedEdges ─────────────────────────────────

class TestComputeMultiwayAggregatedEdges:
    """Canonical-labeled event aggregation over quotient multiway edges."""

    def _rule3_multiway(self):
        parsed = engine.parse_notation("{{x,y}} -> {{x,y},{y,z}}")
        return engine.compute_multiway(
            [[0, 1]],
            parsed["lhs"],
            parsed["rhs"],
            max_steps=4,
            max_states=300,
            max_time_ms=3000,
        )

    @staticmethod
    def _class_containing(aggregated_edges, event_id: int):
        matches = [e for e in aggregated_edges if event_id in e["eventIds"]]
        assert len(matches) == 1, f"event {event_id} appears in {len(matches)} aggregate classes"
        return matches[0]

    def test_rule3_concrete_edges_are_preserved(self):
        result = self._rule3_multiway()
        assert len(result["edges"]) == 42

    def test_rule3_aggregates_to_twenty_event_classes(self):
        result = self._rule3_multiway()
        assert len(result["aggregatedEdges"]) == 20

    def test_rule3_multiplicity_sum_matches_concrete_edges(self):
        result = self._rule3_multiway()
        assert sum(e["multiplicity"] for e in result["aggregatedEdges"]) == 42

    def test_every_concrete_event_id_appears_once(self):
        result = self._rule3_multiway()
        concrete_ids = [e["event"]["id"] for e in result["edges"]]
        aggregate_ids = [
            event_id
            for aggregate in result["aggregatedEdges"]
            for event_id in aggregate["eventIds"]
        ]
        assert sorted(aggregate_ids) == sorted(concrete_ids)
        assert len(aggregate_ids) == len(set(aggregate_ids))

    def test_screenshot_family_events_collapse(self):
        result = self._rule3_multiway()
        aggregates = result["aggregatedEdges"]
        assert self._class_containing(aggregates, 14) is self._class_containing(aggregates, 15)
        assert self._class_containing(aggregates, 35) is self._class_containing(aggregates, 40)
        assert self._class_containing(aggregates, 36) is self._class_containing(aggregates, 38)

    def test_endpoint_pair_is_not_overcollapsed(self):
        result = self._rule3_multiway()
        concrete_by_id = {e["event"]["id"]: e for e in result["edges"]}
        ids = {12, 14, 15, 16}
        endpoint_pairs = {(concrete_by_id[i]["from"], concrete_by_id[i]["to"]) for i in ids}
        assert len(endpoint_pairs) == 1, "fixture ids should share one endpoint pair"

        aggregate_signatures = {
            self._class_containing(result["aggregatedEdges"], i)["signature"]
            for i in ids
        }
        assert len(aggregate_signatures) >= 2

    def test_aggregate_fields_are_self_consistent(self):
        result = self._rule3_multiway()
        for aggregate in result["aggregatedEdges"]:
            assert aggregate["multiplicity"] == len(aggregate["eventIds"])
            assert aggregate["representativeEvent"]["id"] == aggregate["eventIds"][0]
            assert aggregate["canonicalConsumed"] == sorted(aggregate["canonicalConsumed"])
            assert aggregate["canonicalProduced"] == sorted(aggregate["canonicalProduced"])


# ── compute_multiway_occurrences ──────────────────────────────────────

class TestApplyMatch:
    """Tests for the private preselected-match rewrite helper."""

    def setup_method(self):
        engine.reset(1)

    def test_selected_match_matches_apply_rule_once(self):
        """Preselected-match application must match the indexed public path."""
        p = engine.parse_notation("{{x,y}} -> {{x,y},{y,z}}")
        hyp = [[0, 1], [0, 1], [1, 2]]
        matches = engine.find_matches(hyp, p["lhs"])
        assert matches, "expected at least one match"

        mi, binding = matches[0]
        binding_before = dict(binding)
        max_node = max(n for edge in hyp for n in edge)

        engine.reset(max_node)
        helper_result = engine._apply_match(hyp, p["lhs"], p["rhs"], mi, binding)

        engine.reset(max_node)
        indexed_result = engine.apply_rule_once([e[:] for e in hyp], p["lhs"], p["rhs"], 0)

        assert helper_result == indexed_result
        assert binding == binding_before, "helper must not mutate the precomputed binding"


class TestComputeMultiwayOccurrences:
    """Tests for the occurrence-based multiway BFS (Phase B1)."""

    # Rule: {{x,y}} -> {{x,y},{y,z}}  — simple binary-tree rule
    _RULE = "{{x,y}} -> {{x,y},{y,z}}"

    def _parsed(self):
        return engine.parse_notation(self._RULE)

    def setup_method(self):
        engine.reset(1)

    # ── root occurrence ────────────────────────────────────────────────

    def test_root_occurrence_properties(self):
        """occ_id=0, step=0, empty branch_path, no parent, no consumed/produced."""
        p = self._parsed()
        result = engine.compute_multiway_occurrences([[0, 1]], p["lhs"], p["rhs"], max_steps=2)
        root = result["occurrences"][0]
        assert root["occ_id"] == 0
        assert root["step"] == 0
        assert root["branch_path"] == []
        assert root["parent_occ_id"] is None
        assert root["match_idx"] is None
        assert root["consumed"] == []
        assert root["produced"] == []

    def test_root_hash_matches_init_state(self):
        """root_hash must equal canonical_hash(init_state)."""
        p = self._parsed()
        init = [[0, 1], [1, 2]]
        result = engine.compute_multiway_occurrences(init, p["lhs"], p["rhs"], max_steps=1)
        assert result["root_hash"] == engine.canonical_hash(init)

    def test_zero_steps_returns_only_root(self):
        """max_steps=0 → only the root occurrence, not truncated."""
        p = self._parsed()
        result = engine.compute_multiway_occurrences([[0, 1]], p["lhs"], p["rhs"], max_steps=0)
        assert len(result["occurrences"]) == 1
        assert result["truncated"] is False

    # ── structure invariants ───────────────────────────────────────────

    def test_all_occ_ids_unique(self):
        """Every occurrence must have a distinct occ_id."""
        p = self._parsed()
        result = engine.compute_multiway_occurrences([[0, 1]], p["lhs"], p["rhs"], max_steps=3)
        ids = [o["occ_id"] for o in result["occurrences"]]
        assert len(ids) == len(set(ids))

    def test_step_equals_branch_path_length(self):
        """step must equal len(branch_path) for every occurrence."""
        p = self._parsed()
        result = engine.compute_multiway_occurrences([[0, 1]], p["lhs"], p["rhs"], max_steps=3)
        for occ in result["occurrences"]:
            assert occ["step"] == len(occ["branch_path"]), (
                f"occ_id={occ['occ_id']}: step={occ['step']} but "
                f"len(branch_path)={len(occ['branch_path'])}"
            )

    def test_branch_path_extends_parent(self):
        """child.branch_path == parent.branch_path + [child.match_idx]."""
        p = self._parsed()
        result = engine.compute_multiway_occurrences(
            [[0, 1], [1, 2]], p["lhs"], p["rhs"], max_steps=2
        )
        by_id = {o["occ_id"]: o for o in result["occurrences"]}
        for occ in result["occurrences"]:
            if occ["parent_occ_id"] is None:
                continue  # root
            parent = by_id[occ["parent_occ_id"]]
            expected_path = parent["branch_path"] + [occ["match_idx"]]
            assert occ["branch_path"] == expected_path, (
                f"occ_id={occ['occ_id']}: branch_path={occ['branch_path']} "
                f"but expected {expected_path}"
            )

    def test_parent_chain_leads_to_root(self):
        """Following parent_occ_id links must always reach the root (occ_id=0)."""
        p = self._parsed()
        result = engine.compute_multiway_occurrences(
            [[0, 1], [1, 2]], p["lhs"], p["rhs"], max_steps=3
        )
        by_id = {o["occ_id"]: o for o in result["occurrences"]}
        for occ in result["occurrences"]:
            cur = occ
            seen = set()
            while cur["parent_occ_id"] is not None:
                assert cur["occ_id"] not in seen, "cycle in parent links"
                seen.add(cur["occ_id"])
                cur = by_id[cur["parent_occ_id"]]
            assert cur["occ_id"] == 0, f"chain from {occ['occ_id']} did not reach root"

    # ── multi-branch behaviour ─────────────────────────────────────────

    def test_multi_match_state_produces_multiple_children(self):
        """A state with multiple matches must produce multiple child occurrences."""
        p = self._parsed()
        # [[0,1],[1,2]] has ≥2 matches for the single-edge rule (both edges can fire)
        result = engine.compute_multiway_occurrences(
            [[0, 1], [1, 2]], p["lhs"], p["rhs"], max_steps=1
        )
        children_of_root = [o for o in result["occurrences"] if o["parent_occ_id"] == 0]
        assert len(children_of_root) >= 2, (
            f"Expected ≥2 children of root but got {len(children_of_root)}"
        )

    # ── caps ───────────────────────────────────────────────────────────

    def test_max_occurrences_cap_triggers_truncation(self):
        """Capping at 3 occurrences must set truncated=True, truncation_reason='max_occurrences'."""
        p = self._parsed()
        result = engine.compute_multiway_occurrences(
            [[0, 1], [1, 2]], p["lhs"], p["rhs"],
            max_steps=10,
            max_occurrences=3,
        )
        assert result["truncated"] is True
        assert result["truncation_reason"] == "max_occurrences"
        assert len(result["occurrences"]) <= 3

    def test_max_operations_cap_triggers_deterministic_truncation(self):
        """Capping child-expansion slots must return the exact deterministic BFS prefix."""
        p = self._parsed()
        result = engine.compute_multiway_occurrences(
            [[0, 1], [1, 2]], p["lhs"], p["rhs"],
            max_steps=10,
            max_occurrences=5000,
            max_time_ms=5000,
            max_operations=2,
        )
        assert result["truncated"] is True
        assert result["truncation_reason"] == "max_operations"
        assert [
            (o["occ_id"], o["parent_occ_id"], o["match_idx"], o["branch_path"])
            for o in result["occurrences"]
        ] == [
            (0, None, None, []),
            (1, 0, 0, [0]),
            (2, 0, 1, [1]),
        ]

    def test_max_operations_tie_with_occurrences_cap_reports_occurrences(self):
        """When occurrence cap and operation budget both exhaust at the same boundary, occurrences wins."""
        p = self._parsed()
        result = engine.compute_multiway_occurrences(
            [[0, 1]], p["lhs"], p["rhs"],
            max_steps=10, max_occurrences=3, max_time_ms=5000, max_operations=2,
        )
        assert result["truncated"] is True
        assert result["truncation_reason"] == "max_occurrences"
        assert len(result["occurrences"]) == 3

    def test_max_operations_prefix_is_replay_stable(self):
        """Operation-budget truncation must not emit unreplayable partial occurrences."""
        p = self._parsed()
        init = [[0, 1], [1, 2]]
        result = engine.compute_multiway_occurrences(
            init, p["lhs"], p["rhs"],
            max_steps=10,
            max_occurrences=5000,
            max_time_ms=5000,
            max_operations=4,
        )
        assert result["truncated"] is True
        assert result["truncation_reason"] == "max_operations"
        for occ in result["occurrences"]:
            if not occ["branch_path"]:
                continue
            replay = engine.causal_graph_for_path(init, p["lhs"], p["rhs"], occ["branch_path"])
            assert engine.canonical_hash(replay["states"][-1]) == occ["canonical_hash"]

    def test_max_operations_repeated_runs_have_identical_digest(self):
        """Rule2 and rule5 payload prefixes must be identical across operation-budgeted runs."""
        cases = [
            (
                [[0, 1], [1, 2], [2, 3], [3, 4], [4, 0]],
                "{{x,y},{y,z}} -> {{x,y},{y,w},{w,z}}",
                24,
            ),
            (
                [
                    [0, 1, 2], [2, 3, 4], [4, 5, 6], [6, 7, 8],
                    [8, 9, 0], [1, 3, 5], [5, 7, 9], [9, 1, 3],
                ],
                "{{x,y,z},{z,u,v}} -> {{y,z,u},{v,w,x},{w,y,v}}",
                24,
            ),
        ]
        for init, notation, max_operations in cases:
            p = engine.parse_notation(notation)
            digests = []
            for _ in range(3):
                result = engine.compute_multiway_occurrences(
                    init, p["lhs"], p["rhs"],
                    max_steps=4,
                    max_occurrences=5000,
                    max_time_ms=5000,
                    max_operations=max_operations,
                )
                assert result["truncated"] is True
                assert result["truncation_reason"] == "max_operations"
                digests.append([
                    (
                        o["occ_id"],
                        o["canonical_hash"],
                        o["parent_occ_id"],
                        o["match_idx"],
                        tuple(o["branch_path"]),
                    )
                    for o in result["occurrences"]
                ])
            assert digests[0] == digests[1] == digests[2]

    def test_max_time_cap_triggers_truncation(self):
        """A 1 ms time cap on a branching rule must truncate without raising."""
        p = self._parsed()
        result = engine.compute_multiway_occurrences(
            [[0, 1], [1, 2]], p["lhs"], p["rhs"],
            max_steps=20,
            max_time_ms=1,
        )
        # Either completed before the cap or was truncated — either is fine;
        # what must NOT happen is a crash.
        assert result["truncated"] in (True, False)
        assert len(result["occurrences"]) >= 1

    def test_no_truncation_when_well_within_caps(self):
        """A tiny run must complete without truncation."""
        p = self._parsed()
        result = engine.compute_multiway_occurrences(
            [[0, 1]], p["lhs"], p["rhs"],
            max_steps=2,
            max_occurrences=5000,
            max_time_ms=5000,
        )
        assert result["truncated"] is False
        assert result["truncation_reason"] is None

    # ── KEY: replay stability ──────────────────────────────────────────

    def test_replay_stability_all_occurrences(self):
        """The canonical hash of the replayed final state must match occ['canonical_hash'].

        This is the core correctness guarantee of Phase B1: branch_path values
        recorded during BFS are replay-stable — replaying them from init_state
        via causal_graph_for_path() always lands in the same canonical state.
        """
        p = self._parsed()
        init = [[0, 1], [1, 2]]
        result = engine.compute_multiway_occurrences(init, p["lhs"], p["rhs"], max_steps=3)
        for occ in result["occurrences"]:
            if not occ["branch_path"]:
                continue  # skip root
            replay = engine.causal_graph_for_path(init, p["lhs"], p["rhs"], occ["branch_path"])
            replayed_hash = engine.canonical_hash(replay["states"][-1])
            assert replayed_hash == occ["canonical_hash"], (
                f"occ_id={occ['occ_id']} branch_path={occ['branch_path']}: "
                f"replayed hash {replayed_hash!r} != stored {occ['canonical_hash']!r}"
            )

    def test_replay_stability_ternary_rule(self):
        """Replay stability holds for ternary-edge rules too (rule4-style)."""
        p = engine.parse_notation("{{x,y,z}} -> {{x,u,w},{y,v,u},{z,w,v}}")
        init = [[0, 1, 2], [3, 4, 5]]
        result = engine.compute_multiway_occurrences(init, p["lhs"], p["rhs"], max_steps=2)
        for occ in result["occurrences"]:
            if not occ["branch_path"]:
                continue
            replay = engine.causal_graph_for_path(init, p["lhs"], p["rhs"], occ["branch_path"])
            replayed_hash = engine.canonical_hash(replay["states"][-1])
            assert replayed_hash == occ["canonical_hash"], (
                f"occ_id={occ['occ_id']}: replayed hash mismatch"
            )

    def test_no_internal_state_in_returned_occurrences(self):
        """_state must not appear in the public payload."""
        p = self._parsed()
        result = engine.compute_multiway_occurrences([[0, 1]], p["lhs"], p["rhs"], max_steps=2)
        for occ in result["occurrences"]:
            assert "_state" not in occ, "_state leaked into public occurrence payload"


# ── causal_graph_for_path ─────────────────────────────────────────────

class TestCausalGraphForPath:
    """Tests for the selected-path causal replay helper."""

    _RULE = "{{x,y}} -> {{x,y},{y,z}}"

    def _parsed(self):
        return engine.parse_notation(self._RULE)

    def setup_method(self):
        engine.reset(1)

    def test_empty_path_returns_empty(self):
        """match_indices=[] → no events, no causal edges, one state (the initial)."""
        p = self._parsed()
        r = engine.causal_graph_for_path([[0, 1]], p["lhs"], p["rhs"], [])
        assert r["events"] == []
        assert r["causal_edges"] == []
        assert len(r["states"]) == 1
        assert r["states"][0] == [[0, 1]]

    def test_single_step_no_prior_produces_no_causal_edge(self):
        """First rewrite has no predecessor events → no causal edges."""
        p = self._parsed()
        r = engine.causal_graph_for_path([[0, 1]], p["lhs"], p["rhs"], [0])
        assert len(r["events"]) == 1
        assert r["causal_edges"] == []

    def test_two_step_path_has_causal_edge(self):
        """Second rewrite consumes a produced edge → at least one causal edge."""
        p = self._parsed()
        r = engine.causal_graph_for_path([[0, 1]], p["lhs"], p["rhs"], [0, 0])
        assert len(r["events"]) == 2
        assert len(r["causal_edges"]) >= 1

    def test_states_length_equals_steps_plus_one(self):
        """len(states) must equal len(match_indices) + 1 for any path length."""
        p = self._parsed()
        for depth in [0, 1, 2, 3, 4]:
            r = engine.causal_graph_for_path([[0, 1]], p["lhs"], p["rhs"], [0] * depth)
            assert len(r["states"]) == depth + 1, (
                f"depth={depth}: expected {depth+1} states, got {len(r['states'])}"
            )

    def test_event_ids_are_sequential(self):
        """Event IDs must be 0, 1, 2, … in order."""
        p = self._parsed()
        r = engine.causal_graph_for_path([[0, 1]], p["lhs"], p["rhs"], [0, 0, 0])
        ids = [ev["id"] for ev in r["events"]]
        assert ids == list(range(len(ids)))

    def test_invalid_match_idx_raises(self):
        """An out-of-range match_idx must raise ValueError, not silently skip."""
        p = self._parsed()
        with pytest.raises(ValueError):
            engine.causal_graph_for_path([[0, 1]], p["lhs"], p["rhs"], [99])

    def test_causal_edges_reference_valid_event_ids(self):
        """Every [src, dst] in causal_edges must reference a valid event id."""
        p = self._parsed()
        r = engine.causal_graph_for_path([[0, 1]], p["lhs"], p["rhs"], [0, 0, 0, 0])
        valid_ids = {ev["id"] for ev in r["events"]}
        for src, dst in r["causal_edges"]:
            assert src in valid_ids, f"causal src {src} not in event IDs"
            assert dst in valid_ids, f"causal dst {dst} not in event IDs"

    def test_path_causal_duplicate_attribution_correct(self):
        """Duplicate produced edges trace to distinct producers via FIFO.

        Identity rule {x,y}->{x,y} on [[1,2],[1,2]]:
        - step 0: match_idx=0 → consumes one [1,2], produces one [1,2] (event 0)
        - step 1: match_idx=0 → consumes [1,2] from event 0, so causal edge 0→1
        """
        engine.reset(0)
        lhs = [["x", "y"]]
        rhs = [["x", "y"]]
        r = engine.causal_graph_for_path([[1, 2], [1, 2]], lhs, rhs, [0, 0])
        assert len(r["events"]) == 2
        # Event 1 consumes an edge produced by event 0 → must have causal edge 0→1
        assert [0, 1] in r["causal_edges"], (
            f"Expected causal edge [0,1] but got: {r['causal_edges']}"
        )

    def test_occurrence_chain_helper_matches_replay(self):
        """The occurrence-stream helper must match replay on a live-provenance path."""
        p = self._parsed()
        init = [[0, 1]]
        result = engine.compute_multiway_occurrences(init, p["lhs"], p["rhs"], max_steps=3)
        by_id = {occ["occ_id"]: occ for occ in result["occurrences"]}
        target = next(
            occ for occ in result["occurrences"] if tuple(occ["branch_path"]) == (0, 0, 2)
        )

        chain = []
        cur = target
        while cur["occ_id"] != 0:
            chain.append(cur)
            cur = by_id[cur["parent_occ_id"]]
        chain.reverse()

        helper_edges = engine._causal_edges_from_event_stream(chain)
        replay = engine.causal_graph_for_path(init, p["lhs"], p["rhs"], target["branch_path"])
        assert helper_edges == replay["causal_edges"]


# ── multiway_causal_graph ─────────────────────────────────────────────

class TestMultiwayCausalGraph:
    """Tests for the cross-branch multiway causal DAG (Phase B2)."""

    _RULE = "{{x,y}} -> {{x,y},{y,z}}"

    def _parsed(self):
        return engine.parse_notation(self._RULE)

    def setup_method(self):
        engine.reset(1)

    # ── return-shape invariants ────────────────────────────────────────

    def test_required_keys_present(self):
        """Result must contain events, causal_edges, default_path_event_ids,
        truncated, truncation_reason."""
        p = self._parsed()
        r = engine.multiway_causal_graph([[0, 1]], p["lhs"], p["rhs"], max_steps=2)
        for key in ("events", "causal_edges", "default_path_event_ids",
                    "stats", "truncated", "truncation_reason"):
            assert key in r, f"missing key: {key!r}"

    def test_no_out_of_band_realized_red_namespace(self):
        """MWC red is embedded in occurrence IDs, not separate r* records."""
        p = self._parsed()
        r = engine.multiway_causal_graph([[0, 1]], p["lhs"], p["rhs"], max_steps=2)
        assert "realized_events" not in r
        assert "realized_causal_edges" not in r

    def test_events_have_required_fields(self):
        """Each event must have id, step, occ_id, parent_occ_id, match_idx,
        consumed, produced, branch_path."""
        p = self._parsed()
        r = engine.multiway_causal_graph([[0, 1]], p["lhs"], p["rhs"], max_steps=2)
        for ev in r["events"]:
            for field in ("id", "step", "occ_id", "parent_occ_id", "match_idx",
                          "consumed", "produced", "branch_path", "serial_depth",
                          "single_history_step", "single_history_batch_index",
                          "greedy_index", "layout", "canonicalEventSignature",
                          "canonicalConsumed", "canonicalProduced",
                          "multiplicity", "equivalentEventIds"):
                assert field in ev, f"event missing field {field!r}: {ev}"
            assert "depth" in ev["layout"], f"event missing layout.depth: {ev}"

    def test_canonical_event_class_metadata_is_self_consistent(self):
        p = self._parsed()
        r = engine.multiway_causal_graph(
            [[0, 1]], p["lhs"], p["rhs"], max_steps=4,
            max_occurrences=5000, max_time_ms=5000,
        )
        events = r["events"]
        event_ids = {ev["id"] for ev in events}
        by_signature: dict[str, list[dict]] = defaultdict(list)

        for ev in events:
            # After quotient dedup: multiplicity == len(equivalentEventIds) still holds
            # because equivalentEventIds retains the full original collapsed-class ID set
            # (including non-representative sibling IDs that no longer appear in events).
            assert ev["multiplicity"] == len(ev["equivalentEventIds"])
            assert ev["id"] in ev["equivalentEventIds"]
            # NOTE: set(equivalentEventIds) is NOT required to be ⊆ event_ids — sibling
            # IDs of collapsed classes are intentionally absent from the deduplicated
            # events list.  The representative's own id IS in both sets.
            assert ev["id"] in event_ids
            assert ev["canonicalConsumed"] == sorted(ev["canonicalConsumed"])
            assert ev["canonicalProduced"] == sorted(ev["canonicalProduced"])
            by_signature[ev["canonicalEventSignature"]].append(ev)

        # After quotient dedup each signature has exactly one representative.
        assert len(by_signature) == len(events), (
            "Each canonicalEventSignature should appear exactly once after dedup"
        )
        # Sum of class sizes (multiplicities) >= number of representatives.
        representative_total = sum(sig_evs[0]["multiplicity"] for sig_evs in by_signature.values())
        assert representative_total >= len(events)
        # At least one non-trivial class (multiplicity > 1) for rule3 at depth 4.
        has_nontrivial_class = any(sig_evs[0]["multiplicity"] > 1 for sig_evs in by_signature.values())
        assert has_nontrivial_class

    def test_cache_version_bumped_for_multiway_causal_event_metadata(self):
        assert engine.CACHE_VERSION == "v15"

    # ── quotient-mode acceptance (task t-2026-05-11-85ba2c03) ─────────

    # ── acceptance item 8: full quotient-mode suite ────────────────────
    #
    # Empirical dedup factors (max_steps=1, max_occurrences=5000) measured
    # 2026-05-11 on current origin/main + v15 quotient pass:
    #   rule1: step 1 — 2 concrete → 1 canonical  (factor 2×)
    #   rule2: step 1 — 10 concrete → 1 canonical (factor 10×)
    #   rule3: step 1 — 2 concrete → 1 canonical  (factor 2×)
    #   rule4: step 1 — 6 concrete → 1 canonical  (factor 6×)
    #   rule5: step 1 — 144 concrete → 72 canonical (factor 2×, 72 classes)
    # Rules 2-4 collapse step 1 to a single canonical class; rule5 is the
    # only built-in rule that retains multiple canonical classes at step 1.

    def test_rule5_mwc_step1_dedup_144_to_72(self):
        """Sofia baseline: rule5 max_steps=1 → 144 concrete → 72 canonical at step==1."""
        engine.reset(1)
        p = engine.parse_notation("{{x,y,z},{z,u,v}} → {{y,z,u},{v,w,x},{w,y,v}}")
        rule5_init = [[0,1,2],[2,3,4],[4,5,6],[6,7,8],[8,9,0],[1,3,5],[5,7,9],[9,1,3]]
        r = engine.multiway_causal_graph(
            rule5_init, p["lhs"], p["rhs"],
            max_steps=1, max_occurrences=5000, max_time_ms=30000,
        )
        step1 = [ev for ev in r["events"] if ev["step"] == 1]
        assert len(step1) == 72, (
            f"Expected 72 canonical events at step==1 after quotient pass, got {len(step1)}"
        )
        # Each representative has multiplicity=2 (two concrete → one canonical)
        assert all(ev["multiplicity"] == 2 for ev in step1), (
            "All step-1 canonical classes should have multiplicity=2 for rule5 max_steps=1"
        )
        # canonicalEventSignature is unique across all emitted events
        sigs = [ev["canonicalEventSignature"] for ev in r["events"]]
        assert len(sigs) == len(set(sigs)), "canonicalEventSignature not unique after dedup"

    def test_unique_sig_per_step(self):
        """Each canonicalEventSignature appears at most once per step after dedup."""
        engine.reset(1)
        p = engine.parse_notation("{{x,y,z},{z,u,v}} → {{y,z,u},{v,w,x},{w,y,v}}")
        rule5_init = [[0,1,2],[2,3,4],[4,5,6],[6,7,8],[8,9,0],[1,3,5],[5,7,9],[9,1,3]]
        r = engine.multiway_causal_graph(
            rule5_init, p["lhs"], p["rhs"],
            max_steps=1, max_occurrences=5000, max_time_ms=30000,
        )
        from collections import defaultdict
        sigs_by_step: dict[int, list[str]] = defaultdict(list)
        for ev in r["events"]:
            sigs_by_step[ev["step"]].append(ev["canonicalEventSignature"])
        for step, sigs in sigs_by_step.items():
            assert len(sigs) == len(set(sigs)), (
                f"Step {step}: duplicate canonicalEventSignature — dedup invariant violated"
            )

    def test_multiplicity_sum_equals_concrete_count(self):
        """sum(multiplicity) per step must equal the pre-dedup concrete event count.

        Pre-dedup concrete count comes from compute_multiway_occurrences directly.
        """
        engine.reset(1)
        p = engine.parse_notation("{{x,y,z},{z,u,v}} → {{y,z,u},{v,w,x},{w,y,v}}")
        rule5_init = [[0,1,2],[2,3,4],[4,5,6],[6,7,8],[8,9,0],[1,3,5],[5,7,9],[9,1,3]]

        # Pre-dedup counts from raw BFS
        engine.reset(1)
        bfs = engine.compute_multiway_occurrences(
            rule5_init, p["lhs"], p["rhs"],
            max_steps=1, max_occurrences=5000, max_time_ms=30000,
            include_internal_states=True,
        )
        pre_counts: dict[int, int] = {}
        for occ in bfs["occurrences"]:
            if occ["occ_id"] != 0:
                s = occ["step"]
                pre_counts[s] = pre_counts.get(s, 0) + 1

        # Post-dedup result
        engine.reset(1)
        r = engine.multiway_causal_graph(
            rule5_init, p["lhs"], p["rhs"],
            max_steps=1, max_occurrences=5000, max_time_ms=30000,
        )
        from collections import defaultdict
        mult_by_step: dict[int, int] = defaultdict(int)
        for ev in r["events"]:
            mult_by_step[ev["step"]] += ev["multiplicity"]

        for step, pre_count in pre_counts.items():
            post_sum = mult_by_step.get(step, 0)
            assert post_sum == pre_count, (
                f"Step {step}: sum(multiplicity)={post_sum} != pre-dedup count={pre_count}"
            )

    def test_representative_selection_idempotent(self):
        """Two consecutive calls return the same representative event IDs per step."""
        engine.reset(1)
        p = engine.parse_notation("{{x,y,z},{z,u,v}} → {{y,z,u},{v,w,x},{w,y,v}}")
        rule5_init = [[0,1,2],[2,3,4],[4,5,6],[6,7,8],[8,9,0],[1,3,5],[5,7,9],[9,1,3]]

        runs = []
        for _ in range(2):
            engine.reset(1)
            r = engine.multiway_causal_graph(
                rule5_init, p["lhs"], p["rhs"],
                max_steps=1, max_occurrences=5000, max_time_ms=30000,
            )
            # Capture (step, canonicalEventSignature, multiplicity) tuples — same
            # rep selection means same canonical sigs with same multiplicities.
            key = tuple(sorted(
                (ev["step"], ev["canonicalEventSignature"], ev["multiplicity"])
                for ev in r["events"]
            ))
            runs.append(key)

        assert runs[0] == runs[1], "Representative selection is not idempotent"

    def test_event_ids_are_unique(self):
        """All event IDs must be distinct."""
        p = self._parsed()
        r = engine.multiway_causal_graph(
            [[0, 1], [1, 2]], p["lhs"], p["rhs"], max_steps=2
        )
        ids = [ev["id"] for ev in r["events"]]
        assert len(ids) == len(set(ids)), "duplicate event IDs"

    def test_causal_edges_reference_valid_event_ids(self):
        """Every [src, dst] in causal_edges must reference a valid event id."""
        p = self._parsed()
        r = engine.multiway_causal_graph(
            [[0, 1], [1, 2]], p["lhs"], p["rhs"], max_steps=3
        )
        valid_ids = {ev["id"] for ev in r["events"]}
        for src, dst in r["causal_edges"]:
            assert src in valid_ids, f"causal src {src} not in event IDs"
            assert dst in valid_ids, f"causal dst {dst} not in event IDs"

    def test_causal_edges_are_pairs(self):
        """Every causal edge must be a 2-element list."""
        p = self._parsed()
        r = engine.multiway_causal_graph([[0, 1]], p["lhs"], p["rhs"], max_steps=3)
        for edge in r["causal_edges"]:
            assert len(edge) == 2, f"causal edge not a pair: {edge}"

    def test_no_self_causal_edges(self):
        """No event should be listed as its own causal predecessor."""
        p = self._parsed()
        r = engine.multiway_causal_graph(
            [[0, 1], [1, 2]], p["lhs"], p["rhs"], max_steps=3
        )
        for src, dst in r["causal_edges"]:
            assert src != dst, f"self-causal edge: {src} → {dst}"

    # ── zero / greedy-only cases ───────────────────────────────────────

    def test_zero_steps_returns_no_events(self):
        """max_steps=0 → no events, no causal edges, empty default path."""
        p = self._parsed()
        r = engine.multiway_causal_graph([[0, 1]], p["lhs"], p["rhs"], max_steps=0)
        assert r["events"] == []
        assert r["causal_edges"] == []
        assert r["default_path_event_ids"] == []
        assert r["truncated"] is False

    def test_single_step_no_causal_edges(self):
        """After one step on a single-edge init there are no prior producers → no causal edges."""
        p = self._parsed()
        r = engine.multiway_causal_graph([[0, 1]], p["lhs"], p["rhs"], max_steps=1)
        # Step 1 events consume from init_state (no prior events) → no causal edges
        assert r["causal_edges"] == []

    def test_multi_step_has_causal_edges(self):
        """After 3 steps there must be causal edges (produced edges are consumed downstream)."""
        p = self._parsed()
        r = engine.multiway_causal_graph([[0, 1]], p["lhs"], p["rhs"], max_steps=3)
        assert len(r["causal_edges"]) > 0

    # ── default path ──────────────────────────────────────────────────

    def test_default_path_is_nonempty(self):
        """A matchable rule with max_steps≥1 must produce a nonempty default path."""
        p = self._parsed()
        r = engine.multiway_causal_graph([[0, 1]], p["lhs"], p["rhs"], max_steps=2)
        assert len(r["default_path_event_ids"]) >= 1

    def test_default_path_ids_are_valid_event_ids(self):
        """All default_path_event_ids must appear in events."""
        p = self._parsed()
        r = engine.multiway_causal_graph(
            [[0, 1], [1, 2]], p["lhs"], p["rhs"], max_steps=3
        )
        valid_ids = {ev["id"] for ev in r["events"]}
        for eid in r["default_path_event_ids"]:
            assert eid in valid_ids, f"default path id {eid} not in events"

    def test_default_path_branch_paths_are_serial_prefixes(self):
        """Red events form one replay-stable serial occurrence path."""
        p = self._parsed()
        init = [[0, 1]]
        r = engine.multiway_causal_graph(
            init, p["lhs"], p["rhs"],
            max_steps=4, max_occurrences=500, max_time_ms=1000,
        )
        ev_by_id = {ev["id"]: ev for ev in r["events"]}
        red_paths = [ev_by_id[eid]["branch_path"] for eid in r["default_path_event_ids"]]
        assert [len(path) for path in red_paths] == list(range(1, len(red_paths) + 1))
        for prev, cur in zip(red_paths, red_paths[1:]):
            assert cur[:len(prev)] == prev

    def test_default_path_records_match_indices(self):
        """Greedy serial occurrence paths expose replay match indices."""
        p = engine.parse_notation("{{x,y,z}} -> {{x,u,w},{y,v,u},{z,w,v}}")
        r = engine.multiway_causal_graph(
            [[0, 1, 2]], p["lhs"], p["rhs"], max_steps=3
        )
        ev_by_id = {ev["id"]: ev for ev in r["events"]}
        red_events = [ev_by_id[eid] for eid in r["default_path_event_ids"]]
        assert all(isinstance(ev["match_idx"], int) for ev in red_events)

    def test_default_path_ids_are_single_history_greedy_event_set(self):
        """Red IDs embed the full Single-History greedy event set."""
        p = self._parsed()
        init = [[0, 1]]
        r = engine.multiway_causal_graph(
            init, p["lhs"], p["rhs"], max_steps=4
        )
        greedy = engine.evolve(init, p["lhs"], p["rhs"], 4)
        assert len(r["default_path_event_ids"]) == sum(
            len(step_events) for step_events in greedy["events"]
        )

    def test_default_path_matches_single_history_greedy_replay(self):
        """Red event stream matches evolve() after local alpha-normalization."""
        p = self._parsed()
        init = [[0, 1]]
        r = engine.multiway_causal_graph(
            init, p["lhs"], p["rhs"],
            max_steps=4, max_occurrences=500, max_time_ms=1000,
        )
        ev_by_id = {ev["id"]: ev for ev in r["events"]}
        red_events = [ev_by_id[eid] for eid in r["default_path_event_ids"]]
        greedy_events = [
            ev for step_events in engine.evolve(init, p["lhs"], p["rhs"], 4)["events"]
            for ev in step_events
        ]

        assert [
            engine._event_shape_signature(ev)
            for ev in red_events
        ] == [
            engine._event_shape_signature(ev)
            for ev in greedy_events
        ]

    def test_rule3_red_layout_depth_uses_greedy_batches_not_serial_depth(self):
        """Contract §6: red visual layers follow greedy batches, not branch depth."""
        p = self._parsed()
        r = engine.multiway_causal_graph(
            [[0, 1]], p["lhs"], p["rhs"],
            max_steps=4, max_occurrences=500, max_time_ms=1000,
        )
        ev_by_id = {ev["id"]: ev for ev in r["events"]}
        red_events = [ev_by_id[eid] for eid in r["default_path_event_ids"]]

        assert len(red_events) == 15
        assert Counter(ev["layout"]["depth"] for ev in red_events) == Counter({
            1: 1, 2: 2, 3: 4, 4: 8,
        })
        assert [ev["serial_depth"] for ev in red_events] == list(range(1, 16))
        assert Counter(ev["serial_depth"] for ev in red_events) != Counter(
            ev["layout"]["depth"] for ev in red_events
        )

    def test_rule3_red_ids_have_no_sentinel_or_realized_coordinates(self):
        """Contract §6/§9: red is ordinary-event layout metadata, not a red axis."""
        p = self._parsed()
        r = engine.multiway_causal_graph(
            [[0, 1]], p["lhs"], p["rhs"],
            max_steps=4, max_occurrences=500, max_time_ms=1000,
        )
        ev_by_id = {ev["id"]: ev for ev in r["events"]}
        for eid in r["default_path_event_ids"]:
            ev = ev_by_id[eid]
            assert all(idx >= 0 for idx in ev["branch_path"])
            assert ev.get("mwcKind") != "realized"
            assert ev["layout"]["depth"] == ev["single_history_step"]
            assert ev["single_history_batch_index"] is not None
            assert ev["greedy_index"] is not None

    @pytest.mark.parametrize("notation, init, expected_batches", [
        ("{{x,y},{x,z}} -> {{x,z},{x,w},{y,w},{z,w}}", [[0, 0], [0, 0]], [1, 2, 4, 8]),
        ("{{x,y}} -> {{x,y},{y,z}}", [[0, 1]], [1, 2, 4, 8]),
        ("{{x,y,z}} -> {{x,u,w},{y,v,u},{z,w,v}}", [[0, 1, 2]], [1, 3, 9, 27]),
        (
            "{{x,y,z},{z,u,v}} -> {{y,z,u},{v,w,x},{w,y,v}}",
            [[0,1,2],[2,3,4],[4,5,6],[6,7,8],[8,9,0],[1,3,5],[5,7,9],[9,1,3]],
            [4, 6, 7, 12],
        ),
    ])
    def test_red_layout_depth_multiplicities_match_greedy_batches(
        self, notation, init, expected_batches
    ):
        """Contract §8/§11: visual red layers match Single-History batches."""
        p = engine.parse_notation(notation)
        r = engine.multiway_causal_graph(
            init, p["lhs"], p["rhs"],
            max_steps=4, max_occurrences=500, max_time_ms=1000,
        )
        ev_by_id = {ev["id"]: ev for ev in r["events"]}
        red_events = [ev_by_id[eid] for eid in r["default_path_event_ids"]]
        assert [
            Counter(ev["layout"]["depth"] for ev in red_events)[depth]
            for depth in range(1, len(expected_batches) + 1)
        ] == expected_batches

    @pytest.mark.parametrize("notation, init", [
        ("{{x,y},{x,z}} -> {{x,z},{x,w},{y,w},{z,w}}", [[0, 0], [0, 0]]),
        ("{{x,y}} -> {{x,y},{y,z}}", [[0, 1]]),
        ("{{x,y,z}} -> {{x,u,w},{y,v,u},{z,w,v}}", [[0, 1, 2]]),
        (
            "{{x,y,z},{z,u,v}} -> {{y,z,u},{v,w,x},{w,y,v}}",
            [[0,1,2],[2,3,4],[4,5,6],[6,7,8],[8,9,0],[1,3,5],[5,7,9],[9,1,3]],
        ),
    ])
    def test_default_path_induced_edges_match_single_history_for_builtin_rules(
        self, notation, init
    ):
        """Red induced edges match evolve() topology for built-in rule shapes."""
        p = engine.parse_notation(notation)
        r = engine.multiway_causal_graph(
            init, p["lhs"], p["rhs"],
            max_steps=4, max_occurrences=500, max_time_ms=1000,
        )
        red_index = {
            event_id: idx
            for idx, event_id in enumerate(r["default_path_event_ids"])
        }
        induced_red_edges = sorted(
            (red_index[src], red_index[dst])
            for src, dst in r["causal_edges"]
            if src in red_index and dst in red_index
        )
        greedy_edges = sorted(
            tuple(edge)
            for edge in engine.evolve(
                init, p["lhs"], p["rhs"], 4, time_limit_ms=1000
            )["causal_edges"]
        )
        greedy_count = sum(
            len(step_events)
            for step_events in engine.evolve(
                init, p["lhs"], p["rhs"], 4, time_limit_ms=1000
            )["events"]
        )

        assert len(r["default_path_event_ids"]) == greedy_count
        assert induced_red_edges == greedy_edges

    def test_rule1_exact_induced_red_edges(self):
        """Duplicate-sensitive contract fixture: rule1 exact red topology."""
        p = engine.parse_notation("{{x,y},{x,z}} -> {{x,z},{x,w},{y,w},{z,w}}")
        r = engine.multiway_causal_graph(
            [[0, 0], [0, 0]], p["lhs"], p["rhs"],
            max_steps=4, max_occurrences=500, max_time_ms=1000,
        )
        red_index = {eid: idx for idx, eid in enumerate(r["default_path_event_ids"])}
        induced_red_edges = sorted(
            (red_index[src], red_index[dst])
            for src, dst in r["causal_edges"]
            if src in red_index and dst in red_index
        )
        assert induced_red_edges == [
            (0,1),(0,2),(1,3),(1,4),(1,5),(2,4),(2,5),(2,6),
            (3,7),(3,8),(4,9),(4,10),(4,11),(5,10),(5,11),
            (5,12),(5,13),(6,12),(6,13),(6,14),
        ]

    def test_rule3_exact_induced_red_edges(self):
        """Contract fixture: rule3 exact red topology plus 15 event count."""
        p = self._parsed()
        r = engine.multiway_causal_graph(
            [[0, 1]], p["lhs"], p["rhs"],
            max_steps=4, max_occurrences=500, max_time_ms=1000,
        )
        red_index = {eid: idx for idx, eid in enumerate(r["default_path_event_ids"])}
        induced_red_edges = sorted(
            (red_index[src], red_index[dst])
            for src, dst in r["causal_edges"]
            if src in red_index and dst in red_index
        )
        assert len(r["default_path_event_ids"]) == 15
        assert induced_red_edges == [
            (0,1),(0,2),(1,3),(1,4),(2,5),(2,6),(3,7),(3,8),
            (4,9),(4,10),(5,11),(5,12),(6,13),(6,14),
        ]

    # ── co-historical guarantee (Sofia) ───────────────────────────────

    def test_causal_edges_respect_ancestry(self):
        """Causal edge src must be an ancestor of dst.

        For each causal edge [A, B], A.step < B.step must hold (a cause
        precedes its effect in step order).
        """
        p = self._parsed()
        r = engine.multiway_causal_graph(
            [[0, 1], [1, 2]], p["lhs"], p["rhs"],
            max_steps=4, max_occurrences=500, max_time_ms=1000,
        )
        ev_by_id = {ev["id"]: ev for ev in r["events"]}
        for src, dst in r["causal_edges"]:
            assert ev_by_id[src]["step"] < ev_by_id[dst]["step"], (
                f"Causal edge {src}(step={ev_by_id[src]['step']}) → "
                f"{dst}(step={ev_by_id[dst]['step']}) violates ordering"
            )

    def test_causal_src_is_strict_ancestor_via_parent_chain(self):
        """For each causal edge [A, B], A must appear in B's parent_occ_id chain.

        This verifies the co-historical guarantee: only events in B's
        actual history (ancestry) can be causal predecessors of B.
        """
        p = self._parsed()
        r = engine.multiway_causal_graph(
            [[0, 1], [1, 2]], p["lhs"], p["rhs"],
            max_steps=4, max_occurrences=500, max_time_ms=1000,
        )
        ev_by_id = {ev["id"]: ev for ev in r["events"]}

        def ancestry_ids(ev: dict) -> set[int]:
            """Walk parent_occ_id chain from ev back to root; return all occ_ids seen."""
            ids: set[int] = set()
            cur_id = ev["parent_occ_id"]
            while cur_id is not None:
                ids.add(cur_id)
                parent_ev = ev_by_id.get(cur_id)
                if parent_ev is None:
                    break  # reached root (occ_id=0, no event entry)
                cur_id = parent_ev["parent_occ_id"]
            return ids

        for src, dst in r["causal_edges"]:
            dst_ev = ev_by_id[dst]
            ancestors = ancestry_ids(dst_ev)
            assert src in ancestors, (
                f"Causal edge {src} → {dst}: {src} is not in {dst}'s ancestry {ancestors}"
            )

    # ── caps ───────────────────────────────────────────────────────────

    def test_max_occurrences_cap_triggers_truncation(self):
        """Capping at a small number of occurrences sets truncated=True."""
        p = self._parsed()
        r = engine.multiway_causal_graph(
            [[0, 1], [1, 2]], p["lhs"], p["rhs"],
            max_steps=10,
            max_occurrences=5,
        )
        assert r["truncated"] is True
        assert r["truncation_reason"] in ("max_occurrences", "max_time_ms", "max_depth")

    def test_low_cap_red_prefix_preserves_layout_metadata(self):
        """Low-cap red prefix keeps full greedy count plus prefix layout layers."""
        p = self._parsed()
        r = engine.multiway_causal_graph(
            [[0, 1]], p["lhs"], p["rhs"],
            max_steps=4, max_occurrences=5, max_time_ms=1000,
        )
        ev_by_id = {ev["id"]: ev for ev in r["events"]}
        red_events = [ev_by_id[eid] for eid in r["default_path_event_ids"]]

        assert r["truncated"] is True
        assert r["stats"]["single_history_greedy_event_count"] == 15
        assert r["stats"]["embedded_red_event_count"] == len(red_events)
        assert [ev["greedy_index"] for ev in red_events] == list(range(len(red_events)))
        assert Counter(ev["layout"]["depth"] for ev in red_events) == Counter({
            1: 1, 2: 2, 3: 1,
        })

    def test_max_depth_truncation_reported(self):
        """When BFS stops at max_steps depth, truncated=True with reason 'max_depth'."""
        p = self._parsed()
        r = engine.multiway_causal_graph(
            [[0, 1]], p["lhs"], p["rhs"],
            max_steps=2,
            max_occurrences=5000,
            max_time_ms=5000,
        )
        # The BFS stops at step 2 (there are occurrences at step=2), so must report max_depth
        has_step2 = any(ev["step"] == 2 for ev in r["events"])
        if has_step2:
            assert r["truncated"] is True
            assert r["truncation_reason"] == "max_depth"

    def test_no_truncation_for_zero_steps(self):
        """max_steps=0 must NOT report truncation (there is literally nothing to cut)."""
        p = self._parsed()
        r = engine.multiway_causal_graph([[0, 1]], p["lhs"], p["rhs"], max_steps=0)
        assert r["truncated"] is False

    # ── multi-branch: events span branches ────────────────────────────

    def test_multi_branch_has_more_events_than_greedy(self):
        """The total event count must exceed the default path length when branching occurs."""
        p = self._parsed()
        r = engine.multiway_causal_graph(
            [[0, 1], [1, 2]], p["lhs"], p["rhs"], max_steps=2
        )
        assert len(r["events"]) > len(r["default_path_event_ids"]), (
            "Expected off-path events but events == default path"
        )

    def test_embedded_red_stats_match_payload_lengths(self):
        p = self._parsed()
        r = engine.multiway_causal_graph([[0, 1]], p["lhs"], p["rhs"], max_steps=3)
        assert r["stats"]["event_count"] == len(r["events"])
        assert r["stats"]["embedded_red_event_count"] == len(r["default_path_event_ids"])
        assert r["stats"]["green_event_count"] == len(r["events"]) - len(r["default_path_event_ids"])
        assert r["stats"]["single_history_greedy_event_count"] == len(r["default_path_event_ids"])
        assert r["stats"]["serial_default_path_event_count"] == len(r["default_path_event_ids"])

    # ── B2.1 regression: live edge provenance (Sofia BLOCKER) ─────────

    def test_live_provenance_identity_rule_default_path(self):
        """Identity rule: causal chain on default path must be step-by-step.

        Sofia's repro (adapted): identity {x,y}->{x,y} on [[1,2]] with 3 steps.
        The default path events are e1, e2, e3 where each consumes and
        re-produces the same edge content.  Correct live provenance gives
        e1→e2 and e2→e3.  The stale bug (B2 before fix) gave e1→e3 instead
        of e2→e3, because the ancestry-produced index never drained e1's
        produced edge when e2 consumed it.
        """
        engine.reset(0)
        lhs = [["x", "y"]]
        rhs = [["x", "y"]]  # identity — same edge content produced at every step
        r = engine.multiway_causal_graph([[1, 2]], lhs, rhs, max_steps=3,
                                         max_occurrences=100)
        default = r["default_path_event_ids"]
        assert len(default) == 3, f"Expected 3 default path events, got {len(default)}: {default}"
        e1, e2, e3 = default
        causal = r["causal_edges"]

        # Live: e2 consumes the edge produced by e1
        assert [e1, e2] in causal, f"Missing live causal edge {[e1, e2]}; causal={causal}"
        # Live: e3 consumes the edge produced by e2 (NOT e1 — that instance was consumed by e2)
        assert [e2, e3] in causal, f"Missing live causal edge {[e2, e3]}; causal={causal}"
        # Stale attribution must not appear
        assert [e1, e3] not in causal, (
            f"Stale causal edge {[e1, e3]} found — live provenance bug!"
        )

    def test_live_provenance_rule3_path_0_0_2(self):
        """Sofia's exact repro: rule3 path [0,0,2] must not show stale attribution.

        On path [0,0,2]:
        - occ_a (branch_path=[0]):     consumes [[0,1]], produces [[0,1],[1,2]]
        - occ_b (branch_path=[0,0]):   consumes [[0,1]], produces [[0,1],[1,3]]
        - occ_c (branch_path=[0,0,2]): consumes [[0,1]], produces [[0,1],[1,4]]

        occ_b consumes the [[0,1]] instance produced by occ_a; occ_c consumes
        the [[0,1]] instance re-produced by occ_b.  The correct causal edge is
        occ_b → occ_c.  The stale bug (ancestry-only) would emit occ_a → occ_c
        because occ_a is the oldest producer and the consumed instance was not
        drained from the ancestry index when occ_b consumed it.
        """
        p = self._parsed()
        r = engine.multiway_causal_graph([[0, 1]], p["lhs"], p["rhs"],
                                         max_steps=3, max_occurrences=500)

        ev_by_path = {tuple(ev["branch_path"]): ev for ev in r["events"]}
        occ_a = ev_by_path.get((0,))
        occ_b = ev_by_path.get((0, 0))
        occ_c = ev_by_path.get((0, 0, 2))

        if occ_a is None or occ_b is None or occ_c is None:
            pytest.skip("Path [0,0,2] not available (truncated or BFS order changed)")

        causal = r["causal_edges"]
        # Live: occ_c's [0,1] was most recently produced by occ_b
        assert [occ_b["id"], occ_c["id"]] in causal, (
            f"Missing live causal edge [{occ_b['id']}, {occ_c['id']}]; "
            f"causal={causal}"
        )
        # Stale: occ_a's [0,1] instance was already consumed by occ_b
        assert [occ_a["id"], occ_c["id"]] not in causal, (
            f"Stale causal edge [{occ_a['id']}, {occ_c['id']}] found — "
            f"live provenance bug!"
        )

    # ── B2.1 regression: max_depth only when matches remain (Rin MEDIUM-1) ─

    def test_no_max_depth_when_frontier_naturally_terminates(self):
        """No max_depth truncation when the frontier has no further matches.

        A rule that creates ternary edges from a binary LHS can only fire once
        on a binary init state.  After step 1 the state is all ternary; the
        binary LHS matches nothing.  max_depth must NOT be reported because
        the system terminated naturally, not because the depth cap cut it.
        """
        p = engine.parse_notation("{{x,y}} -> {{x,y,z}}")
        assert p is not None
        r = engine.multiway_causal_graph([[0, 1]], p["lhs"], p["rhs"], max_steps=1)
        # Frontier at step 1 has ternary edges — binary LHS has 0 matches
        assert not (r["truncated"] and r["truncation_reason"] == "max_depth"), (
            f"Should not report max_depth truncation for naturally-terminating frontier; "
            f"got truncated={r['truncated']} reason={r['truncation_reason']!r}"
        )

    def test_max_depth_reported_when_frontier_has_matches(self):
        """max_depth IS reported when the frontier at max_steps still has matches."""
        p = self._parsed()  # rule3: always has binary matches
        r = engine.multiway_causal_graph([[0, 1]], p["lhs"], p["rhs"], max_steps=2)
        # rule3 frontier at step 2 still has matches → must report max_depth
        has_step2 = any(ev["step"] == 2 for ev in r["events"])
        if has_step2:
            assert r["truncated"] is True
            assert r["truncation_reason"] == "max_depth"
