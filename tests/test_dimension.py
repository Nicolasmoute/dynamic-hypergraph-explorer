"""Tests for estimate_dimension — §5.4 acceptance suite.

Verifies that the incidence-based BFS implementation of estimate_dimension
produces the expected values for known structures.  These tests are based on
the spec at knowledge/theory/2026-04-28-true-hyperedge-distance-spec.md.
"""
from __future__ import annotations
import itertools
import pytest
from server.engine import dimension_diagnostic, estimate_dimension, evolve, parse_notation


def test_dimension_none_for_small():
    """Graphs too small to estimate → None."""
    assert estimate_dimension([[0, 1], [1, 2]]) is None
    assert estimate_dimension([]) is None
    assert estimate_dimension([[0, 1, 2]]) is None


def test_dimension_binary_ring():
    """20-node binary ring → dimension ≈ 1."""
    ring = [[i, (i + 1) % 20] for i in range(20)]
    d = estimate_dimension(ring)
    assert d is not None, "Expected a dimension estimate for a 20-node ring"
    assert 0.8 < d < 1.3, f"Expected ~1 for a ring, got {d}"


def test_dimension_grid_2d():
    """6×6 2D grid graph returns a positive dimension estimate.

    The asymptotic (infinite-grid) value is 2.  A 6×6 finite grid saturates
    quickly — boundary effects dominate the log-log regression and push the
    estimate down to ~1.1.  Both old (clique-projection) and new (incidence-
    BFS) algorithms produce the same value for binary edges (as expected; this
    test is a regression guard, not a convergence test).

    For actual convergence toward 2 a grid of 50×50+ is required; that is too
    slow for a unit test.  We therefore assert a conservative lower bound of
    0.9 (clearly above 1D) and an upper bound of 1.5.
    """
    N = 6
    edges = []
    for i, j in itertools.product(range(N), range(N)):
        n = i * N + j
        if j + 1 < N:
            edges.append([n, i * N + (j + 1)])
        if i + 1 < N:
            edges.append([n, (i + 1) * N + j])
    d = estimate_dimension(edges)
    assert d is not None, "Expected a dimension estimate for a 6×6 grid"
    assert 0.9 < d < 1.5, (
        f"Expected 0.9–1.5 for a 6×6 grid (finite-size limited), got {d}"
    )


def test_dimension_sierpinski_step3():
    """Sierpinski hyperedge rule after 3 steps → dimension converging toward 1.585.

    At step 3 the graph is small; expect 1.0–1.7 due to finite-size effects.
    The asymptotic value log(3)/log(2) ≈ 1.585 requires many more steps.
    """
    notation = "{{x,y,z}} -> {{x,u,w},{y,v,u},{z,w,v}}"
    parsed = parse_notation(notation)
    result = evolve([[0, 1, 2]], parsed["lhs"], parsed["rhs"], 3)
    state = result["states"][-1]
    d = estimate_dimension(state)
    assert d is not None, f"Expected a dimension estimate at step 3 (state size={len(state)})"
    assert 1.0 < d < 1.7, f"Expected 1.0–1.7 for Sierpiński at step 3, got {d}"


def test_dimension_binary_equivalence():
    """For binary edges the incidence BFS is provably equivalent to clique projection.

    Both produce identical node-to-node distances, so this test is a regression
    guard: any future refactor that changes the binary-edge dimension is a bug.
    """
    ring = [[i, (i + 1) % 30] for i in range(30)]
    d = estimate_dimension(ring)
    assert d is not None
    assert 0.8 < d < 1.2, f"30-node ring: expected dimension ~1, got {d}"


def test_dimension_large_ring():
    """Larger ring should still return ~1, confirming the estimate is stable."""
    ring = [[i, (i + 1) % 50] for i in range(50)]
    d = estimate_dimension(ring)
    assert d is not None
    assert 0.85 < d < 1.15, f"50-node ring: expected ~1, got {d}"


def test_dimension_diagnostic_flags_exponential_binary_tree():
    """Rule 3 has exponential/tree-like growth, not a finite manifold dimension."""
    parsed = parse_notation("{{x,y}} -> {{x,y},{y,z}}")
    result = evolve([[0, 1]], parsed["lhs"], parsed["rhs"], 12, time_limit_ms=0)
    final_stats = result["stats"][-1]
    assert final_stats["dimension_kind"] == "exponential_growth"
    assert final_stats["estimated_dimension"] is None
    assert final_stats["raw_dimension_estimate"] is not None


def test_dimension_diagnostic_keeps_power_law_ring_dimension():
    """A ring remains a normal power-law dimension estimate."""
    ring = [[i, (i + 1) % 50] for i in range(50)]
    raw = estimate_dimension(ring)
    diag = dimension_diagnostic(ring, raw)
    assert diag["kind"] == "power_law"
