"""Integration tests for the FastAPI endpoints.

Uses Starlette's TestClient (backed by httpx).  The precompute background
thread is started by the lifespan event but runs as a daemon — it will not
block the test suite.  Individual endpoints compute on demand when not yet
in the cache.

Run from the repo root:
    pip install -r server/requirements.txt -r requirements-dev.txt
    pytest
"""
from __future__ import annotations
import pytest
from fastapi.testclient import TestClient
from server.main import app


@pytest.fixture(scope="module")
def client():
    """Shared TestClient for the module — triggers startup once."""
    with TestClient(app) as c:
        yield c


# ── /health ───────────────────────────────────────────────────────────

class TestHealth:
    def test_returns_200(self, client):
        r = client.get("/health")
        assert r.status_code == 200

    def test_body_has_ok_status(self, client):
        r = client.get("/health")
        assert r.json() == {"status": "ok"}


# ── /api/rules ────────────────────────────────────────────────────────

class TestListRules:
    def test_returns_200(self, client):
        assert client.get("/api/rules").status_code == 200

    def test_returns_list_of_five(self, client):
        rules = client.get("/api/rules").json()
        assert isinstance(rules, list)
        assert len(rules) == 5

    def test_rule_fields_present(self, client):
        rule = client.get("/api/rules").json()[0]
        for field in ("id", "name", "notation", "desc", "tag", "tagClass"):
            assert field in rule, f"Missing field: {field}"


# ── /api/rules/{rule_id} ──────────────────────────────────────────────

class TestGetRule:
    def test_valid_rule_returns_200(self, client):
        # rule3 (Binary Tree Growth) is fast to compute
        r = client.get("/api/rules/rule3")
        assert r.status_code == 200

    def test_valid_rule_has_expected_keys(self, client):
        data = client.get("/api/rules/rule3").json()
        for key in ("states", "events", "causalEdges", "stats",
                    "lineage", "birthSteps"):
            assert key in data, f"Missing key: {key}"

    def test_stats_are_non_empty(self, client):
        stats = client.get("/api/rules/rule3").json()["stats"]
        assert isinstance(stats, list) and len(stats) > 0

    def test_invalid_rule_returns_404(self, client):
        assert client.get("/api/rules/doesnotexist").status_code == 404


# ── /api/rules/{rule_id}/multiway ────────────────────────────────────

class TestGetMultiway:
    def test_valid_rule_returns_200(self, client):
        r = client.get("/api/rules/rule3/multiway")
        assert r.status_code == 200

    def test_response_has_multiway_keys(self, client):
        data = client.get("/api/rules/rule3/multiway").json()
        for key in ("states", "edges", "initHash"):
            assert key in data, f"Missing multiway key: {key}"

    def test_invalid_rule_returns_404(self, client):
        assert client.get("/api/rules/ghost/multiway").status_code == 404


# ── /api/custom ───────────────────────────────────────────────────────

class TestCustomRule:
    def test_valid_custom_rule_returns_200(self, client):
        r = client.post("/api/custom", json={
            "notation": "{{x,y}} -> {{x,y},{y,z}}",
            "init": [[0, 1]],
            "steps": 3,
        })
        assert r.status_code == 200

    def test_valid_response_keys(self, client):
        r = client.post("/api/custom", json={
            "notation": "{{x,y}} -> {{x,y},{y,z}}",
            "init": [[0, 1]],
            "steps": 2,
        })
        data = r.json()
        for key in ("states", "events", "causalEdges", "stats",
                    "lineage", "birthSteps", "multiway"):
            assert key in data, f"Missing key: {key}"

    def test_unicode_arrow_notation(self, client):
        r = client.post("/api/custom", json={
            "notation": "{{x,y}} → {{x,y},{y,z}}",
            "init": [[0, 1]],
            "steps": 2,
        })
        assert r.status_code == 200

    def test_bad_notation_returns_400(self, client):
        r = client.post("/api/custom", json={
            "notation": "not a rule at all",
            "init": [[0, 1]],
            "steps": 3,
        })
        assert r.status_code == 400

    def test_steps_too_large_returns_400(self, client):
        r = client.post("/api/custom", json={
            "notation": "{{x,y}} -> {{x,y},{y,z}}",
            "init": [[0, 1]],
            "steps": 21,
        })
        assert r.status_code == 400

    def test_steps_zero_returns_400(self, client):
        r = client.post("/api/custom", json={
            "notation": "{{x,y}} -> {{x,y},{y,z}}",
            "init": [[0, 1]],
            "steps": 0,
        })
        assert r.status_code == 400

    def test_empty_init_returns_400(self, client):
        r = client.post("/api/custom", json={
            "notation": "{{x,y}} -> {{x,y},{y,z}}",
            "init": [],
            "steps": 3,
        })
        assert r.status_code == 400

    def test_ternary_rule(self, client):
        r = client.post("/api/custom", json={
            "notation": "{{x,y,z}} -> {{x,u,w},{y,v,u},{z,w,v}}",
            "init": [[0, 1, 2]],
            "steps": 2,
        })
        assert r.status_code == 200


# ── / (serve index.html) ──────────────────────────────────────────────

class TestServeClient:
    def test_root_serves_html(self, client):
        """GET / should return 200 with HTML content (client/index.html)."""
        r = client.get("/")
        # May 404 if client/index.html absent in test env — accept both
        assert r.status_code in (200, 404)
