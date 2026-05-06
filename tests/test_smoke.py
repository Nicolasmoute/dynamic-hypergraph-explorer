"""Smoke tests — serving layer and basic liveness.

These tests validate the "does the deployed app return the right bytes?"
question without requiring a live server.  They run via Starlette's
TestClient and complement the deeper unit/integration tests in
test_engine.py and test_api.py.

For testing against a live Zeabur deployment, see:
  knowledge/devx/deploy-verification-plan.md
"""
from __future__ import annotations
from pathlib import Path
import pytest
from fastapi.testclient import TestClient
from server.main import app


@pytest.fixture(scope="module")
def client():
    with TestClient(app) as c:
        yield c


class TestLiveness:
    def test_health_200(self, client):
        assert client.get("/health").status_code == 200

    def test_health_status_ok(self, client):
        assert client.get("/health").json()["status"] == "ok"

    def test_health_uptime_non_negative(self, client):
        assert client.get("/health").json()["uptime_s"] >= 0

    def test_health_version_present(self, client):
        v = client.get("/health").json()["version"]
        assert isinstance(v, str) and len(v) > 0


class TestClientServing:
    """Verify static client files are served with correct MIME types.
    Accepts 404 gracefully if client/ is absent in the test environment.
    """
    def test_root_returns_html_or_404(self, client):
        assert client.get("/").status_code in (200, 404)

    def test_root_content_type_is_html(self, client):
        r = client.get("/")
        if r.status_code == 200:
            assert "text/html" in r.headers.get("content-type", "")

    def test_app_js_returns_javascript_or_404(self, client):
        assert client.get("/app.js").status_code in (200, 404)

    def test_app_js_content_type_is_javascript(self, client):
        r = client.get("/app.js")
        if r.status_code == 200:
            assert "javascript" in r.headers.get("content-type", "")

    def test_multiway_causal_red_uses_single_history_greedy_events(self):
        app_js = Path(__file__).resolve().parents[1] / "client" / "app.js"
        text = app_js.read_text()
        assert "default_path_event_ids" in text
        assert "data.realized_events" not in text
        assert "data.realized_causal_edges" not in text
        assert "Red = Single-History greedy events" in text
        assert "serial occurrence path" not in text


class TestAPISurface:
    """Minimal API checks that gate the UI's ability to render at all."""
    def test_rules_count(self, client):
        assert len(client.get("/api/rules").json()) == 5

    def test_rule_ids_match_expected(self, client):
        ids = {r["id"] for r in client.get("/api/rules").json()}
        assert ids == {"rule1", "rule2", "rule3", "rule4", "rule5"}

    def test_each_rule_has_notation(self, client):
        for rule in client.get("/api/rules").json():
            assert "notation" in rule and rule["notation"]

    def test_custom_endpoint_accepts_minimal_input(self, client):
        r = client.post("/api/custom", json={
            "notation": "{{x,y}} -> {{x,y},{y,z}}",
            "init": [[0, 1]], "steps": 1,
        })
        assert r.status_code == 200
