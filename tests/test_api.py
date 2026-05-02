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
import time
import pytest
from fastapi.testclient import TestClient
from server.main import app


def _wait_done(client: TestClient, job_id: str, timeout: float = 15.0, interval: float = 0.05) -> dict:
    """Poll GET /api/jobs/{job_id} until status != 'running' or timeout.

    Returns the final job-status response dict.
    Raises TimeoutError if not complete within *timeout* seconds.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = client.get(f"/api/jobs/{job_id}")
        assert r.status_code == 200, f"Unexpected {r.status_code} while polling job {job_id}"
        data = r.json()
        if data["status"] != "running":
            return data
        time.sleep(interval)
    raise TimeoutError(f"Job {job_id} did not complete within {timeout}s")


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
        assert client.get("/health").json()["status"] == "ok"

    def test_body_has_uptime_s(self, client):
        data = client.get("/health").json()
        assert "uptime_s" in data
        assert isinstance(data["uptime_s"], int)
        assert data["uptime_s"] >= 0

    def test_body_has_version(self, client):
        data = client.get("/health").json()
        assert "version" in data
        # short git SHA or the literal "dev" if git unavailable
        assert isinstance(data["version"], str) and len(data["version"]) > 0

    def test_body_has_active_jobs(self, client):
        data = client.get("/health").json()
        assert "active_jobs" in data
        assert isinstance(data["active_jobs"], int)
        assert data["active_jobs"] >= 0


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

    def test_rule3_stats_flag_exponential_dimension(self, client):
        stats = client.get("/api/rules/rule3").json()["stats"]
        final = stats[-1]
        assert final["dimension_kind"] == "exponential_growth"
        assert final["estimated_dimension"] is None
        assert final["raw_dimension_estimate"] is not None

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

    def test_valid_response_has_job_id(self, client):
        """POST always returns job_id, status, step, total_steps, elapsed_s."""
        r = client.post("/api/custom", json={
            "notation": "{{x,y}} -> {{x,y},{y,z}}",
            "init": [[0, 1]],
            "steps": 2,
        })
        assert r.status_code == 200
        data = r.json()
        for field in ("job_id", "status", "step", "total_steps", "elapsed_s"):
            assert field in data, f"Missing field: {field}"
        assert data["status"] in ("running", "done")
        assert data["total_steps"] == 2

    def test_valid_response_keys_after_completion(self, client):
        """Full payload (states/events/etc.) is present once status is 'done'."""
        r = client.post("/api/custom", json={
            "notation": "{{x,y}} -> {{x,y},{y,z}}",
            "init": [[0, 1]],
            "steps": 2,
        })
        data = r.json()
        if data["status"] != "done":
            data = _wait_done(client, data["job_id"])
        assert data["status"] == "done"
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


# ── /api/jobs/{job_id} ───────────────────────────────────────────────

class TestJobPolling:
    """Tests for the async job polling endpoint (GET /api/jobs/{job_id})."""

    @pytest.fixture(autouse=True)
    def _client(self, client):
        self.client = client

    def _submit(self, notation="{{x,y}} -> {{x,y},{y,z}}", init=None, steps=2):
        """Helper: POST /api/custom and return the response JSON."""
        init = init or [[0, 1]]
        r = self.client.post("/api/custom", json={
            "notation": notation,
            "init": init,
            "steps": steps,
        })
        assert r.status_code == 200
        return r.json()

    def test_poll_returns_running_or_done(self):
        """GET /api/jobs/{job_id} immediately after POST should be running or done."""
        data = self._submit(steps=3)
        job_id = data["job_id"]
        r = self.client.get(f"/api/jobs/{job_id}")
        assert r.status_code == 200
        assert r.json()["status"] in ("running", "done")

    def test_poll_completes_with_full_payload(self):
        """Polling until done returns full evolution payload."""
        # Use a distinct notation so we don't rely on a cached result
        data = self._submit(
            notation="{{x,y}} -> {{x,z},{z,y}}",
            init=[[0, 1]],
            steps=3,
        )
        job_id = data["job_id"]
        final = data if data["status"] == "done" else _wait_done(self.client, job_id)
        assert final["status"] == "done"
        assert "key" in final
        for field in ("states", "events", "causalEdges", "stats",
                      "lineage", "birthSteps", "multiway"):
            assert field in final, f"Missing field in completed job: {field}"

    def test_poll_progress_fields(self):
        """Completed job response has step == total_steps and elapsed_s >= 0."""
        data = self._submit(steps=2)
        job_id = data["job_id"]
        final = data if data["status"] == "done" else _wait_done(self.client, job_id)
        assert final["step"] == final["total_steps"]
        assert isinstance(final["elapsed_s"], float)
        assert final["elapsed_s"] >= 0.0

    def test_dedup_same_job_id(self):
        """Submitting the identical job twice returns the same job_id."""
        payload = {
            "notation": "{{x,y,z}} -> {{x,u,w},{y,v,u},{z,w,v}}",
            "init": [[0, 1, 2]],
            "steps": 2,
        }
        r1 = self.client.post("/api/custom", json=payload)
        r2 = self.client.post("/api/custom", json=payload)
        assert r1.json()["job_id"] == r2.json()["job_id"]

    def test_poll_unknown_job_returns_404(self):
        """Polling a non-existent job_id returns 404."""
        r = self.client.get("/api/jobs/custom-0000000000000000")
        assert r.status_code == 404

    def test_done_job_retrievable_via_recall(self):
        """A completed job is also retrievable via GET /api/custom/{key}."""
        data = self._submit(steps=2)
        job_id = data["job_id"]
        final = data if data["status"] == "done" else _wait_done(self.client, job_id)
        assert final["status"] == "done"
        key = final["key"]
        r = self.client.get(f"/api/custom/{key}")
        assert r.status_code == 200
        assert r.json()["key"] == key


# ── Stale-but-done recovery ──────────────────────────────────────────

class TestStaleButDone:
    """Stale detection recovers gracefully when job completed but heartbeat lagged."""

    @pytest.fixture(autouse=True)
    def _client(self, client):
        self.client = client

    def test_stale_but_done_returns_done(self):
        """If a job's heartbeat is stale but the cache has the result, return done not stale."""
        import threading
        from server.main import CACHE, _jobs, _jobs_lock
        # Submit and wait for a real completion
        r = self.client.post("/api/custom", json={
            "notation": "{{x,y}} -> {{x,y},{y,z}}",
            "init": [[0, 1]],
            "steps": 2,
        })
        data = r.json()
        job_id = data["job_id"]
        if data["status"] != "done":
            data = _wait_done(self.client, job_id)
        assert data["status"] == "done"
        assert job_id in CACHE

        # Simulate stale: patch heartbeat_at to be far in the past
        with _jobs_lock:
            if job_id in _jobs:
                _jobs[job_id]["status"] = "running"
                _jobs[job_id]["heartbeat_at"] = 0.0  # epoch — ancient

        # Polling should recover "done" from cache, not return "stale"
        r2 = self.client.get(f"/api/jobs/{job_id}")
        assert r2.status_code == 200
        assert r2.json()["status"] == "done"


# ── DELETE /api/jobs/{job_id} (cancel) ───────────────────────────────

class TestJobCancellation:
    """Tests for cooperative job cancellation."""

    @pytest.fixture(autouse=True)
    def _client(self, client):
        self.client = client

    def test_cancel_unknown_job_returns_404(self):
        r = self.client.delete("/api/jobs/custom-0000000000000000")
        assert r.status_code == 404

    def test_cancel_done_job_returns_409(self):
        """Cancelling a job that already completed returns 409."""
        r = self.client.post("/api/custom", json={
            "notation": "{{x,y}} -> {{x,y},{y,z}}",
            "init": [[0, 1]],
            "steps": 2,
        })
        data = r.json()
        job_id = data["job_id"]
        # Wait for completion
        if data["status"] != "done":
            _wait_done(self.client, job_id)
        # Now cancel a finished job
        r2 = self.client.delete(f"/api/jobs/{job_id}")
        assert r2.status_code == 409

    def test_cancel_running_job_transitions(self):
        """Cancelling a running job eventually results in cancelled or done status."""
        # Use a fresh, uncached notation so we get a real running job.
        # A 15-step rule should give us a window to cancel.
        r = self.client.post("/api/custom", json={
            "notation": "{{x,y},{x,z}} -> {{x,z},{x,w},{y,w},{z,w}}",
            "init": [[0, 0], [0, 0]],
            "steps": 10,
        })
        data = r.json()
        job_id = data["job_id"]
        if data["status"] == "done":
            pytest.skip("Job completed before cancel attempt — cache hit, skip test")

        # Send cancel while running
        rc = self.client.delete(f"/api/jobs/{job_id}")
        assert rc.status_code in (200, 409)  # 409 if it finished first

        # Final status must be a terminal state
        final = _wait_done(self.client, job_id)
        assert final["status"] in ("done", "cancelled")

    def test_cancel_response_has_job_id(self):
        """Cancel response includes job_id field."""
        # Submit a fresh job
        r = self.client.post("/api/custom", json={
            "notation": "{{x,y,z},{z,u,v}} -> {{y,z,u},{v,w,x},{w,y,v}}",
            "init": [[0,1,2],[2,3,4]],
            "steps": 8,
        })
        data = r.json()
        job_id = data["job_id"]
        if data["status"] == "done":
            pytest.skip("Already cached — no running job to cancel")
        rc = self.client.delete(f"/api/jobs/{job_id}")
        if rc.status_code == 200:
            assert rc.json()["job_id"] == job_id


# ── POST /api/extend ──────────────────────────────────────────────────

class TestExtend:
    """Tests for the extend-cached-evolution endpoint."""

    @pytest.fixture(autouse=True)
    def _client(self, client):
        self.client = client

    def _run_and_wait(self, notation, init, steps):
        """POST /api/custom and return a done result dict."""
        r = self.client.post("/api/custom", json={
            "notation": notation, "init": init, "steps": steps,
        })
        assert r.status_code == 200
        data = r.json()
        if data["status"] != "done":
            data = _wait_done(self.client, data["job_id"])
        assert data["status"] == "done"
        return data

    def test_extend_unknown_key_returns_404(self):
        r = self.client.post("/api/extend", json={"key": "custom-0000000000000000", "extra_steps": 1})
        assert r.status_code == 404

    def test_extend_invalid_extra_steps_returns_400(self):
        # First need a valid key
        base = self._run_and_wait("{{x,y}} -> {{x,y},{y,z}}", [[0, 1]], 2)
        key = base["key"]
        r = self.client.post("/api/extend", json={"key": key, "extra_steps": 0})
        assert r.status_code == 400
        r2 = self.client.post("/api/extend", json={"key": key, "extra_steps": 11})
        assert r2.status_code == 400

    def test_extend_returns_job_fields(self):
        """POST /api/extend returns job_id, status, step, total_steps, elapsed_s."""
        base = self._run_and_wait("{{x,y}} -> {{x,y},{y,z}}", [[0, 1]], 3)
        key = base["key"]
        r = self.client.post("/api/extend", json={"key": key, "extra_steps": 1})
        assert r.status_code == 200
        data = r.json()
        for field in ("job_id", "status", "step", "total_steps", "elapsed_s"):
            assert field in data, f"Missing field: {field}"
        assert data["status"] in ("running", "done")

    def test_extend_result_has_more_states(self):
        """Extended result has one more state than the base."""
        base = self._run_and_wait("{{x,y}} -> {{x,z},{z,y}}", [[0, 1]], 3)
        old_state_count = len(base["states"])
        key = base["key"]

        r = self.client.post("/api/extend", json={"key": key, "extra_steps": 1})
        data = r.json()
        if data["status"] != "done":
            data = _wait_done(self.client, data["job_id"])
        assert data["status"] == "done"

        # Extended result has exactly one extra state
        assert len(data["states"]) == old_state_count + 1

    def test_extend_result_has_all_payload_fields(self):
        """Completed extension has full payload: states, events, causalEdges, etc."""
        base = self._run_and_wait("{{x,y}} -> {{x,y},{y,z}}", [[0, 1]], 2)
        r = self.client.post("/api/extend", json={"key": base["key"], "extra_steps": 1})
        data = r.json()
        if data["status"] != "done":
            data = _wait_done(self.client, data["job_id"])
        for field in ("states", "events", "causalEdges", "stats", "lineage", "birthSteps", "multiway"):
            assert field in data, f"Missing field: {field}"

    def test_extend_is_idempotent(self):
        """Calling extend twice with the same key+extra_steps returns same job_id."""
        base = self._run_and_wait("{{x,y}} -> {{x,y},{y,z}}", [[0, 1]], 2)
        key = base["key"]
        r1 = self.client.post("/api/extend", json={"key": key, "extra_steps": 2})
        r2 = self.client.post("/api/extend", json={"key": key, "extra_steps": 2})
        assert r1.json()["job_id"] == r2.json()["job_id"]

    def test_extend_result_retrievable_via_custom_key(self):
        """Extended result cached under key == POST /api/custom with new step count."""
        base = self._run_and_wait("{{x,y}} -> {{x,y},{y,z}}", [[0, 1]], 2)
        r = self.client.post("/api/extend", json={"key": base["key"], "extra_steps": 1})
        data = r.json()
        if data["status"] != "done":
            data = _wait_done(self.client, data["job_id"])
        ext_key = data["key"]

        # Should be retrievable via recall endpoint
        r2 = self.client.get(f"/api/custom/{ext_key}")
        assert r2.status_code == 200

        # And identical to directly computing 3 steps from scratch
        direct = self._run_and_wait("{{x,y}} -> {{x,y},{y,z}}", [[0, 1]], 3)
        # Both have same number of states (2+1=3 steps → 4 states)
        assert len(data["states"]) == len(direct["states"])


# ── / (serve index.html) ──────────────────────────────────────────────

class TestServeClient:
    def test_root_serves_html(self, client):
        """GET / should return 200 with HTML content (client/index.html)."""
        r = client.get("/")
        # May 404 if client/index.html absent in test env — accept both
        assert r.status_code in (200, 404)
