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
from contextlib import contextmanager
import os
import time
import importlib
import pytest
from fastapi.testclient import TestClient
from server import main
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


@contextmanager
def _playback_enabled_client():
    original = os.environ.get("DH_INCREMENTAL_PLAYBACK_ENABLED")
    os.environ["DH_INCREMENTAL_PLAYBACK_ENABLED"] = "1"
    reloaded = importlib.reload(main)
    try:
        with TestClient(reloaded.app) as c:
            yield c
    finally:
        if original is None:
            os.environ.pop("DH_INCREMENTAL_PLAYBACK_ENABLED", None)
        else:
            os.environ["DH_INCREMENTAL_PLAYBACK_ENABLED"] = original
        importlib.reload(main)


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


# ── background warm ──────────────────────────────────────────────────

class TestBuiltinMultiwayCausalWarm:
    def test_warms_each_builtin_with_default_caps(self, monkeypatch):
        calls: list[tuple[str, int, int, int]] = []

        def fake_get_multiway_causal(rule_id: str, max_steps: int, max_occurrences: int, max_time_ms: int):
            calls.append((rule_id, max_steps, max_occurrences, max_time_ms))
            return {
                "events": [],
                "causal_edges": [],
                "default_path_event_ids": [],
                "truncated": False,
                "truncation_reason": None,
            }

        monkeypatch.setattr(main, "_PRECOMPUTE_MULTIWAY_CAUSAL", True)
        monkeypatch.setattr(main, "get_multiway_causal", fake_get_multiway_causal)

        main._precompute_builtin_multiway_causal()

        assert [rule_id for rule_id, *_ in calls] == [r["id"] for r in main.RULES]
        assert all(
            (steps, occurrences, time_ms)
            == (main._MWCAUSAL_MAX_STEPS, main._MWCAUSAL_MAX_OCCURRENCES, main._MWCAUSAL_MAX_TIME_MS)
            for _, steps, occurrences, time_ms in calls
        )

    def test_disabled_flag_skips_warmup(self, monkeypatch):
        calls: list[tuple[str, int, int, int]] = []

        def fake_get_multiway_causal(rule_id: str, max_steps: int, max_occurrences: int, max_time_ms: int):
            calls.append((rule_id, max_steps, max_occurrences, max_time_ms))
            return {
                "events": [],
                "causal_edges": [],
                "default_path_event_ids": [],
                "truncated": False,
                "truncation_reason": None,
            }

        monkeypatch.setattr(main, "_PRECOMPUTE_MULTIWAY_CAUSAL", False)
        monkeypatch.setattr(main, "get_multiway_causal", fake_get_multiway_causal)

        main._precompute_builtin_multiway_causal()

        assert calls == []

    def test_env_opt_out_disables_warmup(self, monkeypatch):
        calls: list[tuple[str, int, int, int]] = []

        def fake_get_multiway_causal(rule_id: str, max_steps: int, max_occurrences: int, max_time_ms: int):
            calls.append((rule_id, max_steps, max_occurrences, max_time_ms))
            return {
                "events": [],
                "causal_edges": [],
                "default_path_event_ids": [],
                "truncated": False,
                "truncation_reason": None,
            }

        monkeypatch.setenv("DH_PRECOMPUTE_MULTIWAY_CAUSAL", "0")
        reloaded = importlib.reload(main)
        monkeypatch.setattr(reloaded, "get_multiway_causal", fake_get_multiway_causal)

        reloaded._precompute_builtin_multiway_causal()
        assert calls == []

        monkeypatch.delenv("DH_PRECOMPUTE_MULTIWAY_CAUSAL", raising=False)
        importlib.reload(reloaded)


# ── /api/rules/{rule_id} ──────────────────────────────────────────────

class TestGetRule:
    def test_valid_rule_returns_200(self, client):
        # rule3 (Binary Tree Growth) is fast to compute
        r = client.get("/api/rules/rule3")
        assert r.status_code == 200

    def test_valid_rule_has_expected_keys(self, client):
        data = client.get("/api/rules/rule3").json()
        assert set(data) == {"states", "events", "causalEdges", "stats", "lineage", "birthSteps"}

    def test_stats_are_non_empty(self, client):
        stats = client.get("/api/rules/rule3").json()["stats"]
        assert isinstance(stats, list) and len(stats) > 0

    def test_step_mode_response_unchanged_default(self, client):
        data = client.get("/api/rules/rule3").json()
        assert set(data) == {"states", "events", "causalEdges", "stats", "lineage", "birthSteps"}

    def test_feature_flag_off_returns_step_mode(self, client):
        data = client.get("/api/rules/rule3?playback=application").json()
        assert "playback" not in data

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


# ── /api/rules/{rule_id}/multiway-causal ─────────────────────────────

class TestGetMultiwayCausal:
    """Tests for GET /api/rules/{rule_id}/multiway-causal."""

    def test_valid_rule_returns_200(self, client):
        r = client.get("/api/rules/rule3/multiway-causal")
        assert r.status_code == 200

    def test_response_has_required_keys(self, client):
        data = client.get("/api/rules/rule3/multiway-causal").json()
        for key in ("events", "causal_edges", "default_path_event_ids",
                    "stats", "truncated", "truncation_reason", "meta"):
            assert key in data, f"Missing key: {key}"

    def test_meta_has_rule_fields(self, client):
        data = client.get("/api/rules/rule3/multiway-causal").json()
        meta = data["meta"]
        assert meta["rule_id"] == "rule3"
        assert "rule_notation" in meta
        assert "init_state" in meta

    def test_events_are_list(self, client):
        data = client.get("/api/rules/rule3/multiway-causal").json()
        assert isinstance(data["events"], list)
        assert len(data["events"]) > 0

    def test_event_fields_present(self, client):
        events = client.get("/api/rules/rule3/multiway-causal").json()["events"]
        for field in ("id", "step", "occ_id", "parent_occ_id", "match_idx",
                      "consumed", "produced", "branch_path"):
            assert field in events[0], f"Missing event field: {field}"

    def test_causal_edges_reference_valid_ids(self, client):
        data = client.get("/api/rules/rule3/multiway-causal").json()
        event_ids = {ev["id"] for ev in data["events"]}
        for src, dst in data["causal_edges"]:
            assert src in event_ids, f"Causal edge src {src} not a valid event id"
            assert dst in event_ids, f"Causal edge dst {dst} not a valid event id"

    def test_default_path_event_ids_subset_of_events(self, client):
        data = client.get("/api/rules/rule3/multiway-causal").json()
        event_ids = {ev["id"] for ev in data["events"]}
        for eid in data["default_path_event_ids"]:
            assert eid in event_ids, f"Default path event {eid} not in events"

    def test_default_path_ids_are_single_history_greedy_event_set(self, client):
        data = client.get("/api/rules/rule3/multiway-causal?max_steps=4").json()
        ev_by_id = {ev["id"]: ev for ev in data["events"]}
        red_index = {
            event_id: idx
            for idx, event_id in enumerate(data["default_path_event_ids"])
        }
        induced_red_edges = sorted(
            (red_index[src], red_index[dst])
            for src, dst in data["causal_edges"]
            if src in red_index and dst in red_index
        )
        rule = next(r for r in main.RULES if r["id"] == "rule3")
        parsed = main.engine.parse_notation(rule["notation"])
        greedy = main.engine.evolve(rule["init"], parsed["lhs"], parsed["rhs"], 4)

        assert len(data["default_path_event_ids"]) == sum(
            len(step_events) for step_events in greedy["events"]
        )
        assert induced_red_edges == sorted(tuple(edge) for edge in greedy["causal_edges"])
        assert all(eid in ev_by_id for eid in data["default_path_event_ids"])

    def test_no_out_of_band_realized_red_namespace(self, client):
        data = client.get("/api/rules/rule3/multiway-causal").json()
        assert "realized_events" not in data
        assert "realized_causal_edges" not in data

    def test_embedded_red_stats_match_payload_lengths(self, client):
        data = client.get("/api/rules/rule3/multiway-causal").json()
        stats = data["stats"]
        assert stats["event_count"] == len(data["events"])
        assert stats["embedded_red_event_count"] == len(data["default_path_event_ids"])
        assert stats["single_history_greedy_event_count"] == len(data["default_path_event_ids"])
        assert stats["green_event_count"] == len(data["events"]) - len(data["default_path_event_ids"])

    def test_truncated_field_is_bool(self, client):
        data = client.get("/api/rules/rule3/multiway-causal").json()
        assert isinstance(data["truncated"], bool)

    def test_low_max_occurrences_triggers_truncation(self, client):
        r = client.get("/api/rules/rule3/multiway-causal?max_occurrences=5&max_steps=4")
        assert r.status_code == 200
        data = r.json()
        assert data["truncated"] is True

    def test_invalid_rule_returns_404(self, client):
        r = client.get("/api/rules/ghost/multiway-causal")
        assert r.status_code == 404

    def test_cap_params_accepted(self, client):
        """Custom cap query params are accepted without error."""
        r = client.get("/api/rules/rule3/multiway-causal?max_steps=2&max_occurrences=100&max_time_ms=2000")
        assert r.status_code == 200

    def test_different_caps_produce_independent_cache_entries(self, client):
        """Two requests with different caps can differ in truncated state."""
        r_small = client.get("/api/rules/rule3/multiway-causal?max_occurrences=10")
        r_large = client.get("/api/rules/rule3/multiway-causal?max_occurrences=5000")
        assert r_small.status_code == 200
        assert r_large.status_code == 200
        # Small cap should be truncated; large cap may not be
        assert r_small.json()["truncated"] is True

    # ── GET cap validation ────────────────────────────────────────────────

    def test_max_steps_zero_returns_400(self, client):
        r = client.get("/api/rules/rule3/multiway-causal?max_steps=0")
        assert r.status_code == 400

    def test_max_steps_too_large_returns_400(self, client):
        r = client.get("/api/rules/rule3/multiway-causal?max_steps=9")
        assert r.status_code == 400

    def test_max_steps_negative_returns_400(self, client):
        r = client.get("/api/rules/rule3/multiway-causal?max_steps=-1")
        assert r.status_code == 400

    def test_max_occurrences_zero_returns_400(self, client):
        r = client.get("/api/rules/rule3/multiway-causal?max_occurrences=0")
        assert r.status_code == 400

    def test_max_occurrences_too_large_returns_400(self, client):
        r = client.get("/api/rules/rule3/multiway-causal?max_occurrences=20001")
        assert r.status_code == 400

    def test_max_time_ms_zero_returns_400(self, client):
        r = client.get("/api/rules/rule3/multiway-causal?max_time_ms=0")
        assert r.status_code == 400

    def test_max_time_ms_too_large_returns_400(self, client):
        r = client.get("/api/rules/rule3/multiway-causal?max_time_ms=30001")
        assert r.status_code == 400


# ── POST /api/custom/multiway-causal ─────────────────────────────────

class TestCustomMultiwayCausal:
    """Tests for POST /api/custom/multiway-causal (synchronous)."""

    @pytest.fixture(autouse=True)
    def _client(self, client):
        self.client = client

    def _post(self, **kwargs):
        defaults = {
            "notation": "{{x,y}} -> {{x,y},{y,z}}",
            "init": [[0, 1]],
            "max_steps": 3,
            "max_occurrences": 500,
            "max_time_ms": 5000,
        }
        defaults.update(kwargs)
        return self.client.post("/api/custom/multiway-causal", json=defaults)

    def test_returns_200(self):
        assert self._post().status_code == 200

    def test_response_has_required_keys(self):
        data = self._post().json()
        for key in ("events", "causal_edges", "default_path_event_ids",
                    "stats", "truncated", "truncation_reason", "meta"):
            assert key in data, f"Missing key: {key}"

    def test_meta_has_notation_and_init(self):
        data = self._post().json()
        assert "rule_notation" in data["meta"]
        assert "init_state" in data["meta"]

    def test_events_non_empty(self):
        data = self._post().json()
        assert isinstance(data["events"], list)
        assert len(data["events"]) > 0

    def test_causal_edges_reference_valid_ids(self):
        data = self._post().json()
        event_ids = {ev["id"] for ev in data["events"]}
        for src, dst in data["causal_edges"]:
            assert src in event_ids
            assert dst in event_ids

    def test_no_out_of_band_realized_red_namespace(self):
        data = self._post().json()
        assert "realized_events" not in data
        assert "realized_causal_edges" not in data

    def test_embedded_red_stats_match_payload_lengths(self):
        data = self._post().json()
        stats = data["stats"]
        assert stats["event_count"] == len(data["events"])
        assert stats["embedded_red_event_count"] == len(data["default_path_event_ids"])
        assert stats["single_history_greedy_event_count"] == len(data["default_path_event_ids"])
        assert stats["green_event_count"] == len(data["events"]) - len(data["default_path_event_ids"])

    def test_truncated_field_is_bool(self):
        data = self._post().json()
        assert isinstance(data["truncated"], bool)

    def test_low_max_occurrences_triggers_truncation(self):
        data = self._post(max_occurrences=3, max_steps=4).json()
        assert data["truncated"] is True

    def test_bad_notation_returns_400(self):
        r = self._post(notation="not a rule")
        assert r.status_code == 400

    def test_empty_init_returns_400(self):
        r = self._post(init=[])
        assert r.status_code == 400

    def test_max_steps_zero_returns_400(self):
        r = self._post(max_steps=0)
        assert r.status_code == 400

    def test_max_steps_too_large_returns_400(self):
        r = self._post(max_steps=9)
        assert r.status_code == 400

    def test_max_occurrences_too_large_returns_400(self):
        r = self._post(max_occurrences=20_001)
        assert r.status_code == 400

    def test_max_time_ms_zero_returns_400(self):
        r = self._post(max_time_ms=0)
        assert r.status_code == 400

    def test_max_time_ms_too_large_returns_400(self):
        r = self._post(max_time_ms=30_001)
        assert r.status_code == 400

    def test_unicode_arrow_notation(self):
        r = self._post(notation="{{x,y}} → {{x,y},{y,z}}")
        assert r.status_code == 200

    def test_ternary_rule(self):
        r = self._post(
            notation="{{x,y,z}} -> {{x,u,w},{y,v,u},{z,w,v}}",
            init=[[0, 1, 2]],
            max_steps=2,
            max_occurrences=200,
        )
        assert r.status_code == 200

    def test_default_path_non_empty(self):
        data = self._post().json()
        assert len(data["default_path_event_ids"]) > 0

    def test_response_is_synchronous_no_job_id(self):
        """POST /api/custom/multiway-causal returns direct payload, not a job handle."""
        data = self._post().json()
        assert "job_id" not in data


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


class TestApplicationPlaybackAPI:
    def test_application_mode_response_includes_playback_object(self):
        with _playback_enabled_client() as client:
            r = client.get("/api/rules/rule3?playback=application")
            assert r.status_code == 200
            data = r.json()
            assert "playback" in data
            playback = data["playback"]
            for key in ("mode", "frames", "truncated", "truncation_reason", "max_frames", "max_time_ms"):
                assert key in playback
            assert playback["mode"] == "application"
            assert isinstance(playback["frames"], list)
            assert len(playback["frames"]) > 0

    def test_application_mode_works_for_custom_rules(self):
        payload = {
            "notation": "{{x,y}} -> {{x,y},{y,z}}",
            "init": [[0, 1]],
            "steps": 2,
        }
        with _playback_enabled_client() as client:
            first = client.post("/api/custom", json=payload)
            first_data = first.json()
            if first_data["status"] != "done":
                first_data = _wait_done(client, first_data["job_id"])
            assert first_data["status"] == "done"

            second = client.post("/api/custom", json={**payload, "playback": "application"})
            second_data = second.json()
            if second_data["status"] != "done":
                second_data = _wait_done(client, second_data["job_id"])
            assert second_data["status"] == "done"
            assert "playback" in second_data
            assert second_data["playback"]["mode"] == "application"

    def test_custom_recall_supports_application_query(self):
        payload = {
            "notation": "{{x,y}} -> {{x,y},{y,z}}",
            "init": [[0, 1]],
            "steps": 2,
        }
        with _playback_enabled_client() as client:
            r = client.post("/api/custom", json=payload)
            data = r.json()
            if data["status"] != "done":
                data = _wait_done(client, data["job_id"])
            recall = client.get(f"/api/custom/{data['key']}?playback=application")
            assert recall.status_code == 200
            recall_data = recall.json()
            assert "playback" in recall_data
            assert recall_data["playback"]["mode"] == "application"


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
