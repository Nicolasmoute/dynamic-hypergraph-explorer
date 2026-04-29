"""Pytest configuration and shared fixtures.

Provides:
  --run-slow  CLI flag  — opt-in to slow (Playwright browser) tests.
  live_server           — session-scoped fixture that spawns a real uvicorn
                          server process and yields its base URL.
"""
from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Slow-test opt-in
# ---------------------------------------------------------------------------

def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--run-slow",
        action="store_true",
        default=False,
        help="Run slow tests (Playwright browser smoke tests). "
             "Requires 'playwright install chromium' to have been run first.",
    )


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "slow: marks tests as slow — skipped unless --run-slow is passed",
    )


def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item]
) -> None:
    if config.getoption("--run-slow"):
        return  # run everything
    skip_slow = pytest.mark.skip(reason="pass --run-slow to run Playwright tests")
    for item in items:
        if "slow" in item.keywords:
            item.add_marker(skip_slow)


# ---------------------------------------------------------------------------
# live_server fixture
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).parent.parent  # /repo/shared


def _free_port() -> int:
    """Return an ephemeral TCP port that is free at the moment of calling."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


def _wait_for_server(url: str, retries: int = 40, interval: float = 0.25) -> bool:
    """Poll the /health endpoint until the server responds or we time out."""
    health_url = f"{url}/health"
    for _ in range(retries):
        try:
            with urllib.request.urlopen(health_url, timeout=1):
                return True
        except Exception:
            time.sleep(interval)
    return False


@pytest.fixture(scope="session")
def live_server(tmp_path_factory: pytest.TempPathFactory) -> str:
    """Start a real uvicorn server; yield its base URL; terminate on teardown.

    Uses a temporary directory for the disk cache so test runs never share
    cached state with each other or with the developer's local cache.
    """
    port = _free_port()
    cache_dir = str(tmp_path_factory.mktemp("dh_cache"))
    env = {**os.environ, "DH_CACHE_DIR": cache_dir}

    proc = subprocess.Popen(
        [
            sys.executable, "-m", "uvicorn",
            "server.main:app",
            "--host", "127.0.0.1",
            "--port", str(port),
            "--log-level", "error",
        ],
        cwd=str(REPO_ROOT),
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    base_url = f"http://127.0.0.1:{port}"
    if not _wait_for_server(base_url):
        proc.kill()
        proc.wait()
        pytest.fail(
            f"Live server on port {port} did not become ready within 10 s. "
            "Check that 'uvicorn' is installed (server/requirements.txt)."
        )

    yield base_url

    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
