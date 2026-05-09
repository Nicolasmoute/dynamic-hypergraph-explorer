"""Pre-warm the v12 disk cache synchronously before the server starts.

Called from start.sh *before* uvicorn so every first user request is a cache
hit (<1s) rather than a cold computation (up to ~15s for rule5 MWC).

The script is idempotent: each compute helper checks in-memory cache first,
then disk, then computes only if both miss.  On a second startup the disk
cache is already populated, so this runs in <1s.

Exit codes:
  0 — all entries warm (loaded from disk or freshly computed)
  1 — one or more entries failed; server still starts, failed entries will
      be computed on first request as before
"""
from __future__ import annotations

import logging
import sys
import time

logging.basicConfig(
    level=logging.INFO,
    format="[warmup] %(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("dh.warmup")


def main() -> int:
    t0 = time.time()
    log.info("starting cache warm-up")

    # Deferred import: logging must be configured first; also avoids pulling
    # in FastAPI/uvicorn before the warmup logger is ready.  The lifespan()
    # background thread only fires inside the ASGI server — importing
    # server.main here is safe.
    from server.main import (  # noqa: PLC0415
        RULES,
        CACHE,
        CACHE_DIR,
        _MWCAUSAL_MAX_STEPS,
        _MWCAUSAL_MAX_OCCURRENCES,
        _MWCAUSAL_MAX_TIME_MS,
        _PRECOMPUTE_MULTIWAY_CAUSAL,
        _preload_disk_cache,
        get_rule_data,
        get_multiway,
        get_multiway_causal,
    )

    loaded = _preload_disk_cache()
    log.info("loaded %d entry/entries from disk (CACHE_DIR=%s)", loaded, CACHE_DIR)

    errors = 0

    for rule in RULES:
        rid = rule["id"]

        # 1. Rule evolution (states / events / causal_edges)
        errors += _warm("rule-data", rid, lambda r=rid: get_rule_data(r))

        # 2. Multiway BFS
        errors += _warm("multiway", rid, lambda r=rid: get_multiway(r))

        # 3. Multiway-causal graph (most expensive; gated by the same flag as
        #    the background precompute so env-var opt-outs are respected)
        if _PRECOMPUTE_MULTIWAY_CAUSAL:
            errors += _warm(
                "multiway-causal",
                rid,
                lambda r=rid: get_multiway_causal(
                    r,
                    _MWCAUSAL_MAX_STEPS,
                    _MWCAUSAL_MAX_OCCURRENCES,
                    _MWCAUSAL_MAX_TIME_MS,
                ),
            )

    elapsed = time.time() - t0
    if errors:
        log.warning("warm-up finished with %d error(s) in %.1fs", errors, elapsed)
    else:
        log.info("warm-up complete — all entries ready in %.1fs", elapsed)

    return 1 if errors else 0


def _warm(label: str, rule_id: str, fn) -> int:
    """Call fn(), log timing, return 0 on success or 1 on error."""
    t = time.time()
    try:
        fn()
        log.info("  %-18s %-8s  %.1fs", label, rule_id, time.time() - t)
        return 0
    except Exception as exc:
        log.error("  %-18s %-8s  FAILED: %s", label, rule_id, exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
