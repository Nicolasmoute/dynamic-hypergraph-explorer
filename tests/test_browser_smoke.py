"""Playwright browser smoke tests — §7.2 of the clean-up spec.

These tests drive a headless browser against a real running server to verify
end-to-end rendering.  They are marked ``slow`` and are **skipped by default**.

Run them with::

    pytest --run-slow tests/test_browser_smoke.py

Requirements (in addition to requirements-dev.txt)::

    playwright install chromium

CI: run in a separate job with ``--run-slow`` flag (see .github/workflows).
"""
from __future__ import annotations

import json
import urllib.request

import pytest
from playwright.sync_api import Page, expect

# Timeouts (milliseconds)
_LOAD_TIMEOUT = 20_000   # server startup + rule precompute on cold cache
_RENDER_TIMEOUT = 10_000  # D3 drawing after data arrives
_INTERACT_TIMEOUT = 5_000  # canvas update after a user interaction


@pytest.mark.slow
class TestBrowserSmoke:
    """End-to-end browser smoke tests via Playwright (headless Chromium)."""

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _wait_for_app_ready(self, page: Page, base_url: str) -> None:
        """Navigate to the app and wait until it is fully interactive.

        'Ready' means:
        - The loading overlay has disappeared (rules fetched from API).
        - At least one rule card is visible in the sidebar.
        - The main spatial SVG has been populated by D3.
        """
        page.goto(base_url)

        # Loading overlay disappears once /api/rules responds.
        page.wait_for_selector(
            "#loading-overlay",
            state="hidden",
            timeout=_LOAD_TIMEOUT,
        )

        # Rule cards must be present before we can interact with them.
        page.wait_for_selector(".rule-card", timeout=_RENDER_TIMEOUT)

        # Wait for the first rule's data to load and D3 to populate the SVG.
        page.wait_for_function(
            """() => {
                const svg = document.querySelector('#main-svg');
                return svg !== null && svg.children.length > 0;
            }""",
            timeout=_RENDER_TIMEOUT,
        )

    # ------------------------------------------------------------------
    # Tests
    # ------------------------------------------------------------------

    def test_page_loads_and_spatial_svg_has_children(
        self, live_server: str, page: Page
    ) -> None:
        """Page loads successfully; #main-svg is rendered with graph nodes/edges."""
        self._wait_for_app_ready(page, live_server)

        svg = page.locator("#main-svg")
        expect(svg).to_be_visible()

        child_count = page.evaluate(
            "() => document.querySelector('#main-svg').children.length"
        )
        assert child_count > 0, (
            f"Expected #main-svg to contain D3-rendered elements, got {child_count} children"
        )

    def test_second_rule_card_click_updates_canvas(
        self, live_server: str, page: Page
    ) -> None:
        """Clicking the second rule card loads its data and re-renders the canvas."""
        self._wait_for_app_ready(page, live_server)

        # Record current child count so we can detect a change.
        before = page.evaluate(
            "() => document.querySelector('#main-svg').children.length"
        )

        # Click second rule card.
        page.locator(".rule-card").nth(1).click()

        # The canvas must be re-populated within the interaction timeout.
        # After the click the SVG may briefly be cleared while the new rule's
        # data loads, then repopulated — wait for a non-empty state.
        page.wait_for_function(
            """() => {
                const svg = document.querySelector('#main-svg');
                return svg !== null && svg.children.length > 0;
            }""",
            timeout=_INTERACT_TIMEOUT,
        )

        after = page.evaluate(
            "() => document.querySelector('#main-svg').children.length"
        )
        assert after > 0, (
            f"Canvas did not re-render after clicking second rule card "
            f"(before={before}, after={after})"
        )

    def test_causal_view_switch_renders_causal_svg(
        self, live_server: str, page: Page
    ) -> None:
        """Switching to 'Causal Graph' view renders nodes/edges in #causal-svg."""
        self._wait_for_app_ready(page, live_server)

        # Click the 'Causal Graph' view tab.
        page.get_by_role("button", name="Causal Graph").click()

        # The causal overlay gains the 'active' class when the view is live.
        page.wait_for_selector("#causal-view.active", timeout=_INTERACT_TIMEOUT)

        # renderCausal() populates #causal-svg with D3 elements.
        page.wait_for_function(
            """() => {
                const svg = document.querySelector('#causal-svg');
                return svg !== null && svg.children.length > 0;
            }""",
            timeout=_INTERACT_TIMEOUT,
        )

        child_count = page.evaluate(
            "() => document.querySelector('#causal-svg').children.length"
        )
        assert child_count > 0, (
            f"Expected #causal-svg to contain D3-rendered elements after switching "
            f"to Causal Graph view, got {child_count} children"
        )

    def test_multiway_causal_dual_render_parity_ignores_red_membership(
        self, live_server: str, page: Page
    ) -> None:
        """Rendering rule3 twice should preserve coordinates when only red styling changes."""
        payload_url = f"{live_server}/api/rules/rule3/multiway-causal?max_steps=4"
        with urllib.request.urlopen(payload_url, timeout=10) as resp:
            payload = json.load(resp)

        red_ids = payload["default_path_event_ids"]
        assert len(red_ids) == 15

        def load_coords(view_page: Page, response_payload: dict | None = None) -> dict[str, tuple[float, float]]:
            if response_payload is not None:
                view_page.route(
                    "**/api/rules/rule3/multiway-causal**",
                    lambda route: route.fulfill(
                        status=200,
                        content_type="application/json",
                        body=json.dumps(response_payload),
                    ),
                )

            view_page.goto(live_server, wait_until="domcontentloaded")
            view_page.wait_for_selector("#loading-overlay", state="hidden", timeout=_LOAD_TIMEOUT)
            view_page.wait_for_selector(".rule-card", timeout=_RENDER_TIMEOUT)
            view_page.locator(".rule-card[data-rule-id='rule3']").click()
            view_page.get_by_role("button", name="Multiway Causal").click()
            view_page.wait_for_selector("#multiway-causal-view.active", timeout=_LOAD_TIMEOUT)
            view_page.wait_for_function(
                """() => {
                    const svg = document.querySelector('#multiway-causal-svg');
                    return svg !== null && svg.querySelectorAll('circle[data-event-id]').length > 0;
                }""",
                timeout=_LOAD_TIMEOUT,
            )

            return view_page.evaluate(
                """() => Object.fromEntries(
                    Array.from(document.querySelectorAll('#multiway-causal-svg circle[data-event-id]'))
                        .map(node => [
                            node.dataset.eventId,
                            {
                                cx: Number(node.getAttribute('cx')),
                                cy: Number(node.getAttribute('cy')),
                                red: node.dataset.red,
                            },
                        ])
                )"""
            )

        baseline_page = page
        baseline_coords = load_coords(baseline_page)
        assert len(baseline_coords) == len(payload["events"])

        stripped_page = page.context.new_page()
        try:
            stripped_payload = json.loads(json.dumps(payload))
            stripped_payload["default_path_event_ids"] = []
            stripped_coords = load_coords(stripped_page, stripped_payload)
        finally:
            stripped_page.close()

        baseline_xy = {event_id: (entry["cx"], entry["cy"]) for event_id, entry in baseline_coords.items()}
        stripped_xy = {event_id: (entry["cx"], entry["cy"]) for event_id, entry in stripped_coords.items()}
        assert baseline_xy == stripped_xy
        assert any(entry["red"] == "true" for entry in baseline_coords.values())
        assert any(entry["red"] == "false" for entry in baseline_coords.values())
