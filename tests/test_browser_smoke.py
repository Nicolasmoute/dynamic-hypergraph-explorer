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

import pytest
from playwright.sync_api import Page, expect

try:
    import pytest_playwright  # noqa: F401
except Exception:  # pragma: no cover - exercised only when plugin is absent
    @pytest.fixture
    def page():
        pytest.skip("pytest-playwright plugin is not installed")

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

    def test_multiway_causal_red_overlay_uses_unified_layout(
        self, live_server: str, page: Page
    ) -> None:
        """Rule3 red nodes render as ordinary event nodes in greedy-step layers."""
        self._wait_for_app_ready(page, live_server)

        page.locator("#card-rule3").click()
        page.get_by_role("button", name="Multiway Causal").click()
        page.wait_for_selector("#multiway-causal-view.active", timeout=_INTERACT_TIMEOUT)
        page.wait_for_function(
            """() => {
                const svg = document.querySelector('#multiway-causal-svg');
                return svg && svg.querySelectorAll('circle[data-event-id]').length > 0;
            }""",
            timeout=_LOAD_TIMEOUT,
        )

        result = page.evaluate(
            """() => {
                const nodes = [...document.querySelectorAll(
                    '#multiway-causal-svg circle[data-event-id]'
                )];
                const ids = nodes.map(n => n.getAttribute('data-event-id'));
                const red = nodes.filter(n => n.getAttribute('data-red') === 'true');
                const redIds = red.map(n => n.getAttribute('data-event-id'));
                const redY = red.map(n => Number(n.getAttribute('cy')));
                const yCounts = {};
                for (const y of redY) {
                    const key = y.toFixed(3);
                    yCounts[key] = (yCounts[key] || 0) + 1;
                }
                return {
                    nodeCount: nodes.length,
                    uniqueNodeCount: new Set(ids).size,
                    redCount: red.length,
                    uniqueRedCount: new Set(redIds).size,
                    yMultiplicity: Object.values(yCounts).sort((a, b) => a - b),
                };
            }"""
        )

        assert result["nodeCount"] == result["uniqueNodeCount"]
        assert result["redCount"] == 15
        assert result["uniqueRedCount"] == 15
        assert result["yMultiplicity"] == [1, 2, 4, 8]
