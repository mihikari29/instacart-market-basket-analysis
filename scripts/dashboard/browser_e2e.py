#!/usr/bin/env python3
"""Run real Chromium browser checks against the loopback-bound dashboard."""

from __future__ import annotations

from datetime import datetime, timezone
from importlib.metadata import version as package_version
import json
import os
import re
import sys
from typing import Any
from urllib.parse import urlencode, urlsplit
from urllib.request import urlopen

from playwright.sync_api import Page, expect, sync_playwright


UI_URL = os.environ.get("MODULE4D_BROWSER_UI_URL", "http://127.0.0.1:18501")
API_URL = os.environ.get("MODULE4D_BROWSER_API_URL", "http://127.0.0.1:18000")
NAMESPACE = os.environ.get("MODULE4D_DEMO_TREND_NAMESPACE", "module4d-demo")
WARM_USER_ID = 601
FALLBACK_USER_ID = 602
UNKNOWN_USER_ID = 9999


def _json_get(url: str) -> dict[str, Any]:
    with urlopen(url, timeout=10) as response:
        if response.status != 200:
            raise RuntimeError(f"API returned HTTP {response.status} for browser oracle request")
        return json.loads(response.read())


def _all_text(page: Page) -> str:
    values = [page.locator("body").inner_text(timeout=5_000)]
    for frame in page.frames:
        if frame == page.main_frame:
            continue
        try:
            values.append(frame.locator("body").inner_text(timeout=1_000))
        except Exception:
            continue
    return "\n".join(values)


def _collect_grid_text(page: Page) -> str:
    """Scroll virtualized Streamlit data grids into view and read their hidden accessible cells."""
    main = page.locator('[data-testid="stMain"]')
    main.wait_for(timeout=5_000)
    dimensions = main.evaluate("element => [element.scrollHeight, element.clientHeight]")
    height, viewport = dimensions
    cells = page.locator('[data-testid^="glide-cell-"]')
    main.evaluate("(element, position) => { element.scrollTop = position; }", max(height - viewport, 0))
    cells.first.wait_for(state="attached", timeout=10_000)
    collected: list[str] = []
    positions = list(range(0, max(height - viewport + 1, 1), max(viewport - 100, 400)))
    positions.append(max(height - viewport, 0))
    for position in sorted(set(positions)):
        main.evaluate("(element, position) => { element.scrollTop = position; }", position)
        page.wait_for_timeout(350)
        if cells.count():
            collected.extend(value for value in cells.all_text_contents() if value)
    main.evaluate("element => { element.scrollTop = 0; }")
    cells = page.locator('[data-testid^="glide-cell-"]')
    if cells.count():
        collected.extend(value for value in cells.all_text_contents() if value)
    grids = page.locator('[data-testid="stDataFrame"]')
    collected.extend(value for value in grids.all_text_contents() if value)
    return "\n".join(collected)


def _navigate(page: Page, name: str, title: str) -> None:
    page.locator('[data-testid="stSidebar"]').get_by_text(name, exact=True).click()
    expect(page.get_by_role("heading", name=title, exact=True)).to_be_visible(timeout=15_000)


def _select_option(page: Page, label: str, option: str) -> None:
    """Choose a Streamlit custom selectbox option through its accessible combobox."""
    selector = page.get_by_label(label, exact=True)
    selector.click()
    choice = page.get_by_role("option", name=option, exact=True)
    expect(choice).to_be_visible(timeout=5_000)
    choice.click()
    expect(selector).to_have_value(option, timeout=5_000)


def _wait_for_evaluation_evidence(page: Page) -> None:
    """Wait for the later C8B section before taking the evaluation text snapshot."""
    heading = "Recorded Spark batch durations (milliseconds)"
    try:
        expect(page.get_by_role("heading", name=heading, exact=True)).to_be_visible(timeout=20_000)
    except AssertionError as exc:
        url = urlsplit(page.url)
        headings = page.locator("h1:visible, h2:visible, h3:visible").all_text_contents()
        visible_headings = [value[:120] for value in headings[:20]]
        raise RuntimeError(
            f"evaluation page did not render required evidence heading {heading!r}; "
            f"url_origin={url.scheme}://{url.hostname}:{url.port}, "
            f"visible_headings={visible_headings}, "
            f"charts={page.locator('[data-testid=stVegaLiteChart]').count()}, "
            f"exceptions={page.locator('[data-testid=stException]').count()}"
        ) from exc


def _wait_for_chart_minimum(page: Page, minimum: int, section: str, expected: str) -> int:
    """Wait for the last required chart before applying the unchanged count gate."""
    charts = page.locator('[data-testid="stVegaLiteChart"]')
    try:
        expect(charts.nth(minimum - 1)).to_be_visible(timeout=20_000)
    except Exception as exc:
        visible_headings = page.locator("h1, h2, h3").all_text_contents()
        raise RuntimeError(
            f"{section} did not render its required charts; minimum={minimum}, "
            f"observed={charts.count()}, expected={expected}, "
            f"visible_headings={visible_headings}"
        ) from exc
    chart_count = charts.count()
    if chart_count < minimum:
        visible_headings = page.locator("h1, h2, h3").all_text_contents()
        raise RuntimeError(
            f"{section} rendered only {chart_count} charts; expected at least {minimum}: {expected}; "
            f"visible_headings={visible_headings}"
        )
    return chart_count


def run() -> dict[str, Any]:
    if not UI_URL.startswith("http://127.0.0.1:") or not API_URL.startswith("http://127.0.0.1:"):
        raise RuntimeError("browser E2E requires API and UI URLs bound to host loopback")
    if not NAMESPACE or NAMESPACE != NAMESPACE.strip():
        raise RuntimeError("browser test namespace must be explicit and stable")

    health = _json_get(f"{API_URL.rstrip('/')}/health")
    assert health == {"status": "ok"}
    now = _json_get(f"{API_URL.rstrip('/')}/api/v1/evidence")
    assert now["items"]
    trends = _json_get(
        f"{API_URL.rstrip('/')}/api/v1/trends?"
        + urlencode(
            {
                "source_namespace": NAMESPACE,
                "as_of_event_time": datetime.now(timezone.utc)
                .isoformat(timespec="milliseconds")
                .replace("+00:00", "Z"),
            }
        )
    )
    recommendation = _json_get(f"{API_URL.rstrip('/')}/api/v1/recommendations/{WARM_USER_ID}")
    fallback = _json_get(f"{API_URL.rstrip('/')}/api/v1/recommendations/{FALLBACK_USER_ID}")
    orders = _json_get(f"{API_URL.rstrip('/')}/api/v1/analytics/orders-distribution")
    if not trends["available"] or not recommendation["recommendations"] or not fallback["recommendations"]:
        raise RuntimeError("browser fixture oracle did not return the expected source-backed values")

    page_errors: list[str] = []
    chromium_version = ""
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True, args=["--no-sandbox"])
        chromium_version = browser.version
        page = browser.new_page(viewport={"width": 1440, "height": 1000})
        page.on("pageerror", lambda error: page_errors.append(str(error)))
        page.goto(UI_URL, wait_until="domcontentloaded", timeout=30_000)
        expect(page.get_by_role("heading", name="Overview", exact=True)).to_be_visible(timeout=30_000)
        expect(page.get_by_text("API connectivity", exact=True)).to_be_visible()
        expect(page.get_by_text("Historical evidence registry", exact=True)).to_be_visible()
        expect(page.get_by_text("Archived reports are historical evidence", exact=False)).to_be_visible(timeout=15_000)
        overview_text = _all_text(page)
        if "not live Kafka ingestion" not in overview_text:
            raise RuntimeError(
                "Overview did not distinguish archived evidence from live Kafka ingestion; "
                f"rendered text: {overview_text[:1_500]}"
            )
        overview_grid = _collect_grid_text(page)
        if "Available" not in overview_text or "Module 4C C8B bounded streaming demonstration receipt" not in overview_grid:
            raise RuntimeError("Overview did not render available API and evidence source summaries")

        _navigate(page, "Realtime & Batch Analytics", "Realtime & Batch Analytics")
        page.get_by_role("button", name="Load / refresh analytics", exact=True).click()
        expect(page.get_by_role("heading", name="Finalized trend ranking", exact=True)).to_be_visible()
        expect(page.get_by_role("heading", name="Batch products", exact=True)).to_be_visible()
        expect(page.get_by_text("Source: batch_product_metrics", exact=False)).to_be_visible(timeout=15_000)
        analytics_text = _all_text(page)
        analytics_grid = _collect_grid_text(page)
        analytics_visible = analytics_text + "\n" + analytics_grid
        if NAMESPACE not in analytics_text or "synthetic-demonstration-window" not in analytics_text:
            raise RuntimeError("analytics page did not show its selected namespace and source scope")
        selected_ids = [str(row["product_id"]) for row in trends["items"]]
        if any(product_id not in analytics_visible for product_id in selected_ids):
            raise RuntimeError("analytics page omitted product IDs returned by the selected trend API response")
        batch = _json_get(f"{API_URL.rstrip('/')}/api/v1/batch/products?order=purchase_count&limit=20")
        if batch["source"]["scope"] != "prior-only item purchases":
            raise RuntimeError("browser fixture did not preserve the prior-only purchase definition")
        if any(str(row["product_id"]) not in analytics_visible for row in batch["items"]):
            raise RuntimeError("analytics page omitted fixture batch products")
        if orders["scope"] != "all orders" or "all-order" not in analytics_visible.lower() and "all orders" not in analytics_visible.lower():
            raise RuntimeError("analytics page did not separate archived all-order distributions from prior-only metrics")
        _wait_for_chart_minimum(
            page, 4, "analytics page", "trend, batch, and two order charts"
        )

        _navigate(page, "Hybrid Recommendations", "ALS and Hybrid Recommendations")
        user_input = page.get_by_label("Positive integer user ID", exact=True)
        user_input.fill(str(WARM_USER_ID))
        page.get_by_role("button", name="Load / refresh ALS and Hybrid", exact=True).click()
        expect(page.get_by_text(recommendation["serving_version"], exact=False).first).to_be_visible(timeout=15_000)
        warm_text = _all_text(page)
        warm_grid = _collect_grid_text(page)
        warm_visible = warm_text + "\n" + warm_grid
        for value in (
            "hybrid",
            "Source run: module4d-demo-source-001",
            "Model version: module4d-demo-als-v1",
            "Graph version: module4d-demo-graph-v1",
            "ALS rank",
            "ALS score",
            "Trend score",
            "Final rank",
            "Final score",
        ):
            if value.lower() not in warm_visible.lower():
                raise RuntimeError(f"Hybrid page did not visibly render expected source-backed field {value!r}")
        for row in recommendation["recommendations"][:3]:
            if str(row["product_id"]) not in warm_visible:
                raise RuntimeError("Hybrid page did not render one of the top ranked persisted products")
        first_row = recommendation["recommendations"][0]
        if str(first_row["final_score"]) not in warm_visible and f"{first_row['final_score']:.5f}" not in warm_visible:
            raise RuntimeError("Hybrid grid did not render the source-derived persisted final score")

        user_input.fill(str(FALLBACK_USER_ID))
        page.get_by_role("button", name="Load / refresh ALS and Hybrid", exact=True).click()
        expect(page.get_by_text("als_fallback_trend_unavailable", exact=False)).to_be_visible(timeout=15_000)
        if fallback["mode"] != "als_fallback_trend_unavailable":
            raise RuntimeError("fallback browser source and API response differ")

        user_input.fill(str(UNKNOWN_USER_ID))
        page.get_by_role("button", name="Load / refresh ALS and Hybrid", exact=True).click()
        expect(page.get_by_text("recommendation_snapshot_not_found", exact=False)).to_be_visible(timeout=15_000)
        user_input.fill(str(WARM_USER_ID))
        page.get_by_role("button", name="Load / refresh ALS and Hybrid", exact=True).click()
        expect(page.get_by_text(recommendation["serving_version"], exact=False).first).to_be_visible(timeout=15_000)

        _navigate(page, "Evaluation & Performance", "Evaluation & Performance")
        expect(page.get_by_text("0.121472", exact=False)).to_be_visible(timeout=20_000)
        expect(page.get_by_text("Structured shuffle metrics unavailable", exact=False)).to_be_visible(timeout=20_000)
        expect(page.get_by_text("Finalized windows", exact=True)).to_be_visible(timeout=20_000)
        expect(page.get_by_text("Planned events", exact=True)).to_be_visible(timeout=20_000)
        _wait_for_evaluation_evidence(page)
        evaluation_text = _all_text(page)
        if "0.121472" not in evaluation_text or "0.319232" not in evaluation_text:
            raise RuntimeError("evaluation page omitted accepted ALS and personal-frequency Recall@10 values")
        if "shuffle metrics unavailable" not in evaluation_text.lower():
            raise RuntimeError("evaluation page omitted the explicit unavailable shuffle state")
        for required in ("C8A", "C8B", "Finalized windows", "Planned events", "Archived source"):
            if required.lower() not in evaluation_text.lower():
                raise RuntimeError(f"evaluation page omitted archived evidence detail {required!r}")
        for required in (
            "Spark batch performance",
            "Join strategy median times",
            "Partition scans",
            "Cache timing",
            "Lambda 0.5 first-publication timing",
            "Recommendation accuracy was not measured in C8A.",
            "Recorded Spark batch durations (milliseconds)",
        ):
            if required.lower() not in evaluation_text.lower():
                raise RuntimeError(f"evaluation page omitted performance evidence section {required!r}")

        evaluation_grid = _collect_grid_text(page)
        for evidence_id in (
            "module2-full-hardening-benchmarks",
            "module4c-c8a-responsiveness",
            "module4c-c8b-streaming-demo",
        ):
            if evidence_id not in evaluation_grid:
                raise RuntimeError(f"evidence registry did not render {evidence_id!r}")

        metric_selector = page.get_by_label("Metric", exact=True)
        method_selector = page.get_by_label("Recorded method", exact=True)
        cohort_selector = page.get_by_label("Primary evaluation cohort", exact=True)
        k_selector = page.get_by_label("K", exact=True)
        for label, selector in (
            ("Metric", metric_selector),
            ("Recorded method", method_selector),
            ("Primary evaluation cohort", cohort_selector),
            ("K", k_selector),
        ):
            expect(selector).to_be_visible(timeout=10_000)

        _select_option(page, "Primary evaluation cohort", "Warm users and all truth items")
        _select_option(page, "Metric", "NDCG@K")
        _select_option(page, "Recorded method", "Global popularity")
        _select_option(page, "K", "5")
        expect(page.get_by_role("heading", name="NDCG@K at K=5", exact=True)).to_be_visible(timeout=15_000)
        expect(page.get_by_text("Global popularity · NDCG@K@5", exact=True)).to_be_visible(timeout=15_000)

        _select_option(page, "Primary evaluation cohort", "All official-train users")
        _select_option(page, "Metric", "Recall@K")
        _select_option(page, "Recorded method", "ALS")
        _select_option(page, "K", "10")
        expect(page.get_by_role("heading", name="Recall@K at K=10", exact=True)).to_be_visible(timeout=15_000)
        evaluation_chart_count = _wait_for_chart_minimum(
            page, 5, "evaluation page", "ALS, three Spark, and C8B charts"
        )
        page.get_by_role("button", name="Refresh evaluation and evidence", exact=True).click()
        expect(page.get_by_role("heading", name="Evaluation & Performance", exact=True)).to_be_visible()
        rendered = _all_text(page)
        if re.search(r"(?i)(mongodb(?:\+srv)?://|password|passwd|secret|token)", rendered):
            raise RuntimeError("browser-rendered content included a credential-like value")
        if re.search(r"(?i)(traceback \(most recent call last\)|script execution error|unhandled exception)", rendered):
            raise RuntimeError("Streamlit displayed a Python exception in the browser")
        if page_errors:
            raise RuntimeError(f"browser observed JavaScript exceptions: {page_errors}")
        browser.close()

    return {
        "browser": "Chromium via pinned Playwright Python 1.63.0 container",
        "playwright_python_version": package_version("playwright"),
        "chromium_version": chromium_version,
        "url": UI_URL,
        "host_loopback_access": True,
        "pages_verified": ["Overview", "Realtime & Batch Analytics", "Hybrid Recommendations", "Evaluation & Performance"],
        "trend_products_visible": [row["product_id"] for row in trends["items"]],
        "batch_products_visible": [row["product_id"] for row in batch["items"]],
        "warm_serving_version": recommendation["serving_version"],
        "warm_top_products_visible": [row["product_id"] for row in recommendation["recommendations"][:3]],
        "fallback_mode": fallback["mode"],
        "all_order_total": orders["total_orders"],
        "evaluation_charts_visible": evaluation_chart_count,
        "evaluation_controls_verified": ["Primary evaluation cohort", "K", "Metric", "Recorded method"],
        "performance_evidence_sections_visible": [
            "Spark batch performance",
            "Join strategy median times",
            "Partition scans",
            "Cache timing",
            "Lambda 0.5 first-publication timing",
            "Recorded Spark batch durations",
        ],
        "refresh_verified": True,
        "unknown_user_error_visible_and_recovered": True,
        "javascript_exceptions": page_errors,
    }


if __name__ == "__main__":
    try:
        result = run()
    except Exception as exc:
        print(f"Module 4D browser E2E failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        raise SystemExit(1)
    print(json.dumps(result, sort_keys=True, indent=2))
