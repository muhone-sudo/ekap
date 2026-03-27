"""Playwright-stealth based EKAP scraper."""
from __future__ import annotations

import asyncio
import json
import random
import shutil
import tempfile
from pathlib import Path
from typing import Optional

from playwright.async_api import (
    Browser,
    BrowserContext,
    Page,
    Playwright,
    async_playwright,
)
from playwright_stealth import stealth_async

from shared.logger import get_logger
from shared.schemas import ScraperConfig

from .circuit_breaker import CircuitBreaker, ScraperBlockedException

log = get_logger(__name__)

_SELECTORS_PATH = Path(__file__).parent.parent / "selectors.json"


def _load_selectors() -> dict[str, str]:
    with open(_SELECTORS_PATH) as fh:
        return json.load(fh)


async def _jitter(config: ScraperConfig) -> None:
    delay = random.uniform(config.jitter_min, config.jitter_max)
    log.debug("jitter_sleep", seconds=round(delay, 2))
    await asyncio.sleep(delay)


class EKAPScraper:
    """Stealth Playwright scraper for EKAPv2."""

    def __init__(self, config: ScraperConfig) -> None:
        self.config = config
        self.circuit_breaker = CircuitBreaker(threshold=config.block_threshold)
        self._selectors = _load_selectors()

    async def scrape_tender(
        self,
        tender_id: str,
        output_dir: Optional[str] = None,
    ) -> str:
        """Download the tender ZIP for *tender_id* and return the local file path.

        Args:
            tender_id: EKAP ihale numarası (e.g. "2024/123456").
            output_dir: Directory to save the downloaded file.
                        Defaults to a temporary directory under ``outputs/``.

        Returns:
            Absolute path of the downloaded ZIP file.
        """

        async def _do_scrape() -> str:
            return await self._scrape(tender_id, output_dir)

        return await self.circuit_breaker.call(_do_scrape())

    async def _scrape(self, tender_id: str, output_dir: Optional[str]) -> str:
        dest_dir = Path(output_dir) if output_dir else Path("outputs") / tender_id
        dest_dir.mkdir(parents=True, exist_ok=True)

        async with async_playwright() as pw:
            browser, context = await self._launch(pw)
            try:
                page = await context.new_page()
                await stealth_async(page)

                await self._navigate_to_search(page)
                await _jitter(self.config)

                await self._enter_tender_id(page, tender_id)
                await _jitter(self.config)

                zip_path = await self._download_zip(page, dest_dir)
                log.info("scrape_complete", tender_id=tender_id, zip_path=zip_path)
                return zip_path
            finally:
                await context.close()
                await browser.close()

    async def _launch(self, pw: Playwright) -> tuple[Browser, BrowserContext]:
        browser = await pw.chromium.launch(headless=self.config.headless)
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 900},
            locale="tr-TR",
        )
        return browser, context

    async def _navigate_to_search(self, page: Page) -> None:
        url = self.config.ekap_base_url
        log.info("navigate", url=url)
        response = await page.goto(url, wait_until="networkidle")
        if response and response.status == 403:
            log.error("blocked_403", url=url)
            raise ScraperBlockedException(f"403 received at {url}")

    async def _enter_tender_id(self, page: Page, tender_id: str) -> None:
        search_input_sel = self._selectors["tender_search_input"]
        search_btn_sel = self._selectors["search_button"]

        log.debug("filling_search_input", tender_id=tender_id)
        await page.fill(search_input_sel, tender_id)
        await _jitter(self.config)
        await page.click(search_btn_sel)
        await page.wait_for_load_state("networkidle")

        # Check for a 403 / bot block page after search
        if page.url and "403" in page.url:
            raise ScraperBlockedException(f"Redirected to 403 page after search: {page.url}")

    async def _download_zip(self, page: Page, dest_dir: Path) -> str:
        download_btn_sel = self._selectors["download_zip_button"]
        log.debug("waiting_for_download_button")
        await page.wait_for_selector(download_btn_sel, timeout=30_000)

        async with page.expect_download() as download_info:
            await page.click(download_btn_sel)

        download = await download_info.value
        suggested = download.suggested_filename or "tender.zip"
        dest = dest_dir / suggested
        await download.save_as(str(dest))
        log.info("zip_downloaded", path=str(dest), size=dest.stat().st_size)
        return str(dest)
