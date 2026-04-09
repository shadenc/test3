import asyncio
import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from playwright.async_api import Browser, Page, TimeoutError as PlaywrightTimeoutError

try:
    from .tadawul_debug import TadawulAccessDeniedError
except ImportError:
    from tadawul_debug import TadawulAccessDeniedError

_logger = logging.getLogger(__name__)

try:
    from .stealth_random import stealth_randint, stealth_uniform
except ImportError:
    from stealth_random import stealth_randint, stealth_uniform

try:
    from .tadawul_portal_common import (
        get_company_symbols_from_json,
        navigate_to_company_profile,
        setup_stealth_browser,
    )
except ImportError:
    from tadawul_portal_common import (
        get_company_symbols_from_json,
        navigate_to_company_profile,
        setup_stealth_browser,
    )

# Constants
PDF_DIR = Path("data/pdfs")
PDF_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_STOP_PDFS_FLAG = "data/runtime/stop_pdfs_pipeline.flag"
DEFAULT_NET_STOP_FLAG = "data/runtime/stop_net_profit.flag"
DEFAULT_PDFS_PROGRESS_FILE = "data/runtime/pdfs_progress.json"


class PdfPipelineStopRequested(Exception):
    """Raised when the user requests Stop; abort current work without treating it as a random failure."""


def _pdf_stop_flag_path() -> Path:
    return Path(os.environ.get("STOP_FLAG_FILE", DEFAULT_STOP_PDFS_FLAG))


def _net_stop_flag_path() -> Path:
    """Separate flag used by /api/net_profit/stop; unified PDF+net job must honor it too."""
    return Path(os.environ.get("NET_STOP_FLAG_FILE", DEFAULT_NET_STOP_FLAG))


def _pdf_stop_requested() -> bool:
    return _pdf_stop_flag_path().exists() or _net_stop_flag_path().exists()


# Statement type priorities (most preferred first)
STATEMENT_PRIORITIES = ["annual", "quarterly", "interim", "financial", "report"]

# Set the target year here. By default, use the current year, but you can set it manually if needed.
target_year = (
    datetime.now().year
)  # Change this to e.g. 2024 to process 2024 and 2023 Q4


async def get_all_financial_reports(  # NOSONAR
    page: Page, symbol: str, *, skip_profile_navigation: bool = False
):
    """Find all available financial report PDFs (Annual, Q1-Q4) and their years, filtered for target_year and Q4 of previous year.

    If skip_profile_navigation is True, the caller must have already opened the company profile (single shared visit with net-profit scrape).
    """
    if not skip_profile_navigation:
        ok = await navigate_to_company_profile(page, symbol)
        if not ok:
            return []
        print("On company profile page, waiting for content...")
        await page.wait_for_timeout(3000)
    else:
        print("Reusing open company profile, opening Financial Statements tab...")
        await page.wait_for_timeout(1500)
    tabs = await page.query_selector_all("li")
    try:
        target_text = "financial statements and reports"
        for tab in tabs:
            tab_text = (await tab.text_content() or "").strip().lower()
            if target_text in tab_text:
                await tab.scroll_into_view_if_needed()
                await tab.click()
                print(f"✅ Clicked tab: {tab_text}")
                break
        else:
            print("❌ 'Financial Statements and Reports' tab not found by substring.")
            return []
    except PlaywrightTimeoutError:
        print("❌ Timeout while trying to find financial tab.")
        return []
    try:
        # Wait for any table to appear first
        await page.wait_for_selector("table", timeout=10000)
        print("Table found, waiting for content to load...")

        # Wait a bit more for dynamic content
        await page.wait_for_timeout(2000)

        # Try to find the financial statements table
        table_selector = "table:has-text('Annual')"
        try:
            await page.wait_for_selector(table_selector, timeout=5000)
            print("Financial statements table loaded with 'Annual' text.")
        except PlaywrightTimeoutError:
            # If that fails, look for any table with financial data
            tables = await page.query_selector_all("table")
            print(f"Found {len(tables)} tables on the page")

            for i, table in enumerate(tables):
                table_text = await table.text_content()
                if any(
                    term in table_text.lower()
                    for term in ["annual", "quarterly", "financial", "report"]
                ):
                    print(f"Table {i} appears to contain financial data")
                    break
            else:
                print("No table with financial data found")
                return []
    except Exception as e:
        print(f"Could not find financial statements table: {e}")
        return []
    header_cells = await page.query_selector_all("table thead tr th")
    years = []
    for cell in header_cells:
        text = (await cell.text_content()).strip()
        if text.isdigit():
            years.append(int(text))
    if not years:
        print("No years found in table header.")
        return []
    rows = await page.query_selector_all("table tbody tr")
    print(f"Found {len(rows)} rows in financial statements table")

    # Debug: Print all row contents to understand the structure
    print("--- Table rows debug ---")
    for i, row in enumerate(rows):
        if _pdf_stop_requested():
            print("🛑 Stop requested during table debug scan — aborting this company.")
            raise PdfPipelineStopRequested()
        cells = await row.query_selector_all("td")
        row_text = []
        for j, cell in enumerate(cells):
            cell_text = (await cell.text_content() or "").strip()
            row_text.append(f"cell{j}: '{cell_text}'")
        print(f"Row {i}: {row_text}")
    print("--- End table debug ---")

    statement_types = ["annual", "q1", "q2", "q3", "q4"]
    found_reports = []

    for stype in statement_types:
        if _pdf_stop_requested():
            print("🛑 Stop requested while resolving report rows.")
            raise PdfPipelineStopRequested()
        row = None
        # Search through all rows for this statement type
        for r in rows:
            first_cell = await r.query_selector("td")
            if first_cell:
                cell_text = (await first_cell.text_content() or "").strip().lower()
                # More flexible matching
                if stype in cell_text or any(
                    term in cell_text for term in ["report", "statement"]
                ):
                    row = r
                    print(f"Found row for {stype}: '{cell_text}'")
                    break

        if not row:
            print(f"No '{stype}' row found in table.")
            continue

        cells = await row.query_selector_all("td")
        print(f"Row for {stype} has {len(cells)} cells")

        for i, year in enumerate(years):
            cell_index = i + 1  # offset by 1 for the label cell
            if cell_index >= len(cells):
                continue
            cell = cells[cell_index]
            pdf_link = await cell.query_selector("a[href$='.pdf']")
            if pdf_link:
                pdf_url = await pdf_link.get_attribute("href")
                if pdf_url:
                    normalized_stype = stype.lower().strip()
                    print(
                        f"🎯 Found {normalized_stype.upper()} PDF URL for {symbol} {year}: {pdf_url}"
                    )
                    found_reports.append((normalized_stype, year, pdf_url))
    # Updated filter: Q1, Q2, Q3 of current year and Annual of previous year
    filtered_reports = []
    for stype, year, pdf_url in found_reports:
        if (year == target_year and stype in ("q1", "q2", "q3")) or (
            year == target_year - 1 and stype == "annual"
        ):
            filtered_reports.append((stype, year, pdf_url))
    print(
        f"[DEBUG] Will download for {symbol}: {[f'{stype}_{year}' for stype, year, _ in filtered_reports]}"
    )
    return filtered_reports


def _net_profit_scrape_enabled_with_pdf() -> bool:
    """Scrape quarterly net profit in the same browser visit as PDFs unless SKIP_NET_PROFIT_WITH_PDF is set."""
    v = os.environ.get("SKIP_NET_PROFIT_WITH_PDF", "").strip().lower()
    return v not in ("1", "true", "yes")


def _write_pdf_bytes_sync(path: Path, pdf_content: List[int]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as f:
        f.write(bytes(pdf_content))


def _write_json_progress_sync(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)


try:
    from .scrape_quarterly_net_profit import (
        merge_quarterly_net_profit_incremental,
        navigate_to_financial_information,
        scrape_quarterly_net_profit,
    )
except ImportError:
    from scrape_quarterly_net_profit import (
        merge_quarterly_net_profit_incremental,
        navigate_to_financial_information,
        scrape_quarterly_net_profit,
    )


async def download_pdf_with_stealth(
    page: Page, pdf_url: str, symbol: str, year: int, statement_type: str
) -> bool:
    """Download PDF using the working stealth approach, generalized for statement type."""
    try:
        if _pdf_stop_requested():
            print("🛑 Stop requested. Skipping new PDF download request.")
            return False
        filename = f"{symbol}_{statement_type}_{year}.pdf"
        pdf_path = PDF_DIR / filename
        if pdf_path.exists():
            print(f"⚠️  {filename} already exists, skipping...")
            return True
        print(f"📥 Downloading {filename}...")
        if not pdf_url.startswith("http"):
            pdf_url = f"https://www.saudiexchange.sa{pdf_url}"
        response = await page.goto(pdf_url, wait_until="networkidle")
        if _pdf_stop_requested():
            print("🛑 Stop requested after navigation. Aborting download save.")
            return False
        content_type = response.headers.get("content-type", "")
        if "pdf" in content_type.lower():
            print(f"✅ Successfully accessed PDF for {symbol}")
            pdf_content = await page.evaluate("""
                async () => {
                    try {
                        const response = await fetch(window.location.href);
                        const arrayBuffer = await response.arrayBuffer();
                        return Array.from(new Uint8Array(arrayBuffer));
                    } catch (error) {
                        console.error('Error fetching PDF:', error);
                        return null;
                    }
                }
            """)
            if pdf_content:
                await asyncio.to_thread(
                    _write_pdf_bytes_sync, pdf_path, pdf_content
                )
                print(f"✅ Downloaded {filename} ({len(pdf_content)} bytes)")
                return True
            else:
                print(f"❌ Failed to get PDF content for {symbol}")
                return False
        else:
            print(
                f"❌ Did not get PDF content for {symbol} (Content-Type: {content_type})"
            )
            return False
    except Exception as e:
        print(f"❌ Download error for {symbol}: {e}")
        return False


async def process_company_with_retry(  # NOSONAR
    browser: Browser, symbol: str, max_retries: int = 3
) -> bool:
    for attempt in range(max_retries):
        page = None
        net_result: Optional[Dict[str, Any]] = None
        try:
            if _pdf_stop_requested():
                print("🛑 Stop requested. Aborting company processing.")
                return False
            page = await browser.new_page()
            await page.mouse.move(stealth_randint(100, 500), stealth_randint(100, 300))
            await asyncio.sleep(stealth_uniform(0.5, 1.5))
            ok_profile = await navigate_to_company_profile(page, symbol)
            if not ok_profile:
                await page.close()
                if _pdf_stop_requested():
                    print("🛑 Stop caused profile navigation failure — not retrying.")
                    return False
                if attempt < max_retries - 1:
                    print(
                        f"🔄 Retrying {symbol} (attempt {attempt + 2}/{max_retries})..."
                    )
                    await asyncio.sleep(stealth_uniform(2, 5))
                    continue
                return False

            reports = await get_all_financial_reports(
                page, symbol, skip_profile_navigation=True
            )
            all_success = True
            stopped_by_user = False
            for stype, year, pdf_url in reports:
                # Check stop flag before starting each report download
                if _pdf_stop_requested():
                    print(
                        "🛑 Stop requested. Halting further report downloads for this company."
                    )
                    all_success = False
                    stopped_by_user = True
                    break
                success = await download_pdf_with_stealth(
                    page, pdf_url, symbol, year, stype
                )
                if not success:
                    all_success = False

            if (
                _net_profit_scrape_enabled_with_pdf()
                and not stopped_by_user
                and not _pdf_stop_requested()
            ):
                try:
                    if await navigate_to_financial_information(page, symbol):
                        net_result = await scrape_quarterly_net_profit(page, symbol)
                        if net_result:
                            merge_quarterly_net_profit_incremental(net_result)
                    else:
                        print(
                            f"⚠️ Skipping net profit (financial info tab) for {symbol} after PDF step."
                        )
                except Exception as e:
                    print(f"⚠️ Net profit scrape after PDFs failed for {symbol}: {e}")

            await page.close()
            page = None
            if stopped_by_user:
                print("🛑 Stop acknowledged — not retrying this company.")
                return False
            if not reports:
                if net_result:
                    print(
                        f"✅ No PDF report rows for {symbol}, but net profit was updated in the same visit."
                    )
                    return True
                if _pdf_stop_requested():
                    print("🛑 Stop caused empty report list — not retrying.")
                    return False
                if attempt < max_retries - 1:
                    print(
                        f"🔄 Retrying {symbol} (attempt {attempt + 2}/{max_retries})..."
                    )
                    await asyncio.sleep(stealth_uniform(2, 5))
                    continue
                return False
            if all_success:
                return True
            elif attempt < max_retries - 1:
                print(f"🔄 Retrying {symbol} (attempt {attempt + 2}/{max_retries})...")
                await asyncio.sleep(stealth_uniform(2, 5))
        except PdfPipelineStopRequested:
            if page is not None:
                try:
                    await page.close()
                except Exception:
                    pass
            print("🛑 Stop acknowledged — not retrying this company.")
            return False
        except TadawulAccessDeniedError:
            if page is not None:
                try:
                    await page.close()
                except Exception:
                    pass
            raise
        except Exception as e:
            print(f"❌ Error processing {symbol} (attempt {attempt + 1}): {e}")
            if page is not None:
                try:
                    await page.close()
                except Exception:
                    pass
            if attempt < max_retries - 1:
                await asyncio.sleep(stealth_uniform(2, 5))
    return False


async def download_all_financial_statements() -> int:  # NOSONAR
    """Download the most recent financial statements for all companies. Returns 0 on success, 1 if Akamai/WAF blocked."""
    # Get company symbols from JSON file
    companies = get_company_symbols_from_json()
    if not companies:
        print("❌ No company symbols found. Please run the ownership scraper first.")
        return 0

    print(f"📋 Found {len(companies)} companies to process")
    if _net_profit_scrape_enabled_with_pdf():
        print(
            "📎 Unified visit: after PDFs, quarterly net profit is scraped on the same page per company "
            "(SKIP_NET_PROFIT_WITH_PDF=1 to disable)."
        )

    waf_aborted = False

    # Setup browser with stealth configuration
    playwright, browser, _ = await setup_stealth_browser()

    try:
        # progress reporting
        progress_path = Path(
            os.environ.get("PROGRESS_FILE", DEFAULT_PDFS_PROGRESS_FILE)
        )
        processed = 0
        success_count = 0
        failed_count = 0

        pdf_stop_path = Path(os.environ.get("STOP_FLAG_FILE", DEFAULT_STOP_PDFS_FLAG))
        pdf_stop_path.parent.mkdir(parents=True, exist_ok=True)

        for i, symbol in enumerate(companies, 1):
            if _pdf_stop_requested():
                print("🛑 Stop requested. Ending PDF pipeline early.")
                break
            print(f"\n{'=' * 50}")
            print(f"📊 Processing {symbol} ({i}/{len(companies)})")
            print(f"{'=' * 50}")

            try:
                success = await process_company_with_retry(browser, symbol)
            except TadawulAccessDeniedError:
                print(
                    "\n🛑 Saudi Exchange blocked access (Akamai). Stopping PDF downloader for all companies on this run."
                )
                failed_count += 1
                processed += 1
                waf_aborted = True
                break

            if success:
                success_count += 1
                print(f"✅ Successfully processed {symbol}")
            else:
                failed_count += 1
                print(f"❌ Failed to process {symbol}")
            processed += 1
            # write progress (keep "finalizing" if user hit Stop — API sets it; do not overwrite with "running")
            try:
                row_status = "finalizing" if _pdf_stop_requested() else "running"
                await asyncio.to_thread(
                    _write_json_progress_sync,
                    progress_path,
                    {
                        "status": row_status,
                        "processed": processed,
                        "success": success_count,
                        "failed": failed_count,
                        "current_symbol": symbol,
                    },
                )
            except Exception as e:
                _logger.debug("progress write skipped: %s", e, exc_info=True)

            # Add delay between companies
            if i < len(companies):
                # If stop requested, skip waiting and break immediately
                if _pdf_stop_requested():
                    print("🛑 Stop requested. Skipping wait and ending now.")
                    break
                delay = stealth_uniform(3, 7)
                print(f"⏳ Waiting {delay:.1f} seconds before next company...")
                loop = asyncio.get_running_loop()
                deadline = loop.time() + delay
                while loop.time() < deadline:
                    if _pdf_stop_requested():
                        print("🛑 Stop requested during wait between companies.")
                        break
                    await asyncio.sleep(min(0.5, deadline - loop.time()))

        # Summary
        print(f"\n{'=' * 50}")
        print("📊 DOWNLOAD SUMMARY")
        print(f"{'=' * 50}")
        print(f"✅ Successful: {success_count}")
        print(f"❌ Failed: {failed_count}")
        total = success_count + failed_count
        rate = (success_count / total * 100) if total > 0 else 0.0
        print(f"📈 Success Rate: {rate:.1f}%")
        # mark done (blocked_by_waf skips downstream extractor in API when exit code != 0)
        try:
            end_status = "blocked_by_waf" if waf_aborted else "completed"
            await asyncio.to_thread(
                _write_json_progress_sync,
                progress_path,
                {
                    "status": end_status,
                    "processed": processed,
                    "success": success_count,
                    "failed": failed_count,
                },
            )
        except Exception as e:
            _logger.debug("final progress write skipped: %s", e, exc_info=True)
    finally:
        try:
            await browser.close()
        except Exception:
            pass
        try:
            await playwright.stop()
        except Exception:
            pass

    return 1 if waf_aborted else 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(download_all_financial_statements()))

    # Comment out the test function when running all companies
    # async def test_single_company():
    #     # Test with the company we know has data
    #     symbol = "2030"
    #     print(f"🧪 Testing with {symbol} to verify new filtering...")
    #
    #     playwright, browser, context = await setup_stealth_browser()
    #     try:
    #         success = await process_company_with_retry(browser, symbol)
    #         print(f"Test result: {'✅ SUCCESS' if success else '❌ FAILED'}")
    #     finally:
    #             await browser.close()
    #             await playwright.stop()
    #
    # # Run the test
    # asyncio.run(test_single_company())
