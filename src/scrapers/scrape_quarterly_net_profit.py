#!/usr/bin/env python3
"""
Quarterly Net Profit Scraper for Saudi Exchange
Scrapes quarterly net profit data from company financial information pages
"""

import asyncio
import json
import os
import random
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

from playwright.async_api import Browser, Page

try:
    from .tadawul_debug import TadawulAccessDeniedError
except ImportError:
    from tadawul_debug import TadawulAccessDeniedError

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
OUTPUT_DIR = Path("data/results")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = OUTPUT_DIR / "quarterly_net_profit.json"

# PDF pipeline stop file: combined UI calls both /pdfs/stop and /net_profit/stop; net-only scraper must react to PDF stop too.
_PDF_STOP_FLAG_REL = "data/runtime/stop_pdfs_pipeline.flag"


def _write_net_progress_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)


def _net_scraper_stop_requested() -> bool:
    net_path = Path(
        os.environ.get("STOP_FLAG_FILE", "data/runtime/stop_net_profit.flag")
    )
    pdf_path = Path(os.environ.get("PDFS_STOP_FLAG_FILE", _PDF_STOP_FLAG_REL))
    return net_path.exists() or pdf_path.exists()


async def _interruptible_sleep_ms(
    page: Page, total_ms: int, chunk_ms: int = 400
) -> bool:
    """Sleep in short chunks so Stop is noticed. Returns False if stop requested."""
    elapsed = 0
    while elapsed < total_ms:
        if _net_scraper_stop_requested():
            return False
        step = min(chunk_ms, total_ms - elapsed)
        await page.wait_for_timeout(step)
        elapsed += step
    return True


def _row_is_net_profit_before_tax(first_cell_lower: str) -> bool:
    if "net profit (loss) before zakat and tax" in first_cell_lower:
        return True
    if "net profit" in first_cell_lower and "zakat" in first_cell_lower:
        return True
    if "صافي الربح" in first_cell_lower and (
        "زكاة" in first_cell_lower
        or "ضريبة" in first_cell_lower
        or "ضريب" in first_cell_lower
    ):
        return True
    if "صافي الربح" in first_cell_lower and "خسارة" in first_cell_lower:
        return True
    return False


async def navigate_to_financial_information(page: Page, symbol: str) -> bool:  # NOSONAR
    """Navigate to FINANCIAL INFORMATION tab and click Quarterly."""
    try:
        print(f"📊 Looking for FINANCIAL INFORMATION tab for {symbol}...")
        if not await _interruptible_sleep_ms(page, 3000):
            print("🛑 Stop requested before financial information tab.")
            return False

        # Find and click FINANCIAL INFORMATION tab
        tabs = await page.query_selector_all("li")
        financial_info_tab = None

        for tab in tabs:
            tab_text = (await tab.text_content() or "").strip()
            tab_id = await tab.get_attribute("id")

            # Look for FINANCIAL INFORMATION tab
            if "financial information" in tab_text.lower() or tab_id == "balancesheet":
                financial_info_tab = tab
                print(
                    f"✅ Found FINANCIAL INFORMATION tab: '{tab_text}' (ID: {tab_id})"
                )
                break

        if not financial_info_tab:
            print(f"❌ FINANCIAL INFORMATION tab not found for {symbol}")
            return False

        # Click FINANCIAL INFORMATION tab
        await financial_info_tab.scroll_into_view_if_needed()
        await financial_info_tab.click()
        if not await _interruptible_sleep_ms(page, 2000):
            print("🛑 Stop requested after opening financial information tab.")
            return False
        print(f"✅ Clicked FINANCIAL INFORMATION tab for {symbol}")

        # Now look for Quarterly tab - be more specific
        print(f"🔍 Looking for Quarterly tab for {symbol}...")
        quarterly_tab = None

        if not await _interruptible_sleep_ms(page, 2000):
            print("🛑 Stop requested while loading quarterly tab area.")
            return False

        # Locator API avoids occasional ElementHandle/dict issues from query_selector_all on huge DOMs
        quarterly_locator = page.locator("li, button, a, div")
        try:
            n = await quarterly_locator.count()
        except Exception:
            n = 0
        cap = min(n, 800)
        for idx in range(cap):
            if idx % 50 == 0 and _net_scraper_stop_requested():
                print("🛑 Stop requested while searching Quarterly tab.")
                return False
            try:
                el = quarterly_locator.nth(idx)
                element_text = (await el.text_content() or "").strip().lower()
                element_class = await el.get_attribute("class") or ""
                if (
                    "quarterly" in element_text
                    and "option" not in element_text
                    and "trading" not in element_text
                ):
                    quarterly_tab = el
                    print(
                        f"✅ Found Quarterly tab: '{element_text}' (class: {element_class})"
                    )
                    break
                if any(
                    term in element_text
                    for term in ["quarterly", "q1", "q2", "q3", "q4"]
                ) and not any(
                    term in element_text for term in ["option", "trading", "armo"]
                ):
                    quarterly_tab = el
                    print(
                        f"✅ Found Quarterly tab via indicators: '{element_text}' (class: {element_class})"
                    )
                    break
            except Exception:
                continue

        if not quarterly_tab:
            print(f"❌ Quarterly tab not found for {symbol}")
            print("🔍 Trying to find any financial data table...")

            # If no quarterly tab, try to find the financial table directly
            tables = await page.query_selector_all("table")
            if tables:
                print(f"📊 Found {len(tables)} tables, proceeding to scrape...")
                return True  # Continue anyway to see what we can scrape
            else:
                print("❌ No tables found either")
                return False

        await quarterly_tab.click()
        if not await _interruptible_sleep_ms(page, 2000):
            print("🛑 Stop requested after clicking Quarterly tab.")
            return False
        print(f"✅ Clicked Quarterly tab for {symbol}")

        return True

    except Exception as e:
        print(f"❌ Failed to navigate to financial information for {symbol}: {e}")
        return False


async def scrape_quarterly_net_profit(page: Page, symbol: str) -> Optional[Dict]:  # NOSONAR
    """Scrape quarterly net profit data from the financial table."""
    try:
        print(f"📈 Scraping quarterly net profit data for {symbol}...")

        # Wait for table to load
        await page.wait_for_selector("table", timeout=10000)
        await page.wait_for_timeout(2000)

        # Find all financial tables
        tables = await page.query_selector_all("table")
        statement_of_income_table = None

        print(f"🔍 Found {len(tables)} tables on the page")

        # Look specifically for the Statement of Income table
        for i, table in enumerate(tables):
            try:
                table_text = await table.text_content()
                print(f"📊 Table {i} content preview: {table_text[:200]}...")

                # Look specifically for Statement of Income with quarterly dates
                if "statement of income" in table_text.lower():
                    # Check if this table has the quarterly dates we want
                    has_quarterly_dates = any(
                        term in table_text.lower()
                        for term in [
                            "2025-06-30",
                            "2025-03-31",
                            "2024-09-30",
                            "2024-06-30",
                        ]
                    )

                    if has_quarterly_dates:
                        statement_of_income_table = table
                        print(
                            f"✅ Found Statement of Income table {i} with quarterly dates"
                        )
                        break
                    else:
                        print(
                            f"📊 Found Statement of Income table {i} but it's annual data"
                        )

            except Exception as e:
                print(f"⚠️  Error reading table {i}: {e}")
                continue

        # If we didn't find quarterly data, look for any table with the quarterly dates
        if not statement_of_income_table:
            print("🔍 Looking for any table with quarterly dates...")

            for i, table in enumerate(tables):
                try:
                    table_text = await table.text_content()

                    # Check if this table has the quarterly dates we want
                    has_quarterly_dates = any(
                        term in table_text.lower()
                        for term in [
                            "2025-06-30",
                            "2025-03-31",
                            "2024-09-30",
                            "2024-06-30",
                        ]
                    )

                    if has_quarterly_dates:
                        statement_of_income_table = table
                        print(f"✅ Found table {i} with quarterly dates")
                        break

                except Exception:
                    continue

        if not statement_of_income_table:
            print(f"❌ Statement of Income table not found for {symbol}")
            return None

        # Get table headers (quarterly dates)
        header_cells = await statement_of_income_table.query_selector_all("thead tr th")
        quarterly_dates = []

        print(f"📅 Table headers: {len(header_cells)} cells")

        for i, cell in enumerate(header_cells):
            try:
                text = (await cell.text_content() or "").strip()
                print(f"  Header {i}: '{text}'")

                # Look for quarterly dates (YYYY-MM-DD format)
                if text and len(text) == 10 and text.count("-") == 2:
                    quarterly_dates.append(text)

            except Exception as e:
                print(f"⚠️  Error reading header {i}: {e}")
                continue

        if not quarterly_dates:
            print("❌ No quarterly dates found in headers, checking table body...")

            # Try to find dates in the first row of table body
            body_rows = await statement_of_income_table.query_selector_all("tbody tr")
            if body_rows:
                first_row_cells = await body_rows[0].query_selector_all("td")
                print(f"📊 First row has {len(first_row_cells)} cells")

                for i, cell in enumerate(first_row_cells):
                    try:
                        text = (await cell.text_content() or "").strip()
                        print(f"  Cell {i}: '{text}'")

                        # Look for date format
                        if text and len(text) == 10 and text.count("-") == 2:
                            quarterly_dates.append(text)
                    except Exception as e:
                        print(f"⚠️  Error reading cell {i}: {e}")
                        continue

        if not quarterly_dates:
            print(f"❌ No quarterly dates found for {symbol}")
            return None

        print(f"📅 Found quarterly dates: {quarterly_dates}")

        # Convert dates to quarter labels
        quarters = []
        for date in quarterly_dates:
            try:
                year, month, _ = date.split("-")
                month = int(month)
                if month <= 3:
                    quarters.append(f"Q1 {year}")
                elif month <= 6:
                    quarters.append(f"Q2 {year}")
                elif month <= 9:
                    quarters.append(f"Q3 {year}")
                else:
                    quarters.append(f"Q4 {year}")
            except (ValueError, AttributeError):
                quarters.append(date)

        print(f"📅 Converted to quarters: {quarters}")

        # Find the Net Profit row in the Statement of Income table
        rows = await statement_of_income_table.query_selector_all("tbody tr")
        net_profit_row = None

        print(f"🔍 Looking through {len(rows)} rows for Net Profit...")

        for i, row in enumerate(rows):
            try:
                cells = await row.query_selector_all("td")
                if cells:
                    first_cell_text = (
                        (await cells[0].text_content() or "").strip().lower()
                    )

                    if _row_is_net_profit_before_tax(first_cell_text):
                        net_profit_row = row
                        print(f"✅ Found Net Profit row {i}: '{first_cell_text}'")
                        break

                    if i < 5:  # Show first few rows for debugging
                        print(f"  Row {i}: '{first_cell_text}'")

            except Exception as e:
                print(f"⚠️  Error reading row {i}: {e}")
                continue

        if not net_profit_row:
            print(f"❌ Net Profit row not found for {symbol}")
            return None

        # Extract net profit values
        cells = await net_profit_row.query_selector_all("td")
        net_profit_values = {}

        print(f"📊 Net Profit row has {len(cells)} cells")

        for i, quarter in enumerate(quarters):
            if i + 1 < len(cells):  # +1 because first cell is the label
                cell = cells[i + 1]
                value_text = (await cell.text_content() or "").strip()

                if value_text and value_text != "-":
                    # Clean and parse the value
                    clean_value = value_text.replace(",", "").replace(" ", "")
                    try:
                        numeric_value = float(clean_value)
                        net_profit_values[quarter] = numeric_value
                        print(f"💰 {quarter}: {numeric_value:,.0f}")
                    except ValueError:
                        print(f"⚠️  Could not parse value for {quarter}: '{value_text}'")
                        net_profit_values[quarter] = None
                else:
                    net_profit_values[quarter] = None
                    print(f"⚠️  No value for {quarter}")

        if not net_profit_values:
            print(f"❌ No net profit values extracted for {symbol}")
            return None

        # Create result structure
        result = {
            "company_symbol": symbol,
            "scraped_date": datetime.now().isoformat(),
            "quarterly_net_profit": net_profit_values,
        }

        print(f"✅ Successfully scraped quarterly net profit data for {symbol}")
        return result

    except Exception as e:
        print(f"❌ Error scraping net profit for {symbol}: {e}")
        import traceback

        traceback.print_exc()
        return None


def merge_quarterly_net_profit_incremental(
    result: Dict, output_path: Optional[Path] = None
) -> None:
    """Merge one company's scraped row into quarterly_net_profit.json (keyed by company_symbol)."""
    out = output_path or OUTPUT_FILE
    sym = str(result.get("company_symbol", "")).strip()
    if not sym:
        return
    existing_map: Dict[str, Dict] = {}
    if out.exists():
        try:
            with open(out, "r", encoding="utf-8") as f:
                existing_list = json.load(f)
            for item in existing_list:
                k = str(item.get("company_symbol", "")).strip()
                if k:
                    existing_map[k] = item
        except Exception as e:
            print(f"⚠️ Failed to read net profit file for merge: {e}")
    existing_map[sym] = result
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", encoding="utf-8") as f:
        json.dump(list(existing_map.values()), f, indent=2, ensure_ascii=False)
    print(f"💾 Net profit merge: updated {out}")


async def process_company_with_retry(  # NOSONAR
    browser: Browser, symbol: str, max_retries: int = 3
) -> Optional[Dict]:
    """Process a single company with retry logic."""
    for attempt in range(max_retries):
        if _net_scraper_stop_requested():
            return None
        page = None
        try:
            page = await browser.new_page()

            # Add random mouse movement for stealth
            await page.mouse.move(random.randint(100, 500), random.randint(100, 300))
            await asyncio.sleep(random.uniform(0.5, 1.5))

            # Navigate to company profile
            if not await navigate_to_company_profile(page, symbol):
                await page.close()
                if attempt < max_retries - 1:
                    print(
                        f"🔄 Retrying navigation for {symbol} (attempt {attempt + 2}/{max_retries})..."
                    )
                    await asyncio.sleep(random.uniform(2, 5))
                    continue
                return None

            # Navigate to financial information
            if not await navigate_to_financial_information(page, symbol):
                await page.close()
                if _net_scraper_stop_requested():
                    print("🛑 Stop requested — not retrying financial navigation.")
                    return None
                if attempt < max_retries - 1:
                    print(
                        f"🔄 Retrying financial info navigation for {symbol} (attempt {attempt + 2}/{max_retries})..."
                    )
                    await asyncio.sleep(random.uniform(2, 5))
                    continue
                return None

            # Scrape net profit data
            result = await scrape_quarterly_net_profit(page, symbol)
            await page.close()

            if result:
                return result
            elif attempt < max_retries - 1:
                print(
                    f"🔄 Retrying scraping for {symbol} (attempt {attempt + 2}/{max_retries})..."
                )
                await asyncio.sleep(random.uniform(2, 5))

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
                await asyncio.sleep(random.uniform(2, 5))

    return None


async def scrape_all_companies_net_profit() -> int:  # NOSONAR
    """Scrape quarterly net profit data for all companies. Returns 0 on success, 1 if Akamai/WAF blocked."""
    # Get company symbols
    companies = get_company_symbols_from_json()

    if not companies:
        print(
            "❌ No company symbols found. Please ensure foreign_ownership_data.json exists."
        )
        return 0

    print(f"📋 Found {len(companies)} companies to process")

    waf_aborted = False

    # Setup browser
    playwright, browser, _ = await setup_stealth_browser()

    try:
        success_count = 0
        failed_count = 0
        progress_path = Path(
            os.environ.get("PROGRESS_FILE", "data/runtime/net_profit_progress.json")
        )
        processed = 0

        stop_flag = Path(
            os.environ.get("STOP_FLAG_FILE", "data/runtime/stop_net_profit.flag")
        )
        stop_flag.parent.mkdir(parents=True, exist_ok=True)

        for i, symbol in enumerate(companies, 1):
            if _net_scraper_stop_requested():
                print("🛑 Stop requested. Ending net profit scraping early.")
                break
            print(f"\n{'=' * 60}")
            print(f"📊 Processing {symbol} ({i}/{len(companies)})")
            print(f"{'=' * 60}")

            try:
                result = await process_company_with_retry(browser, symbol)
            except TadawulAccessDeniedError:
                print(
                    "\n🛑 Saudi Exchange blocked access (Akamai). Stopping net-profit scraper for this run."
                )
                failed_count += 1
                processed += 1
                waf_aborted = True
                break

            if result:
                success_count += 1
                print(f"✅ Successfully processed {symbol}")
                try:
                    await asyncio.to_thread(
                        merge_quarterly_net_profit_incremental, result
                    )
                except Exception as e:
                    print(f"⚠️ Failed to write incremental update: {e}")
            else:
                failed_count += 1
                print(f"❌ Failed to process {symbol}")
            processed += 1
            try:
                await asyncio.to_thread(
                    _write_net_progress_json,
                    progress_path,
                    {
                        "status": "running",
                        "processed": processed,
                        "success": success_count,
                        "failed": failed_count,
                        "current_symbol": symbol,
                    },
                )
            except Exception as e:
                print(f"⚠️ progress write: {e}")

            # Optional limit for safety (also enforced by LIMIT_COMPANIES)
            try:
                limit = int(os.environ.get("LIMIT_COMPANIES", "0"))
            except Exception:
                limit = 0
            if limit and i >= limit:
                print(f"\n🛑 Stopping after {limit} companies as requested")
                break

            # Add delay between companies
            if i < len(companies) and i < 10:
                delay = random.uniform(3, 7)
                print(f"⏳ Waiting {delay:.1f} seconds before next company...")
                loop = asyncio.get_running_loop()
                deadline = loop.time() + delay
                while loop.time() < deadline:
                    if _net_scraper_stop_requested():
                        print("🛑 Stop requested during wait between companies.")
                        break
                    await asyncio.sleep(min(0.5, deadline - loop.time()))

        # Summary
        print(f"\n{'=' * 60}")
        print("📊 SCRAPING SUMMARY")
        print(f"{'=' * 60}")
        print(f"✅ Successful: {success_count}")
        print(f"❌ Failed: {failed_count}")
        print(
            f"📈 Success Rate: {(success_count / (success_count + failed_count) * 100) if (success_count + failed_count) > 0 else 0:.1f}%"
        )
        if waf_aborted:
            print(
                "⛔ Saudi Exchange blocked this run (Akamai). No updates were written to the net profit file."
            )
        elif success_count > 0:
            print(f"💾 Data saved to: {OUTPUT_FILE}")
        else:
            print(
                f"ℹ️ No new rows scraped; {OUTPUT_FILE} left unchanged unless it already existed from earlier runs."
            )
        try:
            await asyncio.to_thread(
                _write_net_progress_json,
                progress_path,
                {
                    "status": "blocked_by_waf" if waf_aborted else "completed",
                    "processed": processed,
                    "success": success_count,
                    "failed": failed_count,
                },
            )
        except Exception as e:
            print(f"⚠️ final progress write: {e}")

    finally:
        try:
            await browser.close()
        except Exception as e:
            print(f"⚠️ Browser close (may be normal after interrupt): {e}")
        try:
            await playwright.stop()
        except Exception:
            pass

    return 1 if waf_aborted else 0


if __name__ == "__main__":
    print("🚀 Starting Quarterly Net Profit Scraper...")
    print("📊 This will scrape quarterly net profit data from Saudi Exchange")
    print("⏳ Please ensure you have a stable internet connection")

    raise SystemExit(asyncio.run(scrape_all_companies_net_profit()))
