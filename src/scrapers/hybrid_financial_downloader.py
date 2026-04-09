import asyncio
import os
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional
import random

from playwright.async_api import (
    async_playwright,
    Browser,
    Page,
    TimeoutError as PlaywrightTimeoutError,
)

try:
    from .tadawul_debug import (
        ACCESS_DENIED_HELP,
        TadawulAccessDeniedError,
        dump_tadawul_navigation_debug,
        is_tadawul_access_denied_page,
    )
except ImportError:
    from tadawul_debug import (
        ACCESS_DENIED_HELP,
        TadawulAccessDeniedError,
        dump_tadawul_navigation_debug,
        is_tadawul_access_denied_page,
    )


def _playwright_headless() -> bool:
    """Default headless=True; set PLAYWRIGHT_HEADLESS=0 to show the browser for debugging."""
    v = os.environ.get("PLAYWRIGHT_HEADLESS", "1").strip().lower()
    return v not in ("0", "false", "no", "off")


# Constants
BASE_URL = "https://www.saudiexchange.sa/wps/portal/saudiexchange/companies/company-profile-main/"
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


def get_company_symbols_from_json():
    """Get company symbols from the existing JSON file."""
    try:
        json_path = Path("frontend/public/foreign_ownership_data.json")
        if not json_path.exists():
            print(f"❌ JSON file not found: {json_path}")
            return []

        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        symbols = [item["symbol"] for item in data if item.get("symbol")]
        # Optional limit for testing
        try:
            limit = int(os.environ.get("LIMIT_COMPANIES", "0"))
            if limit > 0:
                symbols = symbols[:limit]
        except Exception:
            pass
        print(f"📋 Found {len(symbols)} company symbols from JSON file")
        return symbols

    except Exception as e:
        print(f"❌ Error reading JSON file: {e}")
        return []


async def setup_stealth_browser():
    """Setup Playwright browser with stealth configuration from download_pdf_playwright.py."""
    playwright = await async_playwright().start()

    launch_kw: dict = {
        "headless": _playwright_headless(),
        "args": [
            "--no-sandbox",
            "--disable-blink-features=AutomationControlled",
            "--disable-dev-shm-usage",
            "--disable-extensions",
            "--no-first-run",
            "--disable-default-apps",
            "--disable-popup-blocking",
            "--disable-translate",
            "--disable-background-timer-throttling",
            "--disable-backgrounding-occluded-windows",
            "--disable-renderer-backgrounding",
            "--disable-features=TranslateUI",
            "--disable-ipc-flooding-protection",
            "--disable-web-security",
            "--disable-features=VizDisplayCompositor",
        ],
    }
    _ch = os.environ.get("PLAYWRIGHT_CHANNEL", "").strip()
    if _ch:
        launch_kw["channel"] = _ch

    browser = await playwright.chromium.launch(**launch_kw)

    context = await browser.new_context(
        viewport={"width": 1920, "height": 1080},
        user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        accept_downloads=True,
        locale="en-US",
        timezone_id="America/New_York",
        permissions=["geolocation"],
        extra_http_headers={
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Cache-Control": "max-age=0",
        },
    )

    # Add stealth scripts from download_pdf_playwright.py
    await context.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', {
            get: () => undefined,
        });
        
        Object.defineProperty(navigator, 'plugins', {
            get: () => [1, 2, 3, 4, 5],
        });
        
        Object.defineProperty(navigator, 'languages', {
            get: () => ['en-US', 'en'],
        });
        
        window.chrome = {
            runtime: {},
        };
        
        Object.defineProperty(navigator, 'permissions', {
            get: () => ({
                query: () => Promise.resolve({ state: 'granted' }),
            }),
        });
    """)

    return playwright, browser, context


def _tadawul_search_input_timeout_ms() -> int:
    try:
        return int(os.environ.get("TADAWUL_SEARCH_INPUT_TIMEOUT_MS", "45000"))
    except ValueError:
        return 45000


def _tadawul_post_load_ms() -> int:
    try:
        return int(os.environ.get("TADAWUL_POST_LOAD_MS", "3500"))
    except ValueError:
        return 3500


# Portal often injects the search box after the first paint; keep selectors loose.
_SEARCH_INPUT_LOCATOR_CSS = (
    "#query-input, input#query-input, input[name='query'], input[name='Query'], "
    "input[type='search'], input.srchInput, input[placeholder*='Search' i], "
    "input[aria-label*='Search' i], input[title*='Search' i]"
)


async def _dismiss_common_overlays(page: Page) -> None:
    """Close cookie / consent dialogs that block the portal."""
    candidates = [
        'button:has-text("Accept")',
        'button:has-text("Accept All")',
        'button:has-text("Agree")',
        'button:has-text("OK")',
        '[aria-label="Close"]',
        "button.cookie-accept",
        ".cookie-accept",
        "#onetrust-accept-btn-handler",
    ]
    for sel in candidates:
        try:
            loc = page.locator(sel).first
            if await loc.count() > 0:
                await loc.click(timeout=2500)
                await page.wait_for_timeout(400)
        except Exception:
            continue


async def _click_tadawul_search_submit(page: Page) -> None:
    clicked = await page.evaluate(
        """() => {
      const blue = document.querySelector('div.srchBlueBtn');
      if (blue && blue.offsetParent !== null) { blue.click(); return true; }
      const sel = 'button[type="submit"], input[type="submit"], [class*="srchBlue"], [class*="search"][class*="Btn"]';
      for (const el of document.querySelectorAll(sel)) {
        if (el && el.offsetParent !== null) { el.click(); return true; }
      }
      return false;
    }"""
    )
    if not clicked:
        await page.keyboard.press("Enter")


_JS_PICK_BEST_SEARCH_INPUT = """() => {
      const vis = (el) => {
        if (!el || el.disabled) return false;
        const r = el.getBoundingClientRect();
        if (r.width < 2 || r.height < 2) return false;
        const st = window.getComputedStyle(el);
        return st.visibility !== 'hidden' && st.display !== 'none' && Number(st.opacity) > 0;
      };
      const inputs = [...document.querySelectorAll('input')].filter(vis);
      let best = null;
      let bestScore = -1;
      for (const el of inputs) {
        const t = (el.type || 'text').toLowerCase();
        if (t === 'hidden' || t === 'submit' || t === 'button' || t === 'checkbox' || t === 'radio') continue;
        const id = (el.id || '').toLowerCase();
        const nm = (el.name || '').toLowerCase();
        const ph = (el.placeholder || '').toLowerCase();
        const ac = (el.autocomplete || '').toLowerCase();
        let s = 0;
        if (id.includes('query')) s += 20;
        if (nm.includes('query')) s += 15;
        if (ph.includes('search') || ph.includes('symbol') || ph.includes('company')) s += 12;
        if (t === 'search') s += 8;
        if (t === 'text') s += 3;
        if (ac.includes('off')) s += 1;
        if (s > bestScore) {
          bestScore = s;
          best = el;
        }
      }
      if (!best && inputs.length) {
        best = inputs[0];
      }
      if (!best) return null;
      const esc = (typeof CSS !== 'undefined' && CSS.escape) ? CSS.escape : (s) => s.replace(/[^a-zA-Z0-9_-]/g, '\\\\$&');
      if (best.id) return { kind: 'css', value: '#' + esc(best.id) };
      if (best.name) return { kind: 'name', value: best.name };
      return null;
    }"""


async def _locator_from_js_best_input_any_frame(page: Page):
    """Pick search input via DOM scoring in main frame or any iframe."""
    for fr in page.frames:
        try:
            sel = await fr.evaluate(_JS_PICK_BEST_SEARCH_INPUT)
            if not sel:
                continue
            if sel.get("kind") == "css":
                loc = fr.locator(sel["value"]).first
            elif sel.get("kind") == "name":
                loc = fr.locator(f"input[name={json.dumps(sel['value'])}]").first
            else:
                continue
            await loc.wait_for(state="visible", timeout=8000)
            return loc
        except Exception:
            continue
    return None


async def _wait_for_tadawul_search_input(page: Page):
    """Return a Locator for the visible search field (main frame or iframe)."""
    timeout = _tadawul_search_input_timeout_ms()
    main = page.locator(_SEARCH_INPUT_LOCATOR_CSS).first
    try:
        await main.wait_for(state="visible", timeout=min(20000, timeout))
        return main
    except PlaywrightTimeoutError:
        pass
    for frame in page.frames:
        if frame == page.main_frame:
            continue
        loc = frame.locator(_SEARCH_INPUT_LOCATOR_CSS).first
        try:
            await loc.wait_for(state="visible", timeout=12000)
            return loc
        except PlaywrightTimeoutError:
            continue
    js_loc = await _locator_from_js_best_input_any_frame(page)
    if js_loc is not None:
        return js_loc
    try:
        await main.wait_for(state="visible", timeout=timeout)
        return main
    except PlaywrightTimeoutError:
        await dump_tadawul_navigation_debug(page, "search_input_timeout")
        raise


# Deep link observed on Saudi Exchange (may avoid /hidden/search/ WAF rules on some networks).
_COMPANY_PROFILE_DIRECT_TMPL = (
    "https://www.saudiexchange.sa/wps/portal/saudiexchange/hidden/company-profile-main/"
    "!ut/p/z1/04_Sj9CPykssy0xPLMnMz0vMAfIjo8ziTR3NDIw8LAz83d2MXA0C3SydAl1c3Q0NvE30I4EKzBEKDMKcTQzMDPxN3H19LAzdTU31w8syU8v1wwkpK8hOMgUA-oskdg!!/"
    "?companySymbol={symbol}"
)


async def _looks_like_company_profile(page: Page) -> bool:
    if await is_tadawul_access_denied_page(page):
        return False
    try:
        html = (await page.content()).lower()
    except Exception:
        return False
    if len(html) < 800:
        return False
    markers = (
        "financial statement",
        "financial information",
        "company-profile",
        "saudiexchange",
        "tadawul",
    )
    if any(m in html for m in markers):
        return True
    tabs = await page.query_selector_all("li")
    for tab in tabs[:150]:
        t = (await tab.text_content() or "").lower()
        if "financial" in t:
            return True
    return False


async def navigate_to_company_profile(page: Page, symbol: str) -> bool:
    """Open company profile: try direct ?companySymbol= link, then homepage warm-up + legacy search page."""
    search_url = "https://www.saudiexchange.sa/wps/portal/saudiexchange/hidden/search/!ut/p/z0/04_Sj9CPykssy0xPLMnMz0vMAfIjo8ziTR3NDIw8LAz8DTxCnA3MDILdzUJDLAyNHI30C7IdFQEEx_vC/"
    direct_candidates = [
        _COMPANY_PROFILE_DIRECT_TMPL.format(symbol=symbol),
        f"{BASE_URL.rstrip('/')}/?companySymbol={symbol}",
    ]
    access_help_shown = False

    def _warn_access_denied() -> None:
        nonlocal access_help_shown
        if not access_help_shown:
            print(f"❌ {ACCESS_DENIED_HELP}")
            access_help_shown = True

    try:
        for durl in direct_candidates:
            try:
                await page.goto(durl, wait_until="domcontentloaded", timeout=90000)
                await page.wait_for_timeout(2500)
                if await is_tadawul_access_denied_page(page):
                    _warn_access_denied()
                    await dump_tadawul_navigation_debug(
                        page, f"access_denied_direct_{symbol}"
                    )
                    continue
                if await _looks_like_company_profile(page):
                    print(f"✅ Loaded company profile directly for {symbol}")
                    return True
            except Exception as e:
                print(f"⚠️ Direct profile URL failed ({durl[:60]}…): {e}")

        try:
            await page.goto(
                "https://www.saudiexchange.sa/",
                wait_until="domcontentloaded",
                timeout=60000,
            )
            await page.wait_for_timeout(2000)
            await _dismiss_common_overlays(page)
            if await is_tadawul_access_denied_page(page):
                _warn_access_denied()
                await dump_tadawul_navigation_debug(
                    page, f"access_denied_home_{symbol}"
                )
                raise TadawulAccessDeniedError()
        except TadawulAccessDeniedError:
            raise
        except Exception:
            pass

        await page.goto(search_url, wait_until="domcontentloaded", timeout=90000)
        await page.wait_for_timeout(_tadawul_post_load_ms())
        await _dismiss_common_overlays(page)
        try:
            await page.wait_for_load_state("networkidle", timeout=25000)
        except Exception:
            pass
        await page.wait_for_timeout(1500)
        if await is_tadawul_access_denied_page(page):
            _warn_access_denied()
            await dump_tadawul_navigation_debug(page, f"access_denied_search_{symbol}")
            raise TadawulAccessDeniedError()
        print(f"Navigated to search page for symbol {symbol}")
        search_input = await _wait_for_tadawul_search_input(page)
        await search_input.click()
        await search_input.fill(symbol)
        await page.wait_for_timeout(500)
        await _click_tadawul_search_submit(page)
        await page.wait_for_timeout(2000)
        links = await page.query_selector_all("a.pageLink")
        if os.environ.get("TADAWUL_DEBUG_PAGE_LINKS", "").strip() in (
            "1",
            "true",
            "yes",
        ):
            print("--- <a.pageLink> elements on the page ---")
            for i, link in enumerate(links):
                text = (await link.text_content() or "").strip()
                href = await link.get_attribute("href")
                print(f'{i}: text="{text}", href="{href}"')
            print("--- end of <a.pageLink> debug ---")
        visit_links = []
        for link in links:
            text = (await link.text_content() or "").strip().lower()
            if text == "visit profile":
                visit_links.append(link)
        if not visit_links:
            print(f"❌ No 'Visit Profile' link found for symbol {symbol}")
            return False
        await visit_links[0].click()
        await page.wait_for_load_state("domcontentloaded")
        print(f"✅ Clicked 'Visit Profile' for symbol {symbol}")
        return True
    except TadawulAccessDeniedError:
        raise
    except Exception as e:
        print(f"❌ Search failed for {symbol}: {e}")
        try:
            await dump_tadawul_navigation_debug(page, f"nav_error_{symbol}")
        except Exception:
            pass
        return False


async def get_all_financial_reports(
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
        if year == target_year and stype in ["q1", "q2", "q3"]:
            filtered_reports.append((stype, year, pdf_url))
        elif year == target_year - 1 and stype == "annual":
            filtered_reports.append((stype, year, pdf_url))
    print(
        f"[DEBUG] Will download for {symbol}: {[f'{stype}_{year}' for stype, year, _ in filtered_reports]}"
    )
    return filtered_reports


def _net_profit_scrape_enabled_with_pdf() -> bool:
    """Scrape quarterly net profit in the same browser visit as PDFs unless SKIP_NET_PROFIT_WITH_PDF is set."""
    v = os.environ.get("SKIP_NET_PROFIT_WITH_PDF", "").strip().lower()
    return v not in ("1", "true", "yes")


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
                with open(pdf_path, "wb") as f:
                    f.write(bytes(pdf_content))
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


async def process_company_with_retry(
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
            await page.mouse.move(random.randint(100, 500), random.randint(100, 300))
            await asyncio.sleep(random.uniform(0.5, 1.5))
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
                    await asyncio.sleep(random.uniform(2, 5))
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
                    await asyncio.sleep(random.uniform(2, 5))
                    continue
                return False
            if all_success:
                return True
            elif attempt < max_retries - 1:
                print(f"🔄 Retrying {symbol} (attempt {attempt + 2}/{max_retries})...")
                await asyncio.sleep(random.uniform(2, 5))
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
                await asyncio.sleep(random.uniform(2, 5))
    return False


async def download_all_financial_statements() -> int:
    """Download the most recent financial statements for all companies. Returns 0 on success, 1 if Akamai/WAF blocked."""
    # Get company symbols from JSON file
    companies = get_company_symbols_from_json()
    # companies = ["2030"]  # Test with a single company
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
                progress_path.parent.mkdir(parents=True, exist_ok=True)
                row_status = "finalizing" if _pdf_stop_requested() else "running"
                with open(progress_path, "w", encoding="utf-8") as f:
                    json.dump(
                        {
                            "status": row_status,
                            "processed": processed,
                            "success": success_count,
                            "failed": failed_count,
                            "current_symbol": symbol,
                        },
                        f,
                        ensure_ascii=False,
                    )
            except Exception:
                pass

            # Add delay between companies
            if i < len(companies):
                # If stop requested, skip waiting and break immediately
                if _pdf_stop_requested():
                    print("🛑 Stop requested. Skipping wait and ending now.")
                    break
                delay = random.uniform(3, 7)
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
            with open(progress_path, "w", encoding="utf-8") as f:
                # After API "Stop", extraction runs in parallel; when downloader exits the whole step is done
                if _pdf_stop_requested():
                    end_status = "completed"
                elif waf_aborted:
                    end_status = "blocked_by_waf"
                else:
                    end_status = "completed"
                json.dump(
                    {
                        "status": end_status,
                        "processed": processed,
                        "success": success_count,
                        "failed": failed_count,
                    },
                    f,
                    ensure_ascii=False,
                )
        except Exception:
            pass
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
