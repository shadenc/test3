"""
Shared Playwright browser setup and Tadawul portal navigation for PDF and net-profit scrapers.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from playwright.async_api import (
    async_playwright,
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

COMPANY_PROFILE_MAIN_URL = (
    "https://www.saudiexchange.sa/wps/portal/saudiexchange/companies/company-profile-main/"
)

# Deep link observed on Saudi Exchange (may avoid /hidden/search/ WAF rules on some networks).
_COMPANY_PROFILE_DIRECT_TMPL = (
    "https://www.saudiexchange.sa/wps/portal/saudiexchange/hidden/company-profile-main/"
    "!ut/p/z1/04_Sj9CPykssy0xPLMnMz0vMAfIjo8ziTR3NDIw8LAz83d2MXA0C3SydAl1c3Q0NvE30I4EKzBEKDMKcTQzMDPxN3H19LAzdTU31w8syU8v1wwkpK8hOMgUA-oskdg!!/"
    "?companySymbol={symbol}"
)

_SEARCH_INPUT_LOCATOR_CSS = (
    "#query-input, input#query-input, input[name='query'], input[name='Query'], "
    "input[type='search'], input.srchInput, input[placeholder*='Search' i], "
    "input[aria-label*='Search' i], input[title*='Search' i]"
)

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


def _playwright_headless() -> bool:
    """Default headless=True; set PLAYWRIGHT_HEADLESS=0 to show the browser for debugging."""
    v = os.environ.get("PLAYWRIGHT_HEADLESS", "1").strip().lower()
    return v not in ("0", "false", "no", "off")


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
    """Setup Playwright browser with stealth configuration (PDF downloads enabled)."""
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


async def navigate_to_company_profile(page: Page, symbol: str) -> bool:  # NOSONAR
    """Open company profile: try direct ?companySymbol= link, then homepage warm-up + legacy search page."""
    search_url = "https://www.saudiexchange.sa/wps/portal/saudiexchange/hidden/search/!ut/p/z0/04_Sj9CPykssy0xPLMnMz0vMAfIjo8ziTR3NDIw8LAz8DTxCnA3MDILdzUJDLAyNHI30C7IdFQEEx_vC/"
    direct_candidates = [
        _COMPANY_PROFILE_DIRECT_TMPL.format(symbol=symbol),
        f"{COMPANY_PROFILE_MAIN_URL.rstrip('/')}/?companySymbol={symbol}",
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


__all__ = [
    "COMPANY_PROFILE_MAIN_URL",
    "get_company_symbols_from_json",
    "navigate_to_company_profile",
    "setup_stealth_browser",
]
