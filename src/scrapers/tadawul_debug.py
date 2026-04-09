"""Save Playwright state when Tadawul search navigation fails (debugging)."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from playwright.async_api import Page

ACCESS_DENIED_HELP = (
    "Saudi Exchange (Akamai) returned “Access Denied” — not a missing search box. "
    "Try: run from a normal home/office IP (not a datacenter/VPN), set PLAYWRIGHT_CHANNEL=chrome "
    "to use your installed Google Chrome, and PLAYWRIGHT_HEADLESS=0 once to confirm in a real window."
)


class TadawulAccessDeniedError(RuntimeError):
    """WAF block: retrying the same company or IP will not help."""

    def __init__(self, message: str = ACCESS_DENIED_HELP):
        super().__init__(message)


async def is_tadawul_access_denied_page(page: "Page") -> bool:
    """Detect Akamai / WAF block pages (no usable search UI)."""
    try:
        title = (await page.title() or "").lower()
        if "access denied" in title:
            return True
        snippet = await page.evaluate(
            """() => {
          const b = document.body;
          if (!b || !b.innerText) return '';
          return b.innerText.slice(0, 3000).toLowerCase();
        }"""
        )
        if "access denied" in snippet and (
            "permission" in snippet or "edgesuite" in snippet or "akamai" in snippet
        ):
            return True
        html = (await page.content()).lower()
        if "errors.edgesuite.net" in html:
            return True
    except Exception:
        pass
    return False


async def dump_tadawul_navigation_debug(page: "Page", label: str) -> Path:
    """
    Write screenshot, HTML, frame list, and input inventory under data/runtime/tadawul_debug/.
    """
    out_dir = Path("data/runtime/tadawul_debug")
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    stem = out_dir / f"{ts}_{label}"

    try:
        await page.screenshot(path=str(stem) + ".png", full_page=True)
    except Exception:
        pass
    try:
        html = await page.content()
        stem.with_suffix(".html").write_text(html, encoding="utf-8", errors="replace")
    except Exception:
        pass
    try:
        lines = []
        for i, fr in enumerate(page.frames):
            lines.append(f"[{i}] {fr.url}\n")
        stem.with_suffix(".frames.txt").write_text("".join(lines), encoding="utf-8")
    except Exception:
        pass
    try:
        inventory = await page.evaluate(
            """() => {
          const out = { inputs: [], url: location.href, title: document.title };
          for (const el of document.querySelectorAll('input,textarea')) {
            const r = el.getBoundingClientRect();
            out.inputs.push({
              tag: el.tagName,
              id: el.id || null,
              name: el.name || null,
              type: el.type || null,
              placeholder: el.placeholder || null,
              className: el.className || null,
              visible: r.width > 0 && r.height > 0,
              ariaLabel: el.getAttribute('aria-label')
            });
          }
          return out;
        }"""
        )
        stem.with_suffix(".inputs.json").write_text(
            json.dumps(inventory, indent=2, ensure_ascii=False), encoding="utf-8"
        )
    except Exception:
        pass

    print(f"📝 Tadawul debug artifacts written under {stem}.*")
    return stem
