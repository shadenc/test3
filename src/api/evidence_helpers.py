"""Shared helpers for the evidence API (logging, paths, subprocess, Playwright busy JSON)."""

import json
import logging
import re
import subprocess
import sys
from pathlib import Path

from flask import jsonify

from src.api.evidence_constants import (
    MSG_FILE_NOT_FOUND,
    MSG_INTERNAL_ERROR,
    RUNTIME_NET_PROGRESS_JSON,
    RUNTIME_PDFS_PROGRESS_JSON,
    RUNTIME_STOP_PDFS_FLAG,
)

logger = logging.getLogger(__name__)


def _debug_ignored(operation: str, exc: BaseException) -> None:
    """Log non-fatal failures at DEBUG instead of silent except (Bandit B110 / Sonar)."""
    logger.debug("%s (ignored): %s", operation, exc, exc_info=True)


def _safe_log_symbol(value: object) -> str:
    """Sanitize user-supplied symbol for logs (Sonar pythonsecurity:S5145)."""
    raw = str(value).strip()[:16]
    return raw if re.fullmatch(r"[\w-]+", raw, flags=re.ASCII) else "<invalid_symbol>"


def _safe_log_quarter_param(value: object) -> str:
    raw = str(value).strip()[:48]
    return raw if re.fullmatch(r"\w+", raw, flags=re.ASCII) else "<invalid_quarter>"


def _load_retained_earnings_results(results_file: Path) -> list:
    with open(results_file, "r", encoding="utf-8") as f:
        return json.load(f)


def _find_company_retained_result(results: list, company_symbol: str):
    for result in results:
        if result.get("company_symbol") == company_symbol:
            return result
    return None


def _list_quarter_evidence_screenshots(
    screenshots_dir: Path, company_symbol: str, quarter: str
) -> list:
    """
    Glob PNG paths for a company/quarter; Q1_2025 falls back to annual 2024, then any evidence.
    Shared by screenshot download and extraction-by-company endpoints.
    """
    annual_fallback = None
    if quarter == "Q4_2024":
        primary = f"{company_symbol}_*_q4_2024_evidence.png"
    elif quarter == "Q1_2025":
        primary = f"{company_symbol}_*_q1_2025_evidence.png"
        annual_fallback = f"{company_symbol}_*_annual_2024_evidence.png"
    elif quarter == "Q2_2025":
        primary = f"{company_symbol}_*_q2_2025_evidence.png"
    elif quarter == "Q3_2025":
        primary = f"{company_symbol}_*_q3_2025_evidence.png"
    elif quarter == "Q4_2025":
        primary = f"{company_symbol}_*_q4_2025_evidence.png"
    elif quarter == "Annual_2024":
        primary = f"{company_symbol}_*_annual_2024_evidence.png"
    else:
        primary = f"{company_symbol}_*_evidence.png"

    screenshot_files = list(screenshots_dir.glob(primary))
    if quarter == "Q1_2025" and not screenshot_files and annual_fallback:
        logger.info(
            "No Q1 2025 screenshot found for %s, trying annual 2024 as previous quarter reference",
            _safe_log_symbol(company_symbol),
        )
        screenshot_files = list(screenshots_dir.glob(annual_fallback))
    if not screenshot_files:
        screenshot_files = list(
            screenshots_dir.glob(f"{company_symbol}_*_evidence.png")
        )
    return screenshot_files


def _scheduler_run_script(project_root: Path, log_label: str, script: str) -> None:
    logger.info("%s", log_label)
    subprocess.run(
        [sys.executable, script],
        check=True,
        capture_output=True,
        text=True,
        cwd=str(project_root),
    )


def _run_python_script(project_root: Path, script: str, **kwargs):
    """Run `python script` from repo root; merges kwargs into subprocess.run (dedupes /api/refresh)."""
    opts = {
        "cwd": str(project_root),
        "check": True,
        "capture_output": True,
        "text": True,
    }
    opts.update(kwargs)
    return subprocess.run([sys.executable, script], **opts)


def json_internal_error_response():
    """Single definition for repeated route 500 JSON (Sonar duplication)."""
    return jsonify({"error": MSG_INTERNAL_ERROR}), 500


def json_file_not_found_response():
    return jsonify({"error": MSG_FILE_NOT_FOUND}), 404


def _playwright_busy_response(project_root: Path):
    """409 when PDF downloader or net-profit scraper already holds the Playwright lock."""
    message = (
        "Another Playwright job is running (PDF download or net-profit scrape). "
        "Wait for it to finish, then try again."
    )
    payload = {"status": "busy", "message": message}
    pdfs_st = None
    net_st = None
    try:
        pp = project_root / RUNTIME_PDFS_PROGRESS_JSON
        if pp.exists():
            with open(pp, "r", encoding="utf-8") as f:
                pdfs_st = json.load(f).get("status")
    except Exception as e:
        _debug_ignored("read PDFs progress for busy response", e)
    try:
        np_path = project_root / RUNTIME_NET_PROGRESS_JSON
        if np_path.exists():
            with open(np_path, "r", encoding="utf-8") as f:
                net_st = json.load(f).get("status")
    except Exception as e:
        _debug_ignored("read net profit progress for busy response", e)
    stop_pdf = False
    try:
        stop_pdf = (project_root / RUNTIME_STOP_PDFS_FLAG).exists()
    except Exception as e:
        _debug_ignored("check PDF stop flag for busy response", e)

    if net_st == "running":
        payload["hint"] = (
            "Quarterly net profit scrape is still running. Wait until it finishes, then retry."
        )
        payload["hint_ar"] = (
            "جمع صافي الربح من السوق ما زال يعمل. انتظر حتى تنتهي العملية ثم أعد المحاولة."
        )
    elif pdfs_st == "running":
        payload["hint"] = (
            "PDF download from the exchange is in progress. Wait until it completes or use Stop."
        )
        payload["hint_ar"] = (
            "تحميل ملفات PDF ما زال قيد التشغيل. انتظر حتى ينتهي أو اضغط إيقاف، ثم أعد المحاولة."
        )
    elif pdfs_st == "finalizing":
        payload["hint"] = (
            "After Stop: extraction may be running in the background, while the PDF downloader browser "
            "is still closing (often 10–40 seconds). The lock releases when that process exits."
        )
        payload["hint_ar"] = (
            "بعد «إيقاف»: الاستخراج قد يعمل في الخلفية، والمتصفح الخاص بتحميل PDF ما زال يُغلق "
            "(غالباً 10–40 ثانية). انتظر ثم جرّب تحديث صافي الربح."
        )
    elif pdfs_st == "completed" and stop_pdf:
        payload["hint"] = (
            "Extraction and follow-up steps are done, but the PDF downloader browser is still closing. "
            "Wait a few seconds and retry net profit."
        )
        payload["hint_ar"] = (
            "انتهى الاستخراج والخطوات التالية؛ ما يزال فقط إغلاق متصفح التحميل. "
            "انتظر بضع ثوانٍ ثم جرّب تحديث صافي الربح."
        )
    elif stop_pdf:
        payload["hint"] = (
            "A PDF stop was requested; wait for the downloader process to exit, then retry."
        )
        payload["hint_ar"] = (
            "تم طلب إيقاف التحميل—انتظر حتى تُغلق جلسة المتصفح للتحميل، ثم أعد المحاولة."
        )
    return jsonify(payload), 409
