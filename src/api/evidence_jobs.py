"""Scheduled jobs and Playwright-backed pipelines (PDF download, net profit scrape)."""

import json
import logging
import os
import shutil
import subprocess
import sys
import threading
from datetime import datetime
from pathlib import Path

import pandas as pd

from src.api.evidence_constants import (
    FLOW_CSV_RELPATH,
    MSG_NO_DATA_AR,
    QUARTERLY_NET_PROFIT_RELPATH,
    RUNTIME_NET_PROGRESS_JSON,
    RUNTIME_PDFS_PROGRESS_JSON,
    RUNTIME_STOP_NET_FLAG,
    RUNTIME_STOP_PDFS_FLAG,
    SCREENSHOTS_RELPATH,
    SCRIPT_CALCULATE_REINVESTED,
    SCRIPT_GENERATE_SCREENSHOTS,
)
from src.api.evidence_helpers import _debug_ignored, _scheduler_run_script

logger = logging.getLogger(__name__)

PLAYWRIGHT_SUBPROCESS_LOCK = threading.Lock()


def run_quarterly_refresh_and_archive(project_root: Path) -> None:  # NOSONAR
    """
    Quarterly refresh: recalc, screenshots, export, archive.
    Module-level to keep create_app() cognitive complexity low.
    """
    try:
        logger.info("[Scheduler] Running quarterly refresh and archive...")

        _scheduler_run_script(
            project_root,
            "[Scheduler] Step 1: Recalculating reinvested earnings...",
            SCRIPT_CALCULATE_REINVESTED,
        )
        logger.info("[Scheduler] ✅ Reinvested earnings calculation completed")

        _scheduler_run_script(
            project_root,
            "[Scheduler] Step 2: Regenerating evidence screenshots...",
            SCRIPT_GENERATE_SCREENSHOTS,
        )
        logger.info("[Scheduler] ✅ Evidence screenshots regeneration completed")

        logger.info("[Scheduler] Step 3: Exporting dashboard table for each quarter...")

        from src.utils.export_to_excel import ExcelExporter

        exporter = ExcelExporter()

        ownership_json_path = (
            project_root / "data/ownership/foreign_ownership_data.json"
        )
        if not ownership_json_path.exists():
            logger.error("[Scheduler] ❌ Ownership data file not found")
            return

        with open(ownership_json_path, "r", encoding="utf-8") as f:
            ownership_data = json.load(f)

        csv_path = project_root / FLOW_CSV_RELPATH
        if not csv_path.exists():
            logger.error("[Scheduler] ❌ Retained earnings flow data file not found")
            return

        flow_data = pd.read_csv(csv_path)

        net_profit_path = project_root / QUARTERLY_NET_PROFIT_RELPATH
        net_profit_data = _scheduler_load_net_profit_map(net_profit_path)

        now = datetime.now()
        current_quarter, previous_quarter, current_year, previous_year = (
            _scheduler_calendar_quarters(now)
        )

        logger.info(f"[Scheduler] Current quarter: {current_quarter} {current_year}")
        logger.info(f"[Scheduler] Previous quarter: {previous_quarter} {previous_year}")

        flow_map = _scheduler_build_flow_map(flow_data)

        logger.info(
            f"[Scheduler] Exporting data for {current_quarter} {current_year}..."
        )

        merged_data = []
        for ownership_row in ownership_data:
            symbol = str(ownership_row.get("symbol", "")).strip()
            flow_info = flow_map.get(symbol, {})
            net_profit_info = net_profit_data.get(symbol, {})

            quarter_data = flow_info.get(current_quarter, {})

            net_profit_value = MSG_NO_DATA_AR
            if net_profit_info and "quarterly_net_profit" in net_profit_info:
                quarter_key = f"{current_quarter} {current_year}"
                if quarter_key in net_profit_info["quarterly_net_profit"]:
                    net_profit_value = net_profit_info["quarterly_net_profit"][
                        quarter_key
                    ]

            if current_quarter == "Q1":
                previous_quarter_header = f"{previous_year}Q4"
            else:
                previous_quarter_header = f"{current_year}{previous_quarter}"

            current_quarter_header = f"{current_year}{current_quarter}"

            merged_row = {
                "رمز الشركة": symbol,
                "الشركة": ownership_row.get("company_name", ""),
                "ملكية جميع المستثمرين الأجانب": ownership_row.get(
                    "foreign_ownership", ""
                ),
                "الملكية الحالية": ownership_row.get("max_allowed", ""),
                "ملكية المستثمر الاستراتيجي الأجنبي": ownership_row.get(
                    "investor_limit", ""
                ),
                f"الأرباح المبقاة للربع السابق ({previous_quarter_header})": _scheduler_export_format_value(
                    quarter_data.get("previous_value", "")
                ),
                f"الأرباح المبقاة للربع الحالي ({current_quarter_header})": _scheduler_export_format_value(
                    quarter_data.get("current_value", "")
                ),
                "حجم الزيادة أو النقص في الأرباح المبقاة (التدفق)": _scheduler_export_format_value(
                    quarter_data.get("flow", "")
                ),
                "تدفق الأرباح المبقاة للمستثمر الأجنبي": _scheduler_export_format_value(
                    quarter_data.get("reinvested_earnings_flow", "")
                ),
                "صافي الربح": net_profit_value,
                "صافي الربح للمستثمر الأجنبي": _scheduler_export_format_value(
                    quarter_data.get("net_profit_foreign_investor", "")
                ),
                "الأرباح الموزعة للمستثمر الأجنبي": _scheduler_export_format_value(
                    quarter_data.get("distributed_profits_foreign_investor", "")
                ),
            }
            merged_data.append(merged_row)

        data = pd.DataFrame(merged_data)

        output_path = exporter.export_dashboard_table(data)

        if output_path:
            archive_dir = (
                project_root / f"output/archives/{current_year}_{current_quarter}"
            )
            archive_dir.mkdir(parents=True, exist_ok=True)

            archive_excel_name = (
                f"financial_analysis_{current_year}_{current_quarter}.xlsx"
            )
            archive_excel_path = archive_dir / archive_excel_name
            shutil.copy(output_path, archive_excel_path)

            archive_csv_name = (
                f"retained_earnings_flow_{current_year}_{current_quarter}.csv"
            )
            archive_csv_path = archive_dir / archive_csv_name
            shutil.copy(csv_path, archive_csv_path)

            screenshots_archive_dir = archive_dir / "evidence_screenshots"
            screenshots_archive_dir.mkdir(exist_ok=True)

            screenshots_dir = project_root / SCREENSHOTS_RELPATH
            if screenshots_dir.exists():
                quarter_pattern = (
                    f"*_{current_quarter.lower()}_{current_year}_evidence.png"
                )
                for screenshot in screenshots_dir.glob(quarter_pattern):
                    shutil.copy(screenshot, screenshots_archive_dir / screenshot.name)

            logger.info(f"[Scheduler] ✅ Archived results to {archive_dir}")
            logger.info(f"[Scheduler] ✅ Excel file: {archive_excel_name}")
            logger.info(f"[Scheduler] ✅ CSV file: {archive_csv_name}")
            logger.info("[Scheduler] ✅ Evidence screenshots copied")

        else:
            logger.error("[Scheduler] ❌ Failed to export Excel file")

    except Exception as e:
        logger.error(f"[Scheduler] ❌ Error in scheduled refresh: {e}")
        import traceback

        logger.error(f"[Scheduler] Traceback: {traceback.format_exc()}")


def _scheduler_export_format_value(value: object) -> str:
    if value == "" or value is None:
        return MSG_NO_DATA_AR
    if value == 0 or (isinstance(value, str) and value.strip() == "0"):
        return "0"
    return value


def _scheduler_build_flow_map(flow_data: pd.DataFrame) -> dict:
    flow_map: dict = {}
    for _, row in flow_data.iterrows():
        symbol = str(row.get("company_symbol", "")).strip()
        quarter = str(row.get("quarter", "")).strip()
        if symbol and quarter:
            if symbol not in flow_map:
                flow_map[symbol] = {}
            flow_map[symbol][quarter] = {
                "previous_value": row.get("previous_value", ""),
                "current_value": row.get("current_value", ""),
                "flow": row.get("flow", ""),
                "flow_formula": row.get("flow_formula", ""),
                "year": row.get("year", ""),
                "reinvested_earnings_flow": row.get("reinvested_earnings_flow", ""),
                "net_profit_foreign_investor": row.get(
                    "net_profit_foreign_investor", ""
                ),
                "distributed_profits_foreign_investor": row.get(
                    "distributed_profits_foreign_investor", ""
                ),
            }
    return flow_map


def _scheduler_load_net_profit_map(net_profit_path: Path) -> dict:
    net_profit_data: dict = {}
    if not net_profit_path.exists():
        return net_profit_data
    with open(net_profit_path, "r", encoding="utf-8") as f:
        net_profit_raw = json.load(f)
    for company in net_profit_raw:
        symbol = company.get("company_symbol")
        if symbol:
            net_profit_data[symbol] = company
    return net_profit_data


def _scheduler_calendar_quarters(now: datetime) -> tuple[str, str, int, int]:
    """Returns (current_quarter, previous_quarter, current_year, previous_year)."""
    current_year = now.year
    current_month = now.month
    if current_month in (1, 2, 3):
        return "Q1", "Q4", current_year, current_year - 1
    if current_month in (4, 5, 6):
        return "Q2", "Q1", current_year, current_year
    if current_month in (7, 8, 9):
        return "Q3", "Q2", current_year, current_year
    return "Q4", "Q3", current_year, current_year


def run_daily_ownership_scraper_and_recalc(project_root: Path) -> None:
    """Daily job: ownership JSON + recalc flows."""
    try:
        logger.info("[Scheduler] Running daily ownership update and recalculation...")
        try:
            logger.info(
                "[Scheduler] Step 1: Updating foreign ownership via Tadawul scraper..."
            )
            from src.scrapers.ownership import TadawulOwnershipScraper

            scraper = TadawulOwnershipScraper(base_url="https://www.saudiexchange.sa")
            scraper.scrape_to_files(
                output_dir=str(project_root / "data/ownership"), debug=False
            )
            logger.info("[Scheduler] ✅ Ownership data updated")
        except Exception as e:
            logger.error(f"[Scheduler] ❌ Ownership update failed: {e}")

        try:
            logger.info(
                "[Scheduler] Step 2: Recalculating reinvested earnings flows..."
            )
            subprocess.run(
                [sys.executable, SCRIPT_CALCULATE_REINVESTED],
                check=True,
                capture_output=True,
                text=True,
                cwd=str(project_root),
            )
            logger.info("[Scheduler] ✅ Recalculation finished")
        except subprocess.CalledProcessError as e:
            logger.error(f"[Scheduler] ❌ Recalculation failed: {e.stderr}")
    except Exception as e:
        logger.error(f"[Scheduler] ❌ Unexpected error in daily ownership job: {e}")


def _run_pdfs_pipeline_task(project_root: Path, downloader: Path, extractor: Path) -> None:
    downloader_ok = False
    try:
        try:
            stop_flag_file = project_root / RUNTIME_STOP_PDFS_FLAG
            if stop_flag_file.exists():
                stop_flag_file.unlink()
            net_stop_stale = project_root / RUNTIME_STOP_NET_FLAG
            if net_stop_stale.exists():
                net_stop_stale.unlink()
        except Exception as e:
            _debug_ignored("clear stale stop flags before PDF pipeline", e)
        try:
            progress_path = project_root / RUNTIME_PDFS_PROGRESS_JSON
            progress_path.parent.mkdir(parents=True, exist_ok=True)
            with open(progress_path, "w", encoding="utf-8") as f:
                json.dump({"status": "running", "processed": 0}, f)
        except Exception as e:
            _debug_ignored("write PDFs progress running", e)
        logger.info("[Pipeline] Starting hybrid downloader...")
        env = os.environ.copy()
        env.setdefault("STOP_FLAG_FILE", str(project_root / RUNTIME_STOP_PDFS_FLAG))
        env.setdefault(
            "PROGRESS_FILE", str(project_root / RUNTIME_PDFS_PROGRESS_JSON)
        )
        subprocess.run(
            [sys.executable, str(downloader)],
            cwd=str(project_root),
            check=True,
            text=True,
            env=env,
        )
        downloader_ok = True
    except subprocess.CalledProcessError as e:
        logger.error(f"[Pipeline] Downloader failed: {e}")
    except Exception as e:
        logger.error(f"[Pipeline] Downloader crashed: {e}")
    finally:
        PLAYWRIGHT_SUBPROCESS_LOCK.release()

    if not downloader_ok:
        return
    try:
        stop_flag_file = project_root / RUNTIME_STOP_PDFS_FLAG
        if stop_flag_file.exists():
            logger.info(
                "[Pipeline] Stop flag detected after downloader. Skipping extractor/calc/screenshots (finalization thread handles them)."
            )
            return
    except Exception as e:
        _debug_ignored("check stop flag after PDF downloader", e)
    try:
        logger.info("[Pipeline] Starting retained earnings extractor...")
        subprocess.run(
            [sys.executable, str(extractor)],
            cwd=str(project_root),
            check=True,
            text=True,
        )
    except subprocess.CalledProcessError as e:
        logger.error(f"[Pipeline] Extractor failed: {e}")
        return
    try:
        logger.info("[Pipeline] Recalculating reinvested earnings...")
        calc = project_root / SCRIPT_CALCULATE_REINVESTED
        subprocess.run(
            [sys.executable, str(calc)],
            cwd=str(project_root),
            check=True,
            text=True,
        )
    except subprocess.CalledProcessError as e:
        logger.error(f"[Pipeline] Calculation failed: {e}")
        return
    try:
        logger.info("[Pipeline] Regenerating evidence screenshots...")
        shots = project_root / SCRIPT_GENERATE_SCREENSHOTS
        subprocess.run(
            [sys.executable, str(shots)],
            cwd=str(project_root),
            check=True,
            text=True,
        )
    except subprocess.CalledProcessError as e:
        logger.warning(f"[Pipeline] Screenshot regeneration failed: {e}")
    try:
        progress_path = project_root / RUNTIME_PDFS_PROGRESS_JSON
        with open(progress_path, "w", encoding="utf-8") as f:
            json.dump({"status": "completed"}, f)
    except Exception as e:
        _debug_ignored("write PDFs progress completed", e)
    try:
        stop_flag_file = project_root / RUNTIME_STOP_PDFS_FLAG
        if stop_flag_file.exists():
            stop_flag_file.unlink()
    except Exception as e:
        _debug_ignored("unlink PDF stop flag after pipeline", e)
    logger.info(
        "[Pipeline] ✅ Pipeline completed (download → extract → calculate → screenshots)"
    )


def _run_net_profit_pipeline_task(project_root: Path, scraper: Path) -> None:  # NOSONAR
    scraper_ok = False
    try:
        logger.info("[NetProfit] Starting scraper...")
        try:
            net_stop_flag = project_root / RUNTIME_STOP_NET_FLAG
            if net_stop_flag.exists():
                net_stop_flag.unlink()
        except Exception as e:
            _debug_ignored("clear net profit stop flag before scrape", e)
        try:
            net_progress = project_root / RUNTIME_NET_PROGRESS_JSON
            net_progress.parent.mkdir(parents=True, exist_ok=True)
            with open(net_progress, "w", encoding="utf-8") as f:
                json.dump({"status": "running", "processed": 0}, f)
        except Exception as e:
            _debug_ignored("write net profit progress running", e)
        env = os.environ.copy()
        env.setdefault(
            "STOP_FLAG_FILE", str(project_root / RUNTIME_STOP_NET_FLAG)
        )
        env.setdefault(
            "PROGRESS_FILE", str(project_root / RUNTIME_NET_PROGRESS_JSON)
        )
        proc = subprocess.run(
            [sys.executable, str(scraper)],
            cwd=str(project_root),
            text=True,
            env=env,
        )
        if proc.returncode == 0:
            scraper_ok = True
        else:
            prog_path = project_root / RUNTIME_NET_PROGRESS_JSON
            blocked = False
            try:
                if prog_path.exists():
                    with open(prog_path, "r", encoding="utf-8") as fp:
                        blocked = json.load(fp).get("status") == "blocked_by_waf"
            except Exception as e:
                _debug_ignored("read net profit progress for WAF check", e)
            if blocked:
                logger.warning(
                    "[NetProfit] Scraper exited after Saudi Exchange Access Denied (Akamai); "
                    "no recalc. Fix network/browser (e.g. PLAYWRIGHT_CHANNEL=chrome) and retry."
                )
            else:
                logger.error(
                    f"[NetProfit] Scraper failed with exit code {proc.returncode}"
                )
    except Exception as e:
        logger.error(f"[NetProfit] Scraper crashed: {e}")
    finally:
        PLAYWRIGHT_SUBPROCESS_LOCK.release()

    if not scraper_ok:
        return
    try:
        logger.info(
            "[NetProfit] Recalculating flows after net profit update..."
        )
        calc = project_root / SCRIPT_CALCULATE_REINVESTED
        subprocess.run(
            [sys.executable, str(calc)],
            cwd=str(project_root),
            check=True,
            text=True,
        )
        logger.info("[NetProfit] ✅ Completed")
    except subprocess.CalledProcessError as e:
        logger.error(f"[NetProfit] Recalculation failed: {e}")
    finally:
        try:
            net_stop_flag = project_root / RUNTIME_STOP_NET_FLAG
            if net_stop_flag.exists():
                net_stop_flag.unlink()
        except Exception as e:
            _debug_ignored("clear net profit stop flag after scrape", e)
