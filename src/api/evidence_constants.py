"""Paths, messages, and env-driven settings for the evidence API (no Flask)."""

import os

ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "*")

SCREENSHOTS_RELPATH = "output/screenshots"
FLOW_CSV_RELPATH = "data/results/retained_earnings_flow.csv"
RESULTS_JSON_RELPATH = "data/results/retained_earnings_results.json"
REINVESTED_CSV_RELPATH = "data/results/reinvested_earnings_results.csv"
QUARTERLY_NET_PROFIT_RELPATH = "data/results/quarterly_net_profit.json"
RUNTIME_STOP_PDFS_FLAG = "data/runtime/stop_pdfs_pipeline.flag"
RUNTIME_PDFS_PROGRESS_JSON = "data/runtime/pdfs_progress.json"
RUNTIME_STOP_NET_FLAG = "data/runtime/stop_net_profit.flag"
RUNTIME_NET_PROGRESS_JSON = "data/runtime/net_profit_progress.json"
SCRIPT_CALCULATE_REINVESTED = "src/calculators/calculate_reinvested_earnings.py"
SCRIPT_GENERATE_SCREENSHOTS = "src/utils/generate_evidence_screenshots.py"
MSG_INTERNAL_ERROR = "Internal server error"
MSG_FILE_NOT_FOUND = "File not found"
MSG_OWNERSHIP_UPDATED_OK = "Ownership data updated successfully"
MIME_XLSX = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
MSG_NO_DATA_AR = "لايوجد"

_REF_Q1_2025 = "Q1 2025"
_REF_Q2_2025 = "Q2 2025"
_REF_Q3_2025 = "Q3 2025"
_REF_Q4_2025 = "Q4 2025"
_REF_ANNUAL_2024 = "Annual 2024"
