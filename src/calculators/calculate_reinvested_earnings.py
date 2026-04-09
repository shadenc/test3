#!/usr/bin/env python3
"""
Calculate Retained Earnings Flow (Quarterly Changes)
Flow = Retained Earnings (Current Q) - Retained Earnings (Previous Q)
"""

import json
import math
import pandas as pd
from typing import Dict, List, Optional

FLOW_CSV_PATH = "data/results/retained_earnings_flow.csv"
FLOW_JSON_PATH = "data/results/retained_earnings_flow.json"

_STMT_TYPE_ORDER = {"annual": 0, "q1": 1, "q2": 2, "q3": 3, "q4": 4}


def _reinvested_flow_from_row(row) -> float:
    try:
        if pd.isna(row["flow"]) or pd.isna(row["investor_limit"]):
            return 0.0
        lim = str(row["investor_limit"]).replace("%", "").strip()
        if not lim.replace(".", "").isdigit():
            return 0.0
        pct = float(lim)
        if pct <= 0:
            return 0.0
        return float(row["flow"]) * (pct / 100.0)
    except (TypeError, ValueError, KeyError):
        return 0.0


def _find_statement(statements: List[Dict], stype: str, year: int):
    return next(
        (s for s in statements if s["type"] == stype and s["year"] == year), None
    )


def _append_flow(
    flows: List[Dict],
    quarter: str,
    year: int,
    current: Optional[Dict],
    previous: Optional[Dict],
    formula: str,
) -> None:
    if current is None or previous is None:
        return
    flows.append(
        {
            "quarter": quarter,
            "year": year,
            "current_value": current["value"],
            "previous_value": previous["value"],
            "flow": current["value"] - previous["value"],
            "flow_formula": formula,
        }
    )


def _quarterly_flows_for_year(statements: List[Dict], current_year: int) -> List[Dict]:
    flows: List[Dict] = []
    q1 = _find_statement(statements, "q1", current_year)
    annual_prev = _find_statement(statements, "annual", current_year - 1)
    _append_flow(
        flows,
        "Q1",
        current_year,
        q1,
        annual_prev,
        f"Q1 {current_year} - Annual {current_year - 1}",
    )
    q2 = _find_statement(statements, "q2", current_year)
    _append_flow(
        flows,
        "Q2",
        current_year,
        q2,
        q1,
        f"Q2 {current_year} - Q1 {current_year}",
    )
    q3 = _find_statement(statements, "q3", current_year)
    _append_flow(
        flows,
        "Q3",
        current_year,
        q3,
        q2,
        f"Q3 {current_year} - Q2 {current_year}",
    )
    q4 = _find_statement(statements, "q4", current_year)
    _append_flow(
        flows,
        "Q4",
        current_year,
        q4,
        q3,
        f"Q4 {current_year} - Q3 {current_year}",
    )
    return flows


def _dedupe_statements(statements: List[Dict]) -> List[Dict]:
    """Keep one row per (type, year); last occurrence wins."""
    by_key: Dict[tuple, Dict] = {}
    for s in statements:
        k = (s["type"], s["year"])
        by_key[k] = s
    return list(by_key.values())


def _annual_yoy_fallback_rows(company: str, statements: List[Dict]) -> List[Dict]:
    """
    When quarterly PDFs are missing, approximate using consecutive annual reports.
    Same current/previous/flow is repeated for Q1–Q4 so any dashboard quarter filter shows data;
    true quarter-on-quarter needs quarterly statements.
    """
    annuals = [s for s in statements if s["type"] == "annual"]
    annuals.sort(key=lambda x: x["year"])
    if len(annuals) < 2:
        return []
    pair = None
    for i in range(len(annuals) - 1, 0, -1):
        prev_a, curr_a = annuals[i - 1], annuals[i]
        if curr_a["year"] == prev_a["year"] + 1:
            pair = (prev_a, curr_a)
            break
    if not pair:
        return []
    prev_a, curr_a = pair
    try:
        c = float(curr_a["value"])
        p = float(prev_a["value"])
    except (TypeError, ValueError):
        return []
    delta = c - p
    formula = (
        f"Annual {curr_a['year']} - Annual {prev_a['year']} "
        f"(year-end fallback — download quarterly PDFs for true Q1–Q4)"
    )
    rows = []
    for q in ("Q1", "Q2", "Q3", "Q4"):
        rows.append(
            {
                "company_symbol": company,
                "quarter": q,
                "year": curr_a["year"],
                "current_value": c,
                "previous_value": p,
                "flow": delta,
                "flow_formula": formula,
            }
        )
    return rows


def _single_latest_statement_rows(company: str, statements: List[Dict]) -> List[Dict]:
    """
    One successful statement only (e.g. single annual PDF): show retained earnings in the dashboard
    for every quarter filter; previous period and flow stay empty until more PDFs are added.
    """
    if not statements:
        return []
    best = max(
        statements, key=lambda x: (x["year"], _STMT_TYPE_ORDER.get(x["type"], 999))
    )
    try:
        v = float(best["value"])
    except (TypeError, ValueError):
        return []
    y = int(best["year"])
    t = str(best["type"]).upper()
    formula = f"{t} {y} only — add the prior annual or quarterly PDF to compute quarter-to-quarter flows"
    nan = math.nan
    rows = []
    for q in ("Q1", "Q2", "Q3", "Q4"):
        rows.append(
            {
                "company_symbol": company,
                "quarter": q,
                "year": y,
                "current_value": v,
                "previous_value": nan,
                "flow": nan,
                "flow_formula": formula,
            }
        )
    return rows


def parse_statement_info(filename: str) -> Optional[Dict]:
    """Parse PDF filename to extract company, statement type, and year"""
    # Example: 2222_q1_2025.pdf -> company: 2222, type: q1, year: 2025
    # Example: 2382_annual_2024.pdf -> company: 2382, type: annual, year: 2024

    parts = filename.replace(".pdf", "").split("_")
    if len(parts) >= 3:
        company = parts[0]
        statement_type = parts[1]
        year = int(parts[2])

        return {"company": company, "type": statement_type, "year": year}
    return None


def _flow_rows_for_company(company: str, statements: List[Dict]) -> List[Dict]:
    statements = _dedupe_statements(statements)
    statements.sort(key=lambda x: (x["year"], _STMT_TYPE_ORDER.get(x["type"], 999)))
    if not statements:
        return []
    current_year = max(s["year"] for s in statements)
    company_flows: List[Dict] = []
    for flow in _quarterly_flows_for_year(statements, current_year):
        company_flows.append(
            {
                "company_symbol": company,
                "quarter": flow["quarter"],
                "year": flow["year"],
                "current_value": flow["current_value"],
                "previous_value": flow["previous_value"],
                "flow": flow["flow"],
                "flow_formula": flow["flow_formula"],
            }
        )
    if not company_flows:
        for row in _annual_yoy_fallback_rows(company, statements):
            company_flows.append(row)
    if not company_flows:
        for row in _single_latest_statement_rows(company, statements):
            company_flows.append(row)
    return company_flows


def calculate_retained_earnings_flow(retained_data: List[Dict]) -> List[Dict]:
    """Calculate quarterly flow of retained earnings"""

    # Group by company
    companies = {}
    for item in retained_data:
        if not item.get("success"):
            continue

        company = item["company_symbol"]
        if company not in companies:
            companies[company] = []

        # Parse statement info
        info = parse_statement_info(item["pdf_filename"])
        if not info:
            continue

        companies[company].append(
            {
                "type": info["type"],
                "year": info["year"],
                "value": item["numeric_value"],
                "pdf_filename": item["pdf_filename"],
            }
        )

    flow_results: List[Dict] = []

    for company, statements in companies.items():
        flow_results.extend(_flow_rows_for_company(company, statements))

    return flow_results


def main():  # NOSONAR
    """Main function to calculate retained earnings flow"""
    print("🔄 Calculating Retained Earnings Flow (Quarterly Changes)")
    print("=" * 60)

    # Load retained earnings data
    try:
        with open(
            "data/results/retained_earnings_results.json", "r", encoding="utf-8"
        ) as f:
            retained_data = json.load(f)
        print(f"✅ Loaded {len(retained_data)} retained earnings records")
    except FileNotFoundError:
        print("❌ Error: retained_earnings_results.json not found")
        print("Please run the main extraction script first")
        return
    except Exception as e:
        print(f"❌ Error loading data: {e}")
        return

    # Calculate flows
    print("🔄 Calculating quarterly flows...")
    flow_results = calculate_retained_earnings_flow(retained_data)

    if not flow_results:
        print(
            "⚠️ No flows could be calculated (need consecutive annual PDFs or a full quarterly chain). "
            "Wrote empty retained_earnings_flow.csv."
        )
        empty_cols = [
            "company_symbol",
            "company_name",
            "quarter",
            "year",
            "current_value",
            "previous_value",
            "flow",
            "flow_formula",
            "foreign_ownership",
            "max_allowed",
            "investor_limit",
            "reinvested_earnings_flow",
            "net_profit_foreign_investor",
            "distributed_profits_foreign_investor",
        ]
        pd.DataFrame(columns=empty_cols).to_csv(
            FLOW_CSV_PATH, index=False, encoding="utf-8"
        )
        return

    # Convert to DataFrame for easier manipulation
    flow_df = pd.DataFrame(flow_results)

    # Load ownership data for additional context (prefer JSON, fallback to CSV)
    try:
        try:
            with open(
                "data/ownership/foreign_ownership_data.json", "r", encoding="utf-8"
            ) as f:
                ownership_json = json.load(f)
            ownership_df = pd.DataFrame(ownership_json)
            print(f"✅ Loaded ownership data (JSON) for {len(ownership_df)} companies")
        except FileNotFoundError:
            ownership_df = pd.read_csv("data/ownership/foreign_ownership_data.csv")
            print(f"✅ Loaded ownership data (CSV) for {len(ownership_df)} companies")

        # Normalize columns
        if (
            "symbol" not in ownership_df.columns
            and "company_symbol" in ownership_df.columns
        ):
            ownership_df = ownership_df.rename(columns={"company_symbol": "symbol"})

        # Merge with ownership data
        flow_df["company_symbol"] = flow_df["company_symbol"].astype(str)
        ownership_df["symbol"] = ownership_df["symbol"].astype(str)

        merged = pd.merge(
            flow_df,
            ownership_df[
                [
                    "symbol",
                    "company_name",
                    "foreign_ownership",
                    "max_allowed",
                    "investor_limit",
                ]
            ],
            left_on="company_symbol",
            right_on="symbol",
            how="left",
        )

        merged["reinvested_earnings_flow"] = merged.apply(
            _reinvested_flow_from_row, axis=1
        )

        # Load net profit data for additional calculations
        try:
            with open(
                "data/results/quarterly_net_profit.json", "r", encoding="utf-8"
            ) as f:
                net_profit_data = json.load(f)
            print(f"✅ Loaded net profit data for {len(net_profit_data)} companies")

            # Convert net profit data to lookup format
            net_profit_lookup = {}
            for company in net_profit_data:
                symbol = company.get("company_symbol")
                if symbol:
                    net_profit_lookup[symbol] = company

            # Prepare raw net profit per quarter (None if missing)
            def get_raw_net_profit(symbol: str, quarter: str, year: int):
                company = net_profit_lookup.get(str(symbol), {})
                qmap = company.get("quarterly_net_profit", {}) if company else {}
                key = f"{quarter} {year}"
                return qmap.get(key, None) if qmap else None

            # Investor limit fraction
            def investor_fraction(val):
                if pd.isna(val):
                    return 0.0
                s = str(val).replace("%", "")
                if not s.replace(".", "").isdigit():
                    return 0.0
                try:
                    return float(s) / 100.0
                except Exception:
                    return 0.0

            # Compute raw net profit and calc/display values
            merged["__raw_net_profit"] = merged.apply(
                lambda row: get_raw_net_profit(
                    row["company_symbol"], row["quarter"], row["year"]
                ),
                axis=1,
            )
            merged["__inv_frac"] = merged["investor_limit"].apply(investor_fraction)
            # Numeric for calculations: use 0 when missing
            merged["__net_profit_foreign_investor_calc"] = merged.apply(
                lambda row: (
                    (
                        row["__raw_net_profit"]
                        if row["__raw_net_profit"] is not None
                        else 0
                    )
                    * row["__inv_frac"]
                ),
                axis=1,
            )
            # Display column: empty string when raw net profit is missing
            merged["net_profit_foreign_investor"] = merged.apply(
                lambda row: (
                    row["__net_profit_foreign_investor_calc"]
                    if row["__raw_net_profit"] is not None
                    else ""
                ),
                axis=1,
            )
            # Calculate distributed using calc numeric regardless of display
            merged["distributed_profits_foreign_investor"] = merged.apply(
                lambda row: (
                    row["__net_profit_foreign_investor_calc"]
                    - row["reinvested_earnings_flow"]
                    if pd.notna(row["reinvested_earnings_flow"])
                    else 0
                ),
                axis=1,
            )

            print("✅ Added net profit calculations for foreign investors")

        except FileNotFoundError:
            print(
                "⚠️ Warning: quarterly_net_profit.json not found, skipping net profit calculations"
            )
            merged["net_profit_foreign_investor"] = 0
            merged["distributed_profits_foreign_investor"] = 0
        except Exception as e:
            print(f"⚠️ Warning: Error processing net profit data: {e}")
            merged["net_profit_foreign_investor"] = 0
            merged["distributed_profits_foreign_investor"] = 0

        # Clean up the merged data
        final_results = merged[
            [
                "company_symbol",
                "company_name",
                "quarter",
                "year",
                "current_value",
                "previous_value",
                "flow",
                "flow_formula",
                "foreign_ownership",
                "max_allowed",
                "investor_limit",
                "reinvested_earnings_flow",
                "net_profit_foreign_investor",
                "distributed_profits_foreign_investor",
            ]
        ].copy()

        print(f"✅ Calculated flows for {len(final_results)} company-quarters")
        print("✅ Added foreign investor flow calculations")

        # Save to CSV
        final_results.to_csv(FLOW_CSV_PATH, index=False, encoding="utf-8")
        print(f"✅ Saved flow data to {FLOW_CSV_PATH}")

        # Save to JSON for debugging
        final_results.to_json(
            FLOW_JSON_PATH, orient="records", force_ascii=False, indent=2
        )
        print(f"✅ Saved flow data to {FLOW_JSON_PATH}")

        # New: Save compact per-quarter foreign investor metrics
        compact = final_results[
            [
                "company_symbol",
                "company_name",
                "quarter",
                "year",
                "reinvested_earnings_flow",
                "net_profit_foreign_investor",
                "distributed_profits_foreign_investor",
            ]
        ].copy()
        compact_json_path = "data/results/foreign_investor_results.json"
        compact.to_json(
            compact_json_path, orient="records", force_ascii=False, indent=2
        )
        print(f"✅ Saved foreign investor metrics to {compact_json_path}")

        # Display sample results
        print("\n📊 Sample Flow Results:")
        print("=" * 80)
        for _, row in final_results.head(10).iterrows():
            print(f"Company: {row['company_name']} ({row['company_symbol']})")
            print(f"Quarter: {row['quarter']} {row['year']}")
            print(f"Flow: {row['flow']:,.0f} SAR ({row['flow_formula']})")
            print(f"Foreign Investor Flow: {row['reinvested_earnings_flow']:,.2f} SAR")
            print(
                f"Net Profit for Foreign Investor: {row['net_profit_foreign_investor']:,.2f} SAR"
            )
            print(
                f"Distributed Profits for Foreign Investor: {row['distributed_profits_foreign_investor']:,.2f} SAR"
            )
            print("-" * 40)

    except FileNotFoundError:
        print("⚠️ Warning: ownership data not found, saving basic flow data only")
        # Save basic flow data without ownership calculations
        flow_df.to_csv(FLOW_CSV_PATH, index=False, encoding="utf-8")
        print(f"✅ Saved basic flow data to {FLOW_CSV_PATH}")

        flow_df.to_json(FLOW_JSON_PATH, orient="records", force_ascii=False, indent=2)
        print(f"✅ Saved basic flow data to {FLOW_JSON_PATH}")

    except Exception as e:
        print(f"❌ Error processing ownership data: {e}")
        # Save basic flow data as fallback
        flow_df.to_csv(FLOW_CSV_PATH, index=False, encoding="utf-8")
        print(f"✅ Saved basic flow data to {FLOW_CSV_PATH}")

    print("\n🎉 Flow calculation completed successfully!")


if __name__ == "__main__":
    main()
