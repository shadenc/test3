#!/usr/bin/env python3
"""
Simple Retained Earnings Extraction from Financial Statement PDFs
Focused on extracting only retained earnings values with minimal complexity
"""

import fitz  # PyMuPDF
import re
import json
from pathlib import Path
import sqlite3
from datetime import datetime
import openai
import os
from typing import Dict, List, Optional
import logging
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env")

# OpenAI API key: set OPENAI_API_KEY in the environment or in a project-root .env file
openai.api_key = os.getenv("OPENAI_API_KEY")


class EvidenceScreenshotGenerator:
    """Simple screenshot generator for evidence"""

    def generate_highlight_screenshot(
        self, pdf_path: str, search_value: str, company_symbol: str
    ) -> Optional[str]:
        """Generate screenshot highlighting the found value and unit text on the same page"""
        try:
            import fitz

            doc = fitz.open(pdf_path)
            screenshots_dir = Path("output/screenshots")
            screenshots_dir.mkdir(parents=True, exist_ok=True)

            # Get PDF filename for unique naming
            pdf_filename = Path(pdf_path).stem  # Remove .pdf extension

            # Find page with the search value and highlight it
            for page_num in range(len(doc)):
                page = doc[page_num]
                text = page.get_text()
                if search_value in text:
                    # Found the page, now highlight the value
                    page = self._highlight_value_on_page(page, search_value)

                    # Also try to highlight the unit declaration on the same page
                    try:
                        self._highlight_units_on_page(page, text)
                    except Exception as e:
                        logger.warning(f"Unit highlight failed: {e}")

                    # Take screenshot with highlighting - use unique filename
                    pix = page.get_pixmap(
                        matrix=fitz.Matrix(2, 2)
                    )  # 2x zoom for better quality
                    screenshot_path = (
                        screenshots_dir
                        / f"{company_symbol}_{pdf_filename}_evidence.png"
                    )
                    pix.save(str(screenshot_path))
                    doc.close()
                    return str(screenshot_path)

            doc.close()
            return None

        except Exception as e:
            logger.error(f"Screenshot error: {e}")
            return None

    def _highlight_value_on_page(self, page, search_value: str):
        """Highlight the found value on the page with a yellow highlighter effect"""
        try:
            # Search for the text on the page
            text_instances = page.search_for(search_value)

            if text_instances:
                # Get the first instance and highlight it
                rect = text_instances[0]  # First occurrence

                # Draw a yellow highlighter effect around the found text
                highlight_rect = page.add_rect_annot(rect)
                highlight_rect.set_colors(stroke=(1, 1, 0))  # Yellow stroke
                highlight_rect.set_colors(fill=(1, 1, 0))  # Yellow fill
                highlight_rect.set_opacity(0.3)  # Semi-transparent yellow

                logger.info(
                    f"Highlighted value '{search_value}' on page with yellow highlighter"
                )

            return page

        except Exception as e:
            logger.error(f"Highlighting error: {e}")
            return page

    def _highlight_units_on_page(self, page, _page_text: str) -> None:
        """Attempt to find and highlight unit declaration text on the page.
        Draw a second rectangle (green) around the first matching unit phrase.
        """
        # Candidate phrases (simple contains search via page.search_for)
        unit_phrases = [
            # English
            "in millions of saudi riyals",
            "in million of saudi riyals",
            "in millions",
            "in million",
            "millions of saudi riyals",
            "in thousands of saudi riyals",
            "in thousand of saudi riyals",
            "in thousands",
            "in thousand",
            "thousands of saudi riyals",
            "saudi riyals",
            "SAR",
            # Arabic
            "بالملايين",
            "الملايين",
            "مليون",
            "بالآلاف",
            "بالالاف",
            "ألف",
            "الآلاف",
            "بالريال السعودي",
            "ريال سعودي",
            "ريال",
        ]
        # Try longer phrases first for specificity
        unit_phrases.sort(key=len, reverse=True)
        for phrase in unit_phrases:
            areas = page.search_for(phrase, quads=False)
            if areas:
                rect = areas[0]
                try:
                    unit_rect = page.add_rect_annot(rect)
                    # Differentiate color from value highlight (green)
                    unit_rect.set_colors(stroke=(0, 1, 0))
                    unit_rect.set_colors(fill=(0, 1, 0))
                    unit_rect.set_opacity(0.25)
                    logger.info(f"Highlighted unit phrase '{phrase}' on page")
                except Exception as e:
                    logger.warning(f"Failed to draw unit rectangle: {e}")
                break


RETAINED_EARNINGS_LABEL = "retained earnings"


class RetainedEarningsExtractor:
    def __init__(self):
        self.target_years = []
        self.most_recent_year = None

    def detect_years(self, text: str) -> List[int]:
        """Detect available years in the financial statement"""
        current_year = datetime.now().year

        # Look for 4-digit years (2020-2030 range)
        year_pattern = r"\b(20[2-3]\d)\b"
        years_found = re.findall(year_pattern, text)

        # Convert to integers and filter realistic years
        realistic_years = []
        for year in set(int(y) for y in years_found):
            if current_year - 10 <= year <= current_year + 1:
                realistic_years.append(year)

        # Sort by most recent first
        realistic_years.sort(reverse=True)
        self.target_years = realistic_years
        self.most_recent_year = realistic_years[0] if realistic_years else None

        logger.info(f"Detected years: {realistic_years}")
        return realistic_years

    # --- New: Unit detection helpers ---
    def _detect_units_from_text(self, text: str) -> Dict[str, object]:
        """Detect unit declarations in nearby text. Returns dict with unit and multiplier."""
        try:
            lowered = text.lower()
            # English patterns
            english_million = re.search(
                r"all\s+amounts?.*in\s+millions?\s+of\s+saudi\s+riyals|in\s+millions\b|millions\s+of\s+saudi\s+riyals",
                lowered,
            )
            english_thousand = re.search(
                r"all\s+amounts?.*in\s+thousands?\s+of\s+saudi\s+riyals|in\s+thousands\b|thousands\s+of\s+saudi\s+riyals",
                lowered,
            )
            english_sar = re.search(r"saudi\s+riyals?|\bSAR\b", lowered)

            # Arabic patterns (approximate common variants)
            arabic_million = re.search(r"بالملايين|\bمليون\b|\bالملايين\b", text)
            arabic_thousand = re.search(r"بال[اآ]لاف|\bألف\b|\bال[اآ]لاف\b", text)
            arabic_sar = re.search(r"بالريال\s+السعودي|\bريال\b", text)

            if english_million or arabic_million:
                return {"unit_detected": "million_SAR", "applied_multiplier": 1_000_000}
            if english_thousand or arabic_thousand:
                return {"unit_detected": "thousand_SAR", "applied_multiplier": 1_000}
            if english_sar or arabic_sar:
                return {"unit_detected": "SAR", "applied_multiplier": 1}

            # Default when nothing explicit found
            return {"unit_detected": "unknown", "applied_multiplier": 1}
        except Exception as e:
            logger.warning(f"Unit detection error: {e}")
            return {"unit_detected": "unknown", "applied_multiplier": 1}

    def _find_page_for_value(self, pdf_path: str, search_value: str) -> Optional[int]:
        """Find the first page (1-based) that contains the given search value."""
        try:
            doc = fitz.open(pdf_path)
            for page_num in range(len(doc)):
                page = doc[page_num]
                if page.search_for(str(search_value)):
                    doc.close()
                    return page_num + 1
            doc.close()
            return None
        except Exception as e:
            logger.warning(f"Failed to locate page for value '{search_value}': {e}")
            return None

    def _detect_units_for_pdf(
        self,
        pdf_path: str,
        page_num: Optional[int] = None,
        search_value: Optional[str] = None,
    ) -> Dict[str, object]:
        """
        Detect units by reading text from a specific page if provided; otherwise, try to locate
        the page via the search_value. Falls back to first page if needed.
        """
        try:
            doc = fitz.open(pdf_path)
            target_page_index = None
            if page_num is not None and 1 <= page_num <= len(doc):
                target_page_index = page_num - 1
            elif search_value is not None:
                for p in range(len(doc)):
                    if doc[p].search_for(str(search_value)):
                        target_page_index = p
                        break

            # Fallback to first page if not found
            if target_page_index is None:
                target_page_index = 0

            page_text = doc[target_page_index].get_text()
            doc.close()
            return self._detect_units_from_text(page_text)
        except Exception as e:
            logger.warning(f"Failed to detect units for PDF: {e}")
            return {"unit_detected": "unknown", "applied_multiplier": 1}

    def extract_with_spire_pdf(self, pdf_path: str) -> Optional[Dict]:
        """Extract using Spire.PDF if available"""
        try:
            from spire.pdf import PdfDocument, PdfTableExtractor
        except ImportError:
            return None

        try:
            doc = PdfDocument()
            doc.LoadFromFile(pdf_path)
            extractor = PdfTableExtractor(doc)

            for page_index in range(doc.Pages.Count):
                tables = extractor.ExtractTable(page_index)
                if tables:
                    for table in tables:
                        # Look for retained earnings row
                        for row_index in range(table.GetRowCount()):
                            first_col = table.GetText(row_index, 0).strip().lower()
                            if first_col == RETAINED_EARNINGS_LABEL:
                                # Found retained earnings row, extract values
                                for year in self.target_years:
                                    for col_index in range(table.GetColumnCount()):
                                        cell_data = table.GetText(
                                            row_index, col_index
                                        ).strip()
                                        if str(year) in cell_data:
                                            # Look for numeric value in this column
                                            for row_idx in range(table.GetRowCount()):
                                                value_cell = table.GetText(
                                                    row_idx, col_index
                                                ).strip()
                                                if (
                                                    value_cell
                                                    and value_cell.replace(
                                                        ",", ""
                                                    ).isdigit()
                                                ):
                                                    numeric_value = float(
                                                        value_cell.replace(",", "")
                                                    )
                                                    if numeric_value >= 10000:
                                                        # Detect units on the same page
                                                        units = (
                                                            self._detect_units_for_pdf(
                                                                pdf_path,
                                                                page_num=page_index + 1,
                                                                search_value=value_cell,
                                                            )
                                                        )
                                                        scaled_value = (
                                                            numeric_value
                                                            * units[
                                                                "applied_multiplier"
                                                            ]
                                                        )
                                                        doc.Close()
                                                        return {
                                                            "success": True,
                                                            "value": value_cell,
                                                            "numeric_value": scaled_value,
                                                            "method": "spire_pdf",
                                                            "year": year,
                                                            "page": page_index + 1,
                                                            "unit_detected": units[
                                                                "unit_detected"
                                                            ],
                                                            "applied_multiplier": units[
                                                                "applied_multiplier"
                                                            ],
                                                        }
            doc.Close()
            return None
        except Exception as e:
            logger.error(f"Spire.PDF error: {e}")
            return None

    def extract_with_camelot(self, pdf_path: str) -> Optional[Dict]:
        """Extract using Camelot if available"""
        try:
            import camelot
        except ImportError:
            return None

        try:
            tables = camelot.read_pdf(pdf_path, flavor="stream")
            for table in tables:
                df = table.df
                # Look for retained earnings row
                for i, row in df.iterrows():
                    if RETAINED_EARNINGS_LABEL in str(row.iloc[0]).lower():
                        # Found retained earnings row, look for years
                        for year in self.target_years:
                            for col_idx, col_name in enumerate(df.columns):
                                if str(year) in str(col_name):
                                    # Look for numeric value in this column
                                    for row_idx in range(len(df)):
                                        value = df.iloc[row_idx, col_idx]
                                        if (
                                            value
                                            and str(value).replace(",", "").isdigit()
                                        ):
                                            numeric_value = float(
                                                str(value).replace(",", "")
                                            )
                                            if numeric_value >= 10000:
                                                # Try to find the page for the found value and detect units
                                                page_num = self._find_page_for_value(
                                                    pdf_path, str(value)
                                                )
                                                units = self._detect_units_for_pdf(
                                                    pdf_path,
                                                    page_num=page_num,
                                                    search_value=str(value),
                                                )
                                                scaled_value = (
                                                    numeric_value
                                                    * units["applied_multiplier"]
                                                )
                                                return {
                                                    "success": True,
                                                    "value": str(value),
                                                    "numeric_value": scaled_value,
                                                    "method": "camelot",
                                                    "year": year,
                                                    "page": page_num if page_num else 1,
                                                    "unit_detected": units[
                                                        "unit_detected"
                                                    ],
                                                    "applied_multiplier": units[
                                                        "applied_multiplier"
                                                    ],
                                                }
            return None
        except Exception as e:
            logger.error(f"Camelot error: {e}")
            return None

    def extract_with_regex(self, pdf_path: str) -> Optional[Dict]:
        """Simple regex extraction as fallback"""
        try:
            doc = fitz.open(pdf_path)
            text = ""
            for page in doc:
                text += page.get_text()
            doc.close()

            # Detect years first
            self.detect_years(text)
            if not self.target_years:
                return None

            # Look for retained earnings line
            lines = text.split("\n")
            for i, line in enumerate(lines):
                if RETAINED_EARNINGS_LABEL in line.lower():
                    # Look for numbers in nearby lines
                    for j in range(i + 1, min(i + 10, len(lines))):
                        next_line = lines[j]
                        numbers = re.findall(r"([\d,]+)", next_line)
                        for number in numbers:
                            clean_value = number.replace(",", "")
                            if clean_value.isdigit():
                                numeric_value = float(clean_value)
                                # Filter out years and small numbers
                                if (
                                    numeric_value >= 10000
                                    and numeric_value not in self.target_years
                                ):
                                    # Try to locate actual page for the number and detect units
                                    page_num = self._find_page_for_value(
                                        pdf_path, number
                                    )
                                    units = self._detect_units_for_pdf(
                                        pdf_path, page_num=page_num, search_value=number
                                    )
                                    scaled_value = (
                                        numeric_value * units["applied_multiplier"]
                                    )
                                    return {
                                        "success": True,
                                        "value": number,
                                        "numeric_value": scaled_value,
                                        "method": "regex",
                                        "year": self.most_recent_year,
                                        "page": page_num if page_num else 1,
                                        "unit_detected": units["unit_detected"],
                                        "applied_multiplier": units[
                                            "applied_multiplier"
                                        ],
                                    }
            return None
        except Exception as e:
            logger.error(f"Regex error: {e}")
            return None

    def extract_retained_earnings(self, pdf_path: str) -> Dict:
        """Main extraction method with fallback chain"""
        logger.info(f"Processing: {pdf_path}")

        # Try Spire.PDF first (most reliable)
        result = self.extract_with_spire_pdf(pdf_path)
        if result:
            return result

        # Try Camelot
        result = self.extract_with_camelot(pdf_path)
        if result:
            return result

        # Try regex as last resort
        result = self.extract_with_regex(pdf_path)
        if result:
            return result

        return {
            "success": False,
            "error": "No retained earnings found using any method",
        }


def get_company_symbol_from_filename(filename):
    """Extract company symbol from PDF filename"""
    return filename.split("_")[0]


def save_to_database(results):
    """Save results to SQLite database"""
    conn = sqlite3.connect("data/financial_analysis.db")
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS retained_earnings")
    cursor.execute("""
        CREATE TABLE retained_earnings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_symbol TEXT,
            pdf_filename TEXT,
            retained_earnings_value REAL,
            year INTEGER,
            method TEXT,
            extraction_date TIMESTAMP
        )
    """)

    for result in results:
        if result.get("success"):
            cursor.execute(
                """
                INSERT INTO retained_earnings 
                (company_symbol, pdf_filename, retained_earnings_value, year, method, extraction_date)
                VALUES (?, ?, ?, ?, ?, ?)
            """,
                (
                    result["company_symbol"],
                    result["pdf_filename"],
                    result.get("numeric_value"),
                    result.get("year"),
                    result.get("method"),
                    datetime.now(),
                ),
            )

    conn.commit()
    conn.close()


def main():
    pdf_dir = Path("data/pdfs")
    pdf_files = [f for f in pdf_dir.glob("*.pdf")]

    if not pdf_files:
        print("No PDF files found in data/pdfs/")
        return

    print(f"Found {len(pdf_files)} PDF files to process")

    extractor = RetainedEarningsExtractor()
    evidence_generator = EvidenceScreenshotGenerator()
    results = []
    successful_extractions = 0

    # Support graceful stop via flag file (ignored when finalizing after "Stop download")
    ignore_stop = os.environ.get("IGNORE_PDFS_STOP_FLAG", "").strip().lower() in (
        "1",
        "true",
        "yes",
    )
    stop_flag_file = os.environ.get(
        "STOP_FLAG_FILE", str(Path("data/runtime/stop_pdfs_pipeline.flag").resolve())
    )

    for i, pdf_file in enumerate(pdf_files, 1):
        try:
            if not ignore_stop and os.path.exists(stop_flag_file):
                print(
                    "🛑 Stop requested. Ending extraction loop early and saving partial results..."
                )
                break
        except Exception:
            # If any error reading stop flag, proceed safely
            pass
        print(f"\n[{i}/{len(pdf_files)}] Processing: {pdf_file.name}")

        company_symbol = get_company_symbol_from_filename(pdf_file.name)
        result = extractor.extract_retained_earnings(str(pdf_file))

        # Add metadata
        result["company_symbol"] = company_symbol
        result["pdf_filename"] = pdf_file.name

        if result["success"]:
            successful_extractions += 1
            print(f"  ✓ Found: {result['value']} (Year: {result['year']})")
            print(f"  ✓ Method: {result['method']}")

            # Generate evidence screenshot
            try:
                print("  📸 Generating evidence screenshot...")
                screenshot_path = evidence_generator.generate_highlight_screenshot(
                    str(pdf_file), result["value"], company_symbol
                )
                if screenshot_path:
                    print(f"  ✓ Evidence screenshot saved: {screenshot_path}")
                else:
                    print("  ⚠️ Failed to generate evidence screenshot")
            except Exception as e:
                print(f"  ⚠️ Error generating evidence screenshot: {e}")
        else:
            print(f"  ✗ Error: {result.get('error', 'Unknown error')}")

        results.append(result)

        # Persist partial results after each file so UI can finalize immediately on stop
        try:
            results_dir = Path("data/results")
            results_dir.mkdir(parents=True, exist_ok=True)
            output_file_tmp = results_dir / "retained_earnings_results.partial.json"
            with open(output_file_tmp, "w", encoding="utf-8") as f:
                json.dump(results, f, indent=2, ensure_ascii=False)
        except Exception:
            pass

    # Save results
    results_dir = Path("data/results")
    results_dir.mkdir(parents=True, exist_ok=True)

    output_file = results_dir / "retained_earnings_results.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    # Save to database
    save_to_database(results)

    # Print summary
    print(f"\n{'=' * 50}")
    print("EXTRACTION SUMMARY")
    print(f"{'=' * 50}")
    print(f"Total PDFs processed: {len(pdf_files)}")
    print(f"Successful extractions: {successful_extractions}")
    print(f"Success rate: {successful_extractions / len(pdf_files) * 100:.1f}%")
    print(f"Results saved to: {output_file}")
    print("Results also saved to database: data/financial_analysis.db")


if __name__ == "__main__":
    main()
