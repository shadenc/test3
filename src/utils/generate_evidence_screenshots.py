#!/usr/bin/env python3
"""
Generate Evidence Screenshots for Retained Earnings Extraction
Creates highlighted screenshots showing where values were found in PDFs
"""

import fitz
import json
from pathlib import Path
import logging
from typing import Optional, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def _safe_pdfs_path(pdf_filename: str) -> Optional[Path]:
    """Resolve PDF under project data/pdfs using basename only (blocks path traversal)."""
    root = Path(__file__).resolve().parent.parent.parent
    base = (root / "data" / "pdfs").resolve()
    name = Path(str(pdf_filename)).name
    if not name:
        return None
    resolved = (base / name).resolve()
    try:
        resolved.relative_to(base)
    except ValueError:
        return None
    return resolved


class EvidenceScreenshotGenerator:
    def __init__(self, output_dir: str = "output/screenshots"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def find_value_in_pdf(
        self, pdf_path: str, search_value: str
    ) -> Optional[Tuple[int, fitz.Rect]]:
        """
        Find the exact location of a value in a PDF
        Returns (page_number, rectangle) or None
        """
        try:
            doc = fitz.open(pdf_path)

            # Try multiple formats of the search value
            search_variants = [
                str(search_value),  # Original format
                str(search_value).replace(",", ""),  # Without commas
                str(search_value).replace(",", ","),  # With commas
            ]

            # Try to add formatted variants
            try:
                numeric_value = float(str(search_value).replace(",", ""))
                search_variants.extend(
                    [
                        f"{int(numeric_value):,}",  # Formatted with commas
                        f"{int(numeric_value)}",  # Integer format
                    ]
                )
            except ValueError:
                pass

            # Remove duplicates while preserving order
            seen = set()
            unique_variants = []
            for variant in search_variants:
                if variant not in seen:
                    seen.add(variant)
                    unique_variants.append(variant)

            for page_num in range(len(doc)):
                page = doc[page_num]

                # Try each variant
                for variant in unique_variants:
                    areas = page.search_for(variant)
                    if areas:
                        doc.close()
                        logger.info(
                            "Found match in %s page %s",
                            Path(pdf_path).name,
                            page_num + 1,
                        )
                        return (page_num, areas[0])  # Return first match

            doc.close()
            logger.warning(
                "Could not find search value in %s", Path(pdf_path).name
            )
            return None

        except Exception as e:
            logger.error("Error searching PDF %s: %s", Path(pdf_path).name, e)
            return None

    def generate_highlight_screenshot(
        self, pdf_path: str, search_value: str
    ) -> Optional[str]:
        """
        Generate a highlighted screenshot showing where the value was found
        Returns the path to the generated screenshot or None
        """
        try:
            # Find the value location
            result = self.find_value_in_pdf(pdf_path, search_value)
            if not result:
                logger.warning(
                    "Could not find value in %s", Path(pdf_path).name
                )
                return None
            page_num, rect = result
            # Open PDF and get the page
            doc = fitz.open(pdf_path)
            page = doc[page_num]
            # Add highlight annotation
            highlight = page.add_highlight_annot(rect)
            highlight.set_colors(stroke=(1, 1, 0))  # Yellow highlight
            highlight.set_opacity(0.5)
            # Create pixmap with higher DPI for better quality
            mat = fitz.Matrix(2.0, 2.0)  # 2x zoom for better quality
            pix = page.get_pixmap(matrix=mat, dpi=300)
            # Generate output filename using the PDF base name
            base_name = Path(pdf_path).stem
            output_filename = f"{base_name}_evidence.png"
            output_path = self.output_dir / output_filename
            # Save the screenshot
            pix.save(str(output_path))
            doc.close()
            logger.info(f"Generated evidence screenshot: {output_path}")
            return str(output_path)
        except Exception as e:
            logger.error(
                "Error generating screenshot for PDF %s: %s",
                Path(pdf_path).name,
                e,
            )
            return None

    def generate_page_screenshot(
        self, pdf_path: str, page_number: int, company_symbol: str
    ) -> Optional[str]:
        """
        Generate a screenshot of the specified page (no highlight).
        Returns the path to the generated screenshot or None.
        """
        try:
            doc = fitz.open(pdf_path)
            page = doc[page_number - 1]  # Convert 1-based to 0-based
            mat = fitz.Matrix(2.0, 2.0)  # 2x zoom for better quality
            pix = page.get_pixmap(matrix=mat, dpi=300)
            output_filename = f"{company_symbol}_evidence.png"
            output_path = self.output_dir / output_filename
            pix.save(str(output_path))
            doc.close()
            logger.info(f"Generated page screenshot: {output_path}")
            return str(output_path)
        except Exception as e:
            logger.error(
                "Error generating page screenshot for PDF %s: %s",
                Path(pdf_path).name,
                e,
            )
            return None

    def generate_all_evidence_screenshots(
        self, results_file: str = "data/results/retained_earnings_results.json"
    ):
        """
        Generate evidence screenshots for all successful extractions
        """
        try:
            results_path = Path(results_file)
            if not results_path.is_file():
                logger.warning(
                    "Evidence screenshots skipped: results file not found",
                )
                return []
            # Load results
            with open(results_path, "r", encoding="utf-8") as f:
                results = json.load(f)

            successful_extractions = [r for r in results if r["success"]]
            logger.info(
                f"Generating evidence screenshots for {len(successful_extractions)} successful extractions"
            )

            generated_screenshots = []

            for record_index, result in enumerate(successful_extractions):
                company_symbol = result["company_symbol"]
                pdf_filename = result["pdf_filename"]
                value = result["value"]

                pdf_resolved = _safe_pdfs_path(pdf_filename)
                if pdf_resolved is None:
                    logger.warning(
                        "Evidence skipped: invalid PDF reference (record_index=%s)",
                        record_index,
                    )
                    continue
                if not pdf_resolved.is_file():
                    logger.warning(
                        "Evidence skipped: PDF missing on disk (record_index=%s)",
                        record_index,
                    )
                    continue

                screenshot_path = self.generate_highlight_screenshot(
                    str(pdf_resolved), value
                )

                if screenshot_path:
                    generated_screenshots.append(
                        {
                            "company_symbol": company_symbol,
                            "value": value,
                            "screenshot_path": screenshot_path,
                            "pdf_filename": pdf_filename,
                        }
                    )

            # Save metadata about generated screenshots
            metadata_file = self.output_dir / "evidence_metadata.json"
            with open(metadata_file, "w", encoding="utf-8") as f:
                json.dump(generated_screenshots, f, indent=2, ensure_ascii=False)

            logger.info(f"Generated {len(generated_screenshots)} evidence screenshots")
            logger.info(f"Metadata saved to: {metadata_file}")

            return generated_screenshots

        except Exception as e:
            logger.error(f"Error generating evidence screenshots: {e}")
            return []


def main():
    """Generate evidence screenshots for all successful extractions"""
    generator = EvidenceScreenshotGenerator()
    screenshots = generator.generate_all_evidence_screenshots()

    print(f"\n{'=' * 50}")
    print("EVIDENCE SCREENSHOT GENERATION SUMMARY")
    print(f"{'=' * 50}")
    print(f"Generated screenshots: {len(screenshots)}")

    if screenshots:
        print("\nGenerated evidence for:")
        for screenshot in screenshots:
            print(
                f"  {screenshot['company_symbol']}: {screenshot['value']} -> {screenshot['screenshot_path']}"
            )

    print("\nScreenshots saved to: output/screenshots/")
    print("Metadata saved to: output/screenshots/evidence_metadata.json")


if __name__ == "__main__":
    main()
