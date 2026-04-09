"""Minimal tests so CI can publish Python coverage to SonarCloud."""

from src.calculators.calculate_reinvested_earnings import _find_statement


def test_find_statement_match():
    stmts = [{"type": "q1", "year": 2025, "value": 100}]
    found = _find_statement(stmts, "q1", 2025)
    assert found is not None
    assert found["value"] == 100


def test_find_statement_miss():
    assert _find_statement([], "q1", 2025) is None
