"""
Flask API package (Saudi Investment Analyzer).

- ``evidence_constants``: paths, messages, env (no Flask)
- ``evidence_helpers``: JSON helpers, screenshot globbing, CSRF-adjacent responses
- ``evidence_jobs``: schedulers, PDF/net-profit Playwright pipelines
- ``evidence_api``: ``create_app()`` factory and HTTP routes (imported by Gunicorn)
"""

from .evidence_api import create_app

__all__ = ["create_app"]
