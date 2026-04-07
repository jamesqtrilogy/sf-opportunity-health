"""
enrich.py -- Runs HealthScoreEngine over raw opportunity records.

Appends health score fields to each record dict without mutating the originals.
"""

from __future__ import annotations
from datetime import date
from kpi_tools import HealthScoreEngine
from kpi_tools.engine import _days_since


def _days_remaining(renewal_date_str) -> int | None:
    """Positive = days until renewal; negative = days overdue."""
    days_ago = _days_since(renewal_date_str)
    if days_ago is None:
        return None
    return -days_ago   # invert: positive means future


def enrich(
    records: list[dict],
    redistribute_missing: bool = False,
) -> list[dict]:
    """
    Score each opportunity with HealthScoreEngine.

    Returns a new list of dicts -- original records are not mutated.
    Each dict has these fields appended:

        health_score          float   final score after overrides (0-100)
        raw_composite         float   pre-override composite
        health_band           str     "Healthy" | "Caution" | "At Risk"
        data_confidence       float   0.0-1.0
        overrides_applied     list[str]
        engagement_score      float   Domain 1 (0-100)
        renewal_score         float   Domain 2 (0-100)
        commercial_score      float   Domain 3 (0-100)
        risk_score            float   Domain 4 (0-100)
        days_to_renewal       int|None  positive=future, negative=overdue
    """
    engine = HealthScoreEngine(redistribute_missing=redistribute_missing)
    enriched = []

    for rec in records:
        result = engine.score(rec, account_name=rec.get("Account.Name", rec.get("Name", "")))
        flat = engine.score_to_dict(result)

        enriched_rec = dict(rec)   # shallow copy -- do not mutate original
        enriched_rec.update({
            "health_score":       flat["health_score"],
            "raw_composite":      flat["raw_composite"],
            "health_band":        flat["band"],
            "data_confidence":    flat["data_confidence"],
            "overrides_applied":  [o.rule for o in result.overrides_applied],
            "engagement_score":   round(result.domains["engagement"].score, 1),
            "renewal_score":      round(result.domains["renewal"].score, 1),
            "commercial_score":   round(result.domains["commercial"].score, 1),
            "risk_score":         round(result.domains["risk"].score, 1),
            "days_to_renewal":    _days_remaining(rec.get("Renewal_Date__c")),
        })
        enriched.append(enriched_rec)

    return enriched
