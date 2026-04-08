"""
engine.py — Customer Health Score Engine (sf-opportunity-health edition)
=========================================================================
Computes a weighted-average health KPI (0–100) from Salesforce opportunity
records using four domains sourced entirely from Salesforce CRM:

    Engagement   (35%)  —  status, outcome, activity, follow-up discipline
    Renewal      (30%)  —  timing, pipeline stage, auto-renewal protection
    Commercial   (20%)  —  ARR movement, success tier, deal value
    Risk         (15%)  —  explicit risk flags and status signals

Field names are defined in FIELD_MAP and must match config/fields.yaml exactly.
Do not add fields from external systems (Kayako, NetSuite, Legal).
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta
from enum import Enum
from typing import Any, Optional


# ============================================================
# 1. CONFIGURATION — field names & weights
# ============================================================

# ---- Domain weights (must sum to 1.0) ----
WEIGHTS = {
    "engagement":  0.35,
    "renewal":     0.30,
    "commercial":  0.20,
    "risk":        0.15,
}

# ---- Salesforce opportunity field names ----
# Change these strings to match your actual Salesforce API field names.
# The scoring functions reference these constants so you only remap once.

FIELD_MAP = {
    # Engagement signals
    "opportunity_status":       "Opportunity_Status__c",
    "probable_outcome":         "Probable_Outcome__c",
    "churn_risks":              "Churn_Risks__c",
    "last_activity_date":       "LastActivityDate",
    "next_follow_up_date":      "Next_Follow_Up_Date__c",

    # Renewal signals
    "renewal_date":             "Renewal_Date__c",
    "stage_name":               "StageName",
    "auto_renewed_last_term":   "Auto_Renewed_Last_Term__c",
    "opportunity_term":         "Opportunity_Term__c",

    # Commercial signals
    "arr":                      "ARR__c",
    "current_success_level":    "Current_Success_Level__c",
    "high_value_opp":           "High_Value_Opp__c",

    # Risk signals
    "late_status":              "Late_Status__c",
    "win_type":                 "Win_Type__c",
    "opportunity_status_notes": "Opportunity_Status_Notes__c",
    "opportunity_report":       "Opportunity_Report__c",
}


# ============================================================
# 2. DATA STRUCTURES
# ============================================================

class Band(Enum):
    HEALTHY = "Healthy"
    CAUTION = "Caution"
    AT_RISK = "At Risk"


@dataclass
class SubScore:
    """Score for a single signal within a domain."""
    signal_name: str
    raw_value: Any
    score: float          # 0-100
    weight: float         # sub-weight within the domain
    weighted: float       # score * weight


@dataclass
class DomainScore:
    """Aggregated score for one of the four domains."""
    domain: str
    score: float                        # 0-100 (weighted sum of sub-scores)
    weight: float                       # domain weight (e.g. 0.35)
    contribution: float                 # score * weight -> added to composite
    signals: list[SubScore] = field(default_factory=list)
    data_present: bool = True           # False if all signals were missing


@dataclass
class Override:
    """Record of a veto / override rule that fired."""
    rule: str
    action: str           # "cap" or "floor" or "set"
    threshold: float
    reason: str


@dataclass
class HealthResult:
    """Complete output of the health-score engine for one opportunity."""
    account_name: str
    raw_composite: float                # before overrides
    final_score: float                  # after overrides & clamping
    band: Band
    domains: dict[str, DomainScore]     # keyed by domain name
    overrides_applied: list[Override] = field(default_factory=list)
    data_confidence: float = 1.0        # 0-1; reduced when data is missing
    scored_at: datetime = field(default_factory=datetime.utcnow)

    def summary(self) -> str:
        parts = [
            f"Account: {self.account_name}",
            f"Health Score: {self.final_score:.1f} / 100  [{self.band.value}]",
            f"  Engagement : {self.domains['engagement'].score:5.1f}  (x {WEIGHTS['engagement']:.0%} = {self.domains['engagement'].contribution:5.1f})",
            f"  Renewal    : {self.domains['renewal'].score:5.1f}  (x {WEIGHTS['renewal']:.0%} = {self.domains['renewal'].contribution:5.1f})",
            f"  Commercial : {self.domains['commercial'].score:5.1f}  (x {WEIGHTS['commercial']:.0%} = {self.domains['commercial'].contribution:5.1f})",
            f"  Risk       : {self.domains['risk'].score:5.1f}  (x {WEIGHTS['risk']:.0%} = {self.domains['risk'].contribution:5.1f})",
            f"  Raw composite: {self.raw_composite:.1f}",
            f"  Data confidence: {self.data_confidence:.0%}",
        ]
        if self.overrides_applied:
            parts.append("  Overrides:")
            for o in self.overrides_applied:
                parts.append(f"    -> {o.rule}: {o.action} at {o.threshold} -- {o.reason}")
        return "\n".join(parts)


# ============================================================
# 3. HELPER UTILITIES
# ============================================================

def _get(record: dict, field_key: str, default=None):
    """Safely pull a value from the opportunity record using FIELD_MAP."""
    sf_field = FIELD_MAP.get(field_key, field_key)
    val = record.get(sf_field, record.get(field_key, default))
    return default if val is None else val


def _clamp(value: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, value))


def _days_since(dt_value, today: date | None = None) -> int | None:
    """Return days between today and a date-like value. None if missing."""
    if dt_value is None:
        return None
    today = today or date.today()
    if isinstance(dt_value, datetime):
        dt_value = dt_value.date()
    elif isinstance(dt_value, str):
        for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"):
            try:
                dt_value = datetime.strptime(dt_value, fmt).date()
                break
            except ValueError:
                continue
        else:
            return None
    return (today - dt_value).days


def apply_time_decay(score: float, last_updated: Any,
                     decay_rate: float = 0.15, period_days: int = 90,
                     floor_pct: float = 0.50) -> float:
    """
    Decay a signal score based on staleness.
    Reduces by `decay_rate` for every `period_days` since last update,
    flooring at `floor_pct` of the original score.
    """
    days = _days_since(last_updated)
    if days is None or days <= 0:
        return score
    periods = days / period_days
    decay_multiplier = max(floor_pct, (1 - decay_rate) ** periods)
    return score * decay_multiplier


def _parse_report_sections(report_text: str) -> dict[str, str]:
    """Parse Opportunity_Report__c markdown into sections by ### headers."""
    if not report_text or not isinstance(report_text, str):
        return {}

    sections = {}
    current_section = None
    current_content = []

    for line in report_text.split('\n'):
        if line.strip().startswith('###'):
            if current_section:
                sections[current_section] = '\n'.join(current_content).strip()
            current_section = line.strip().lstrip('#').strip()
            current_content = []
        elif current_section:
            current_content.append(line)

    if current_section:
        sections[current_section] = '\n'.join(current_content).strip()

    return sections


def _count_bullet_items(text: str) -> int:
    """Count bullet points (-, *, •) in text."""
    if not text:
        return 0
    count = 0
    for line in text.split('\n'):
        stripped = line.strip()
        if stripped.startswith(('-', '*', '•')) and len(stripped) > 1:
            count += 1
    return count


def _parse_categorical_value(text: str, mapping: dict[str, int], default: int = 50) -> int:
    """Extract a categorical value from section text and map to score."""
    if not text:
        return default
    text_lower = text.lower().strip()
    for key, score in mapping.items():
        if key.lower() in text_lower:
            return score
    return default


def score_opportunity_report(report_text: str) -> tuple[float, dict]:
    """
    Score the Opportunity_Report__c markdown field.

    Returns (score 0-100, details dict with section breakdown).
    """
    sections = _parse_report_sections(report_text)

    if not sections:
        return 50.0, {"parsed": False, "reason": "empty or missing report"}

    details = {"parsed": True, "sections_found": list(sections.keys())}

    # Count positive items (higher = better)
    positive_count = _count_bullet_items(sections.get("Positive Signals", ""))
    activities_count = _count_bullet_items(sections.get("Opportunity Activities", ""))

    # Count negative items (higher = worse)
    negative_count = _count_bullet_items(sections.get("Negative Signals", ""))
    pain_count = _count_bullet_items(sections.get("Pain Points", ""))
    gaps_count = _count_bullet_items(sections.get("Renewal Process Gaps", ""))
    risks_count = _count_bullet_items(sections.get("Risks List", ""))

    details["positive_signals"] = positive_count
    details["activities"] = activities_count
    details["negative_signals"] = negative_count
    details["pain_points"] = pain_count
    details["renewal_gaps"] = gaps_count
    details["risks"] = risks_count

    # Parse categorical fields
    status_map = {"on track": 100, "warning": 45, "attention required": 0}
    outcome_map = {"likely to win": 100, "undetermined": 50, "likely to churn": 0}
    health_map = {"healthy": 100, "caution": 50, "at risk": 0}
    confidence_map = {"high": 100, "medium": 60, "low": 20}

    status_score = _parse_categorical_value(sections.get("Opportunity Status", ""), status_map)
    outcome_score = _parse_categorical_value(sections.get("Probable Outcome", ""), outcome_map)
    health_score = _parse_categorical_value(sections.get("Engagement Health", ""), health_map)
    confidence_score = _parse_categorical_value(
        sections.get("Information Quality and Confidence", ""), confidence_map
    )

    details["status_score"] = status_score
    details["outcome_score"] = outcome_score
    details["health_score"] = health_score
    details["confidence_score"] = confidence_score

    # Composite scoring:
    # - Categorical signals (40%): avg of status, outcome, health
    # - Positive balance (30%): positives vs negatives
    # - Confidence (15%): information quality
    # - Activity level (15%): engagement evidence

    categorical_avg = (status_score + outcome_score + health_score) / 3

    total_positive = positive_count + activities_count
    total_negative = negative_count + pain_count + gaps_count + risks_count

    if total_positive + total_negative == 0:
        balance_score = 50  # neutral if no items
    else:
        # Ratio of positive to total, scaled to 0-100
        balance_score = (total_positive / (total_positive + total_negative)) * 100

    # Activity score: 0 items = 30, 1-2 = 60, 3+ = 90
    if activities_count >= 3:
        activity_score = 90
    elif activities_count >= 1:
        activity_score = 60
    else:
        activity_score = 30

    composite = (
        categorical_avg * 0.40 +
        balance_score * 0.30 +
        confidence_score * 0.15 +
        activity_score * 0.15
    )

    details["composite_breakdown"] = {
        "categorical": round(categorical_avg, 1),
        "balance": round(balance_score, 1),
        "confidence": confidence_score,
        "activity": activity_score,
    }

    return _clamp(composite), details


# ============================================================
# 4. DOMAIN SCORERS
# ============================================================

def score_engagement(record: dict) -> DomainScore:
    """Score Engagement signals (35% of composite).

    Signals: opportunity_status, probable_outcome, activity_recency,
             follow_up_discipline.
    """
    signals: list[SubScore] = []

    # 4.1a -- Opportunity status (35%)
    status = _get(record, "opportunity_status")
    if status == "On Track":
        s = 100
    elif status == "Warning":
        s = 45
    elif status == "Attention Required":
        s = 0
    else:
        s = 50
    signals.append(SubScore("opportunity_status", status, s, 0.35, s * 0.35))

    # 4.1b -- Probable outcome (30%)
    outcome = _get(record, "probable_outcome")
    if outcome == "Likely to Win":
        s = 100
    elif outcome == "Undetermined":
        s = 50
    elif outcome == "Likely to Churn":
        s = 0
    else:
        s = 50
    signals.append(SubScore("probable_outcome", outcome, s, 0.30, s * 0.30))

    # 4.1c -- Activity recency (20%)
    days = _days_since(_get(record, "last_activity_date"))
    if days is None or days > 60:
        s = 0
    elif days <= 7:
        s = 100
    elif days <= 14:
        s = 80
    elif days <= 30:
        s = 50
    else:  # 31-60
        s = 20
    signals.append(SubScore("activity_recency", days, s, 0.20, s * 0.20))

    # 4.1d -- Follow-up discipline (15%)
    follow_up = _get(record, "next_follow_up_date")
    if follow_up is None:
        s = 30
    else:
        days_ago = _days_since(follow_up)
        if days_ago is None:
            s = 30
        elif days_ago < 0:
            # Negative = date is in the future -- good
            s = 100
        else:
            # Zero or positive = today or overdue
            s = 10
    signals.append(SubScore("follow_up_discipline", follow_up, s, 0.15, s * 0.15))

    domain_score = sum(sig.weighted for sig in signals)
    return DomainScore(
        domain="engagement",
        score=_clamp(domain_score),
        weight=WEIGHTS["engagement"],
        contribution=_clamp(domain_score) * WEIGHTS["engagement"],
        signals=signals,
        data_present=True,
    )


def score_renewal(record: dict) -> DomainScore:
    """Score Renewal signals (30% of composite).

    Signals: days_to_renewal, pipeline_stage, auto_renewed_history.
    """
    signals: list[SubScore] = []

    # 4.2a -- Days to renewal (45%)
    renewal_raw = _get(record, "renewal_date")
    days_ago = _days_since(renewal_raw)  # positive = past (overdue)
    if days_ago is None:
        s = 50
        days_to_renewal_val = None
    else:
        days_to_renewal_val = -days_ago  # positive = future
        if days_to_renewal_val < 0:
            s = 0   # overdue / past
        elif days_to_renewal_val <= 7:
            s = 5
        elif days_to_renewal_val <= 30:
            s = 20
        elif days_to_renewal_val <= 90:
            s = 50
        elif days_to_renewal_val <= 180:
            s = 80
        else:
            s = 100
    signals.append(SubScore("days_to_renewal", days_to_renewal_val, s, 0.45, s * 0.45))

    # 4.2b -- Pipeline stage (35%)
    stage = _get(record, "stage_name")
    stage_scores = {
        "Closed Won":     100,
        "Finalizing":      90,
        "Quote Follow-Up": 65,
        "Proposal":        50,
        "Engaged":         35,
        "Outreached":      20,
        "Closed Lost":      0,
    }
    s = stage_scores.get(stage, 40) if stage else 40
    signals.append(SubScore("pipeline_stage", stage, s, 0.35, s * 0.35))

    # 4.2c -- Auto renewed last term (20%)
    auto_history = _get(record, "auto_renewed_last_term")
    if auto_history is True:
        s = 100
    elif auto_history is False:
        s = 40
    else:
        s = 60
    signals.append(SubScore("auto_renewed_history", auto_history, s, 0.20, s * 0.20))

    domain_score = sum(sig.weighted for sig in signals)
    return DomainScore(
        domain="renewal",
        score=_clamp(domain_score),
        weight=WEIGHTS["renewal"],
        contribution=_clamp(domain_score) * WEIGHTS["renewal"],
        signals=signals,
        data_present=True,
    )


def score_commercial(record: dict) -> DomainScore:
    """Score Commercial signals (20% of composite).

    Signals: high_value_flag.
    """
    signals: list[SubScore] = []

    # 4.3a -- High value flag (100%)
    hv = _get(record, "high_value_opp")
    if hv is True:
        s = 100
    else:
        s = 60   # False and null both score 60
    signals.append(SubScore("high_value_flag", hv, s, 1.00, s * 1.00))

    domain_score = sum(sig.weighted for sig in signals)
    return DomainScore(
        domain="commercial",
        score=_clamp(domain_score),
        weight=WEIGHTS["commercial"],
        contribution=_clamp(domain_score) * WEIGHTS["commercial"],
        signals=signals,
        data_present=True,
    )


def score_risk(record: dict) -> DomainScore:
    """Score Risk signals (15% of composite).

    Signals: late_payment_status, commitment_signal, opportunity_report.
    (Churn_Risks__c is retained for display only — not scored.)
    """
    signals: list[SubScore] = []

    # 4.4a -- Late payment status (35%)
    late = _get(record, "late_status")
    if late is None or str(late).strip() == "":
        s = 100
    else:
        s = 0
    signals.append(SubScore("late_payment_status", late, s, 0.35, s * 0.35))

    # 4.4b -- Commitment signal (30%)
    win_type = _get(record, "win_type")
    if win_type is None:
        s = 40
    elif str(win_type).strip() == "Quote Signed":
        s = 100
    else:
        s = 60
    signals.append(SubScore("commitment_signal", win_type, s, 0.30, s * 0.30))

    # 4.4c -- Opportunity report analysis (35%)
    report_text = _get(record, "opportunity_report")
    report_score, _ = score_opportunity_report(report_text)
    signals.append(SubScore("opportunity_report", report_text is not None, report_score, 0.35, report_score * 0.35))

    domain_score = sum(sig.weighted for sig in signals)
    return DomainScore(
        domain="risk",
        score=_clamp(domain_score),
        weight=WEIGHTS["risk"],
        contribution=_clamp(domain_score) * WEIGHTS["risk"],
        signals=signals,
        data_present=True,
    )


# ============================================================
# 5. OVERRIDE / VETO RULES
# ============================================================

def evaluate_overrides(record: dict, raw_score: float) -> tuple[float, list[Override]]:
    """
    Apply hard cap / floor / set rules that override the computed composite.
    Returns (adjusted_score, list_of_overrides_that_fired).
    Rules are evaluated in order; Rule 1 short-circuits on match.
    """
    score = raw_score
    overrides: list[Override] = []

    # Rule 1 -- Closed Lost -> set to 5 (short-circuit)
    if _get(record, "stage_name") == "Closed Lost":
        overrides.append(Override(
            rule="Closed Lost",
            action="set", threshold=5,
            reason="Opportunity lost; score reflects reality",
        ))
        return 5.0, overrides

    # Rule 2 -- Renewal overdue -> cap at 30
    renewal_raw = _get(record, "renewal_date")
    days_ago = _days_since(renewal_raw)  # positive = date is in the past
    if days_ago is not None and days_ago > 0:
        overrides.append(Override(
            rule="Renewal overdue",
            action="cap", threshold=30,
            reason="Renewal date has passed with no closed deal",
        ))
        score = min(score, 30)

    # Rule 3 -- Attention Required -> cap at 35
    if _get(record, "opportunity_status") == "Attention Required":
        overrides.append(Override(
            rule="Attention Required status",
            action="cap", threshold=35,
            reason="Account flagged as Attention Required -- risk overrides positive signals",
        ))
        score = min(score, 35)

    # Rule 5 -- Late status active -> cap at 45
    late = _get(record, "late_status")
    if late is not None and str(late).strip() != "":
        overrides.append(Override(
            rule="Late status active",
            action="cap", threshold=45,
            reason="Late payment status is an existential retention risk",
        ))
        score = min(score, 45)

    return score, overrides


# ============================================================
# 6. DATA CONFIDENCE
# ============================================================

def compute_data_confidence(record: dict) -> float:
    """
    Return a 0.0-1.0 confidence factor based on how many signal fields
    are actually populated in the record.  Lets consumers know when a
    score is based on thin data.
    """
    total = 0
    present = 0
    for key, sf_field in FIELD_MAP.items():
        total += 1
        val = record.get(sf_field, record.get(key))
        if val is not None and val != "" and val != 0:
            present += 1
    return round(present / total, 2) if total > 0 else 0.0


# ============================================================
# 7. HEALTH SCORE ENGINE
# ============================================================

class HealthScoreEngine:
    """
    Main entry point.  Instantiate once, then call `.score(record)` for
    each Salesforce opportunity dict.

    Options:
        neutral_fill (float): Sub-score to use when a domain has no data.
                              Default 50 (neutral).
        redistribute_missing (bool): If True, redistribute weight from
                              entirely-missing domains proportionally.
    """

    def __init__(self, neutral_fill: float = 50.0,
                 redistribute_missing: bool = False):
        self.neutral_fill = neutral_fill
        self.redistribute = redistribute_missing

    def score(self, record: dict, account_name: str = "") -> HealthResult:
        """Score a single opportunity record and return a HealthResult."""
        if not account_name:
            account_name = (
                record.get("Account.Name", "")
                or record.get("Account_Name", "")
                or record.get("Name", "")
                or record.get("AccountName", "Unknown")
            )

        # Score each domain
        domains: dict[str, DomainScore] = {
            "engagement":  score_engagement(record),
            "renewal":     score_renewal(record),
            "commercial":  score_commercial(record),
            "risk":        score_risk(record),
        }

        # Handle missing domains
        effective_weights = dict(WEIGHTS)
        missing_weight = 0.0
        for name, ds in domains.items():
            all_missing = all(
                sig.raw_value is None or sig.raw_value == ""
                for sig in ds.signals
            )
            if all_missing:
                ds.data_present = False
                if self.redistribute:
                    missing_weight += effective_weights[name]
                    effective_weights[name] = 0.0
                else:
                    ds.score = self.neutral_fill

        # Redistribute missing weight
        if self.redistribute and missing_weight > 0:
            present_total = sum(
                w for n, w in effective_weights.items()
                if domains[n].data_present
            )
            if present_total > 0:
                for n in effective_weights:
                    if domains[n].data_present:
                        effective_weights[n] *= (1 + missing_weight / present_total)

        # Recalculate contributions with effective weights
        for name, ds in domains.items():
            ds.weight = effective_weights[name]
            ds.contribution = ds.score * ds.weight

        # Composite
        raw_composite = _clamp(sum(ds.contribution for ds in domains.values()))

        # Overrides
        final_score, overrides = evaluate_overrides(record, raw_composite)
        final_score = _clamp(final_score)

        # Band
        if final_score >= 80:
            band = Band.HEALTHY
        elif final_score >= 50:
            band = Band.CAUTION
        else:
            band = Band.AT_RISK

        # Confidence
        confidence = compute_data_confidence(record)

        return HealthResult(
            account_name=account_name,
            raw_composite=raw_composite,
            final_score=final_score,
            band=band,
            domains=domains,
            overrides_applied=overrides,
            data_confidence=confidence,
        )

    def score_batch(self, records: list[dict],
                    name_field: str = "Account.Name") -> list[HealthResult]:
        """Score a list of opportunity records."""
        return [
            self.score(rec, account_name=rec.get(name_field, ""))
            for rec in records
        ]

    def score_to_dict(self, result: HealthResult) -> dict:
        """Flatten a HealthResult into a dict suitable for CSV / DataFrame."""
        row = {
            "account_name": result.account_name,
            "health_score": round(result.final_score, 1),
            "raw_composite": round(result.raw_composite, 1),
            "band": result.band.value,
            "data_confidence": result.data_confidence,
            "overrides": "; ".join(o.rule for o in result.overrides_applied) or "None",
            "scored_at": result.scored_at.isoformat(),
        }
        for domain_name, ds in result.domains.items():
            row[f"{domain_name}_score"] = round(ds.score, 1)
            row[f"{domain_name}_contribution"] = round(ds.contribution, 1)
            row[f"{domain_name}_data_present"] = ds.data_present
        return row
