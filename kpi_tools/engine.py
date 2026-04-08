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
    "last_portal_activity":     "Last_Self_Serve_Portal_Activity__c",

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
    "account_report":           "Account.Account_Report__c",
    "support_tickets_summary":  "Account.Support_Tickets_Summary__c",
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
    """Parse Opportunity_Report__c or Account_Report__c markdown into sections.

    Handles both standard headers (### Section Name) and inline value headers
    (### Status: Warning, ### Likely Outcome: Churn).
    """
    if not report_text or not isinstance(report_text, str):
        return {}

    sections = {}
    current_section = None
    current_content = []
    inline_value_section = False  # Track if current section had inline value

    for line in report_text.split('\n'):
        stripped = line.strip()
        if stripped.startswith('###'):
            # Save previous section (only if it wasn't an inline value section)
            if current_section and not inline_value_section:
                content = '\n'.join(current_content).strip()
                if content:  # Only overwrite if there's actual content
                    sections[current_section] = content

            header = stripped.lstrip('#').strip()

            # Handle inline value headers like "### Status: ⚠️ Warning" or "### Likely Outcome: Churn"
            if ': ' in header and not header.endswith(':'):
                key, value = header.split(': ', 1)
                sections[key] = value  # Store inline value directly
                current_section = key
                current_content = []
                inline_value_section = True
            else:
                current_section = header.rstrip(':')  # Remove trailing colon if present
                current_content = []
                inline_value_section = False
        elif current_section:
            current_content.append(line)

    # Save final section
    if current_section and not inline_value_section:
        content = '\n'.join(current_content).strip()
        if content:
            sections[current_section] = content

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


def _count_table_rows(text: str) -> int:
    """Count table rows (lines starting with |) excluding header/separator."""
    if not text:
        return 0
    count = 0
    for line in text.split('\n'):
        stripped = line.strip()
        if stripped.startswith('|') and not stripped.startswith('|--') and '---' not in stripped:
            # Skip header row (usually first row with column names)
            if count > 0 or '**' in stripped or 'Name' not in stripped:
                count += 1
    return max(0, count - 1)  # Subtract header row


def score_opportunity_report(report_text: str) -> tuple[float, dict]:
    """
    Score the Opportunity_Report__c or Account.Account_Report__c markdown field.

    Parses all major sections from Account Reports:
    - Account Status / Status (categorical: On Track, Warning, Attention Required)
    - Likely Outcome (categorical: Renewal/Likely to Win, Undetermined, Churn)
    - Positive Signals (bullet count)
    - Negative Signals (bullet count)
    - Pain Points (bullet count, categorized by type)
    - Churn Risks (bullet count)
    - Account Activities (engagement evidence)
    - Key Individuals (contact count)
    - Recommended Actions (action items count)
    - Information Quality and Confidence (data quality)
    - Why Does the Customer Use Our Product Today (product stickiness)
    - Account Summary (overview presence)
    - Story We Want to Be Able to Tell (call preparation)

    Returns (score 0-100, details dict with section breakdown).
    """
    sections = _parse_report_sections(report_text)

    if not sections:
        return 50.0, {"parsed": False, "reason": "empty or missing report"}

    details = {"parsed": True, "sections_found": list(sections.keys())}

    # === COUNT POSITIVE INDICATORS ===
    positive_count = _count_bullet_items(sections.get("Positive Signals", ""))

    # Account/Opportunity Activities - engagement evidence
    activities_text = sections.get("Opportunity Activities", "") or sections.get("Account Activities", "")
    activities_count = _count_bullet_items(activities_text)

    # Key Individuals - more contacts = better relationship mapping
    key_individuals = sections.get("Key Individuals", "")
    contacts_count = _count_table_rows(key_individuals) or _count_bullet_items(key_individuals)

    # Product stickiness - presence indicates clear value proposition
    has_product_usage = bool(sections.get("Why Does the Customer Use Our Product Today", "").strip())

    # Call preparation - indicates proactive engagement planning
    has_call_story = bool(sections.get("Story We Want to Be Able to Tell on the Next Customer Call", "").strip())

    # Account Summary presence
    has_summary = bool(sections.get("Account Summary", "").strip())

    # === COUNT NEGATIVE INDICATORS ===
    negative_count = _count_bullet_items(sections.get("Negative Signals", ""))

    # Pain Points - parse by category (Support Issues, Product Issues, etc.)
    pain_text = sections.get("Pain Points", "")
    pain_count = _count_bullet_items(pain_text)

    # Renewal Process Gaps
    gaps_count = _count_bullet_items(sections.get("Renewal Process Gaps", ""))

    # Churn Risks / Risks List
    risks_text = sections.get("Risks List", "") or sections.get("Churn Risks", "")
    risks_count = _count_bullet_items(risks_text)

    # Recommended Actions - more actions = more work needed (neutral/slight negative)
    actions_count = _count_bullet_items(sections.get("Recommended Actions", ""))

    # Store counts in details
    details["positive_signals"] = positive_count
    details["activities"] = activities_count
    details["contacts"] = contacts_count
    details["has_product_usage"] = has_product_usage
    details["has_call_story"] = has_call_story
    details["has_summary"] = has_summary
    details["negative_signals"] = negative_count
    details["pain_points"] = pain_count
    details["renewal_gaps"] = gaps_count
    details["risks"] = risks_count
    details["recommended_actions"] = actions_count

    # === PARSE CATEGORICAL FIELDS ===
    # Status values may include emojis (e.g., "⚠️ Warning", "🔴 Attention Required")
    status_map = {
        "on track": 100, "✅": 100,
        "warning": 45, "⚠️": 45,
        "attention required": 0, "🔴": 0,
    }
    outcome_map = {
        "likely to win": 100, "renewal": 100,
        "undetermined": 50,
        "likely to churn": 0, "churn": 0,
    }
    health_map = {"healthy": 100, "caution": 50, "at risk": 0}
    confidence_map = {"high": 100, "confident": 100, "good": 80, "medium": 60, "low": 20}

    # Status - check multiple possible section names and inline format
    status_text = (
        sections.get("Status", "") or
        sections.get("Account Status", "") or
        sections.get("Opportunity Status", "")
    )
    # Also check Explanation section for status context
    explanation_text = sections.get("Explanation", "")
    if not status_text and explanation_text:
        status_text = explanation_text
    status_score = _parse_categorical_value(status_text, status_map)

    # Outcome - check multiple formats
    outcome_text = (
        sections.get("Likely Outcome", "") or
        sections.get("Probable Outcome", "")
    )
    # Also parse Likely Outcome Explanation for additional context
    outcome_explanation = sections.get("Likely Outcome Explanation", "")
    if outcome_explanation and not outcome_text:
        outcome_text = outcome_explanation
    outcome_score = _parse_categorical_value(outcome_text, outcome_map)

    # Engagement Health
    health_score = _parse_categorical_value(sections.get("Engagement Health", ""), health_map)

    # Information Quality and Confidence
    confidence_text = sections.get("Information Quality and Confidence", "")
    confidence_score = _parse_categorical_value(confidence_text, confidence_map)

    details["status_score"] = status_score
    details["outcome_score"] = outcome_score
    details["health_score"] = health_score
    details["confidence_score"] = confidence_score

    # === COMPOSITE SCORING ===
    # Weights:
    # - Categorical signals (35%): status, outcome, health average
    # - Positive/Negative balance (25%): ratio of positive to total items
    # - Confidence (15%): information quality
    # - Engagement depth (15%): activities + contacts
    # - Preparedness (10%): product usage docs + call story

    categorical_avg = (status_score + outcome_score + health_score) / 3

    # Balance score: positive items vs negative items
    total_positive = positive_count + activities_count
    total_negative = negative_count + pain_count + gaps_count + risks_count

    if total_positive + total_negative == 0:
        balance_score = 50
    else:
        balance_score = (total_positive / (total_positive + total_negative)) * 100

    # Engagement depth: activities and contact coverage
    if activities_count >= 3 and contacts_count >= 3:
        engagement_score = 100
    elif activities_count >= 2 or contacts_count >= 3:
        engagement_score = 75
    elif activities_count >= 1 or contacts_count >= 1:
        engagement_score = 50
    else:
        engagement_score = 25

    # Preparedness: documentation quality
    preparedness_score = 0
    if has_product_usage:
        preparedness_score += 50
    if has_call_story:
        preparedness_score += 30
    if has_summary:
        preparedness_score += 20

    composite = (
        categorical_avg * 0.35 +
        balance_score * 0.25 +
        confidence_score * 0.15 +
        engagement_score * 0.15 +
        preparedness_score * 0.10
    )

    details["composite_breakdown"] = {
        "categorical": round(categorical_avg, 1),
        "balance": round(balance_score, 1),
        "confidence": confidence_score,
        "engagement": engagement_score,
        "preparedness": preparedness_score,
    }

    return _clamp(composite), details


def score_support_tickets(summary_text: str) -> tuple[float, dict]:
    """
    Score the Account.Support_Tickets_Summary__c field.

    Parses support ticket information to assess support health:
    - Open tickets count (fewer = better)
    - P1/Critical tickets (any = bad)
    - Resolution times
    - Recent ticket volume
    - Customer sentiment indicators

    Returns (score 0-100, details dict).
    """
    if not summary_text or not isinstance(summary_text, str):
        return 70.0, {"parsed": False, "reason": "empty or missing support summary"}

    text_lower = summary_text.lower()
    details = {"parsed": True}

    # === PARSE KEY METRICS ===

    # Count open tickets (look for patterns like "X open", "open: X", "X active")
    import re
    open_match = re.search(r'(\d+)\s*(?:open|active|pending)', text_lower)
    open_tickets = int(open_match.group(1)) if open_match else 0
    details["open_tickets"] = open_tickets

    # Check for P1/Critical/Urgent tickets
    has_critical = any(term in text_lower for term in [
        'p1', 'critical', 'urgent', 'severity 1', 'sev1', 'high priority',
        'escalat', 'outage', 'down', 'blocker'
    ])
    details["has_critical"] = has_critical

    # Check for P2/High priority
    has_high = any(term in text_lower for term in [
        'p2', 'high', 'severity 2', 'sev2'
    ]) and not has_critical
    details["has_high_priority"] = has_high

    # Look for resolution time indicators
    slow_resolution = any(term in text_lower for term in [
        'overdue', 'sla breach', 'sla miss', 'delayed', 'waiting',
        'pending for', 'no response', 'unresolved'
    ])
    details["slow_resolution"] = slow_resolution

    # Look for positive indicators
    positive_indicators = sum([
        'all resolved' in text_lower or 'no open' in text_lower or 'zero open' in text_lower,
        'satisfied' in text_lower or 'positive feedback' in text_lower,
        'quick resolution' in text_lower or 'fast response' in text_lower,
        'no tickets' in text_lower or 'no active' in text_lower,
    ])
    details["positive_indicators"] = positive_indicators

    # Look for negative sentiment
    negative_indicators = sum([
        'frustrated' in text_lower or 'unhappy' in text_lower or 'angry' in text_lower,
        'complaint' in text_lower or 'escalat' in text_lower,
        'repeat' in text_lower or 'recurring' in text_lower,
        'dissatisfied' in text_lower or 'poor' in text_lower,
    ])
    details["negative_indicators"] = negative_indicators

    # Count total tickets mentioned
    total_match = re.search(r'(\d+)\s*(?:total|tickets|cases)', text_lower)
    total_tickets = int(total_match.group(1)) if total_match else None
    details["total_tickets"] = total_tickets

    # === CALCULATE SCORE ===

    # Start at 80 (healthy baseline)
    score = 80.0

    # Deduct for open tickets
    if open_tickets == 0:
        score += 10  # Bonus for no open tickets
    elif open_tickets <= 2:
        score -= 5
    elif open_tickets <= 5:
        score -= 15
    elif open_tickets <= 10:
        score -= 25
    else:
        score -= 40  # Many open tickets

    # Critical tickets are severe
    if has_critical:
        score -= 30

    # High priority tickets
    if has_high:
        score -= 15

    # Slow resolution
    if slow_resolution:
        score -= 15

    # Sentiment adjustments
    score += positive_indicators * 5
    score -= negative_indicators * 10

    details["calculated_score"] = round(score, 1)

    return _clamp(score), details


# ============================================================
# 4. DOMAIN SCORERS
# ============================================================

def score_engagement(record: dict) -> DomainScore:
    """Score Engagement signals (35% of composite).

    Signals: opportunity_status, probable_outcome, activity_recency,
             follow_up_discipline, portal_activity.
    """
    signals: list[SubScore] = []

    # 4.1a -- Opportunity status (30%)
    status = _get(record, "opportunity_status")
    if status == "On Track":
        s = 100
    elif status == "Warning":
        s = 45
    elif status == "Attention Required":
        s = 0
    else:
        s = 50
    signals.append(SubScore("opportunity_status", status, s, 0.30, s * 0.30))

    # 4.1b -- Probable outcome (25%)
    outcome = _get(record, "probable_outcome")
    if outcome == "Likely to Win":
        s = 100
    elif outcome == "Undetermined":
        s = 50
    elif outcome == "Likely to Churn":
        s = 0
    else:
        s = 50
    signals.append(SubScore("probable_outcome", outcome, s, 0.25, s * 0.25))

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

    # 4.1e -- Self-serve portal activity (10%) - positive signal
    portal_days = _days_since(_get(record, "last_portal_activity"))
    if portal_days is None:
        s = 40  # No data = neutral-low
    elif portal_days <= 7:
        s = 100  # Active in last week
    elif portal_days <= 30:
        s = 80   # Active in last month
    elif portal_days <= 90:
        s = 60   # Active in last quarter
    else:
        s = 30   # Stale
    signals.append(SubScore("portal_activity", portal_days, s, 0.10, s * 0.10))

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

    Signals: late_payment_status, commitment_signal, report_analysis, support_tickets.
    (Churn_Risks__c is retained for display only — not scored.)
    """
    signals: list[SubScore] = []

    # 4.4a -- Late payment status (25%)
    late = _get(record, "late_status")
    if late is None or str(late).strip() == "":
        s = 100
    else:
        s = 0
    signals.append(SubScore("late_payment_status", late, s, 0.25, s * 0.25))

    # 4.4b -- Commitment signal (20%)
    win_type = _get(record, "win_type")
    if win_type is None:
        s = 40
    elif str(win_type).strip() == "Quote Signed":
        s = 100
    else:
        s = 60
    signals.append(SubScore("commitment_signal", win_type, s, 0.20, s * 0.20))

    # 4.4c -- Opportunity/Account report analysis (30%)
    report_text = _get(record, "opportunity_report")
    if not report_text:
        account = record.get("Account", {})
        if isinstance(account, dict):
            report_text = account.get("Account_Report__c")
    report_score, _ = score_opportunity_report(report_text)
    has_report = report_text is not None and len(str(report_text).strip()) > 0
    signals.append(SubScore("report_analysis", has_report, report_score, 0.30, report_score * 0.30))

    # 4.4d -- Support tickets analysis (25%)
    support_text = None
    account = record.get("Account", {})
    if isinstance(account, dict):
        support_text = account.get("Support_Tickets_Summary__c")
    support_score, _ = score_support_tickets(support_text)
    has_support = support_text is not None and len(str(support_text).strip()) > 0
    signals.append(SubScore("support_tickets", has_support, support_score, 0.25, support_score * 0.25))

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
