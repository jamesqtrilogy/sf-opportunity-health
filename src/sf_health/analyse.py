"""
analyse.py -- Aggregate KPIs from enriched opportunity records.

Input  : list of dicts from enrich.py (each has health_score, domain scores,
         days_to_renewal, and all confirmed Salesforce fields)
Output : single KPI dict passed to report.py and printed by the CLI
"""

from __future__ import annotations
from collections import defaultdict
from datetime import date


def _arr(rec: dict) -> float:
    return float(rec.get("ARR__c") or 0)


def _band_buckets() -> dict:
    return {"Healthy": {"arr": 0.0, "count": 0},
            "Caution":  {"arr": 0.0, "count": 0},
            "At Risk":  {"arr": 0.0, "count": 0}}


def analyse(opportunities: list[dict]) -> dict:
    """Return a KPI dict computed from the enriched opportunity list."""
    if not opportunities:
        return {"total_count": 0, "total_arr": 0.0}

    total_count = len(opportunities)
    total_arr   = sum(_arr(r) for r in opportunities)

    # -- Health bands -----------------------------------------------------------
    band_dist = _band_buckets()
    for r in opportunities:
        b = r.get("health_band", "Caution")
        band_dist[b]["arr"]   += _arr(r)
        band_dist[b]["count"] += 1

    at_risk_arr   = band_dist["At Risk"]["arr"]
    at_risk_count = band_dist["At Risk"]["count"]

    avg_health_score    = sum(r.get("health_score",   0) for r in opportunities) / total_count
    avg_data_confidence = sum(r.get("data_confidence", 0) for r in opportunities) / total_count

    # -- Domain averages --------------------------------------------------------
    avg_domain_scores = {
        "engagement": sum(r.get("engagement_score", 0) for r in opportunities) / total_count,
        "renewal":    sum(r.get("renewal_score",    0) for r in opportunities) / total_count,
        "commercial": sum(r.get("commercial_score", 0) for r in opportunities) / total_count,
        "risk":       sum(r.get("risk_score",       0) for r in opportunities) / total_count,
    }

    # -- Override summary -------------------------------------------------------
    override_summary: dict[str, int] = defaultdict(int)
    for r in opportunities:
        for rule in r.get("overrides_applied", []):
            override_summary[rule] += 1

    # -- Outcome KPIs (Probable_Outcome__c) -------------------------------------
    outcome_kpis: dict[str, dict] = defaultdict(lambda: {"arr": 0.0, "count": 0})
    for r in opportunities:
        o = (r.get("Probable_Outcome__c") or "Undetermined").strip()
        outcome_kpis[o]["arr"]   += _arr(r)
        outcome_kpis[o]["count"] += 1

    likely_win_arr     = outcome_kpis.get("Likely to Win",   {}).get("arr", 0.0)
    likely_win_count   = outcome_kpis.get("Likely to Win",   {}).get("count", 0)
    likely_churn_arr   = outcome_kpis.get("Likely to Churn", {}).get("arr", 0.0)
    likely_churn_count = outcome_kpis.get("Likely to Churn", {}).get("count", 0)
    undetermined_arr   = outcome_kpis.get("Undetermined",    {}).get("arr", 0.0)
    undetermined_count = outcome_kpis.get("Undetermined",    {}).get("count", 0)

    # -- Status KPIs (Opportunity_Status__c) ------------------------------------
    status_kpis: dict[str, dict] = defaultdict(lambda: {"arr": 0.0, "count": 0})
    for r in opportunities:
        s = (r.get("Opportunity_Status__c") or "Unknown").strip()
        status_kpis[s]["arr"]   += _arr(r)
        status_kpis[s]["count"] += 1

    # -- ARR groupings ----------------------------------------------------------
    arr_by_product: dict[str, float] = defaultdict(float)
    arr_by_owner:   dict[str, float] = defaultdict(float)
    arr_by_stage:   dict[str, float] = defaultdict(float)

    for r in opportunities:
        arr_by_product[r.get("Product__c") or "Unknown"]  += _arr(r)
        arr_by_owner[r.get("Owner.Name")   or "Unknown"]  += _arr(r)
        arr_by_stage[r.get("StageName")    or "Unknown"]  += _arr(r)

    # Top 10 by ARR
    arr_by_owner = dict(sorted(arr_by_owner.items(), key=lambda x: x[1], reverse=True)[:10])

    # -- Renewal timeline -------------------------------------------------------
    def renewal_bucket(days) -> str:
        if days is None: return "Unknown"
        if days < 0:     return "Overdue"
        if days <= 30:   return "0-30d"
        if days <= 90:   return "31-90d"
        if days <= 180:  return "91-180d"
        return "180d+"

    renewal_timeline: dict[str, dict] = defaultdict(lambda: {"arr": 0.0, "count": 0})
    for r in opportunities:
        bucket = renewal_bucket(r.get("days_to_renewal"))
        renewal_timeline[bucket]["arr"]   += _arr(r)
        renewal_timeline[bucket]["count"] += 1

    # -- Engagement flags -------------------------------------------------------
    today = date.today()

    def _date(val):
        if not val: return None
        try:
            from datetime import datetime
            for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"):
                try: return datetime.strptime(str(val), fmt).date()
                except ValueError: pass
        except Exception: pass
        return None

    no_follow_up_set   = sum(1 for r in opportunities if not r.get("Next_Follow_Up_Date__c"))
    overdue_follow_ups = sum(1 for r in opportunities
                             if (d := _date(r.get("Next_Follow_Up_Date__c"))) and d < today)
    stale_activity     = sum(1 for r in opportunities
                             if not r.get("LastActivityDate") or
                             (d := _date(r.get("LastActivityDate"))) and (today - d).days > 30)
    churn_risk_flagged = sum(1 for r in opportunities
                             if (r.get("Churn_Risks__c") or "").strip())

    # -- Auto-renewal exposure --------------------------------------------------
    no_auto_renewal       = [r for r in opportunities
                              if not r.get("CurrentContractHasAutoRenewalClause__c")]
    no_auto_renewal_arr   = sum(_arr(r) for r in no_auto_renewal)
    no_auto_renewal_count = len(no_auto_renewal)

    # -- Commercial flags -------------------------------------------------------
    flat_or_declining_arr = sum(_arr(r) for r in opportunities
                                if (r.get("ARR_Increase__c") or 0) <= 0)
    expanding_arr         = sum(_arr(r) for r in opportunities
                                if (r.get("ARR_Increase__c") or 0) > 0)

    return {
        # Pipeline
        "total_count":          total_count,
        "total_arr":            total_arr,
        "band_distribution":    dict(band_dist),
        "arr_by_product":       dict(arr_by_product),
        "arr_by_owner":         arr_by_owner,
        "arr_by_stage":         dict(arr_by_stage),

        # Health
        "avg_health_score":     round(avg_health_score, 1),
        "avg_data_confidence":  round(avg_data_confidence, 3),
        "at_risk_arr":          at_risk_arr,
        "at_risk_count":        at_risk_count,
        "avg_domain_scores":    {k: round(v, 1) for k, v in avg_domain_scores.items()},
        "override_summary":     dict(override_summary),

        # Outcome
        "likely_win_arr":       likely_win_arr,
        "likely_win_count":     likely_win_count,
        "likely_churn_arr":     likely_churn_arr,
        "likely_churn_count":   likely_churn_count,
        "undetermined_arr":     undetermined_arr,
        "undetermined_count":   undetermined_count,

        # Status
        "status_kpis":          {k: dict(v) for k, v in status_kpis.items()},

        # Renewal timeline
        "renewal_timeline":     {k: dict(v) for k, v in renewal_timeline.items()},

        # Engagement flags
        "no_follow_up_set":     no_follow_up_set,
        "overdue_follow_ups":   overdue_follow_ups,
        "stale_activity":       stale_activity,
        "churn_risk_flagged":   churn_risk_flagged,

        # Auto-renewal exposure
        "no_auto_renewal_arr":   no_auto_renewal_arr,
        "no_auto_renewal_count": no_auto_renewal_count,

        # Commercial
        "flat_or_declining_arr": flat_or_declining_arr,
        "expanding_arr":         expanding_arr,
    }
