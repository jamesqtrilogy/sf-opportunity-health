import json, sys
sys.path.insert(0, "."); sys.path.insert(0, "src")
from sf_health.enrich  import enrich
from sf_health.analyse import analyse

with open("tests/fixtures/sample_opportunities.json") as f:
    FIXTURES = json.load(f)

ENRICHED = enrich(FIXTURES)
KPIS = analyse(ENRICHED)


def test_total_count():
    assert KPIS["total_count"] == 8


def test_band_keys():
    assert set(KPIS["band_distribution"].keys()) == {"Healthy", "Caution", "At Risk"}


def test_churn_risk_flagged():
    # Gamma Inc has Churn_Risks__c set
    assert KPIS["churn_risk_flagged"] >= 1


def test_renewals_overdue():
    # Epsilon SA is overdue
    assert KPIS["renewal_timeline"].get("Overdue", {}).get("count", 0) >= 1


def test_no_auto_renewal():
    assert KPIS["no_auto_renewal_count"] >= 1


def test_override_summary_has_closed_lost():
    assert any("Lost" in k or "lost" in k for k in KPIS.get("override_summary", {}).keys())
