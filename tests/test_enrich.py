import json, sys, os, copy
sys.path.insert(0, "."); sys.path.insert(0, "src")
from sf_health.enrich import enrich

with open("tests/fixtures/sample_opportunities.json") as f:
    FIXTURES = json.load(f)

REQUIRED_KEYS = {
    "health_score", "raw_composite", "health_band", "data_confidence",
    "overrides_applied", "engagement_score", "renewal_score",
    "commercial_score", "risk_score", "days_to_renewal",
}


def test_enrich_appends_all_keys():
    results = enrich(FIXTURES)
    for r in results:
        missing = REQUIRED_KEYS - r.keys()
        assert not missing, f"Missing keys in enriched record: {missing}"


def test_enrich_does_not_mutate():
    originals = copy.deepcopy(FIXTURES)
    enrich(FIXTURES)
    for orig, current in zip(originals, FIXTURES):
        assert orig == current, "enrich() mutated the original records"


def test_days_to_renewal_sign():
    results = enrich(FIXTURES)
    # Epsilon SA has overdue renewal -> days_to_renewal should be negative
    epsilon = next(r for r in results if r["Account.Name"] == "Epsilon SA")
    assert epsilon["days_to_renewal"] < 0, "Overdue renewal should give negative days_to_renewal"
    # Acme Corp has future renewal
    acme = next(r for r in results if r["Account.Name"] == "Acme Corp")
    assert acme["days_to_renewal"] > 0, "Future renewal should give positive days_to_renewal"
