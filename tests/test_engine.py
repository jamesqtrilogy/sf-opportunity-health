import pytest
import json
import sys, os
sys.path.insert(0, ".")
from kpi_tools import HealthScoreEngine, Band, WEIGHTS

with open("tests/fixtures/sample_opportunities.json") as f:
    FIXTURES = json.load(f)

engine = HealthScoreEngine()
RESULTS = {r["Account.Name"]: engine.score(r, r.get("Account.Name", "")) for r in FIXTURES}


def test_weights_sum_to_one():
    assert abs(sum(WEIGHTS.values()) - 1.0) < 1e-9


def test_domain_keys():
    result = list(RESULTS.values())[0]
    assert set(result.domains.keys()) == {"engagement", "renewal", "commercial", "risk"}


def test_acme_healthy():
    r = RESULTS["Acme Corp"]
    assert r.band == Band.HEALTHY, f"Expected Healthy, got {r.band} (score={r.final_score})"


def test_beta_caution():
    r = RESULTS["Beta Ltd"]
    assert r.band == Band.CAUTION, f"Expected Caution, got {r.band} (score={r.final_score})"


def test_gamma_churn_cap():
    r = RESULTS["Gamma Inc"]
    assert r.final_score <= 40, f"Churn cap should limit to 40, got {r.final_score}"


def test_delta_closed_lost():
    r = RESULTS["Delta Co"]
    assert r.final_score == 5, f"Closed Lost must set score to 5, got {r.final_score}"


def test_epsilon_renewal_overdue():
    r = RESULTS["Epsilon SA"]
    assert r.final_score <= 30, f"Overdue renewal cap should be 30, got {r.final_score}"


def test_zeta_late_status():
    r = RESULTS["Zeta Corp"]
    assert r.final_score <= 45, f"Late status cap should be 45, got {r.final_score}"


def test_eta_positive_floor():
    r = RESULTS["Eta GmbH"]
    assert r.final_score >= 75, f"Positive floor should be 75, got {r.final_score}"


def test_theta_low_confidence():
    r = RESULTS["Theta Pty"]
    assert r.data_confidence < 0.35, f"Sparse record confidence should be < 0.35, got {r.data_confidence}"


def test_score_to_dict_keys():
    flat = engine.score_to_dict(list(RESULTS.values())[0])
    expected = {"health_score", "raw_composite", "band", "data_confidence", "overrides"}
    assert expected.issubset(flat.keys())
