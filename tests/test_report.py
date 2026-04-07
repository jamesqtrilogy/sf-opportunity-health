import json, os, sys, tempfile
sys.path.insert(0, "."); sys.path.insert(0, "src")
from sf_health.enrich  import enrich
from sf_health.analyse import analyse
from sf_health.report  import write_json, write_html

with open("tests/fixtures/sample_opportunities.json") as f:
    FIXTURES = json.load(f)

ENRICHED = enrich(FIXTURES)
KPIS = analyse(ENRICHED)


def test_json_report_structure():
    with tempfile.TemporaryDirectory() as tmp:
        path = write_json(KPIS, ENRICHED, output_dir=tmp)
        with open(path) as f:
            data = json.load(f)
        assert "generated_at" in data
        assert "kpis" in data
        assert "opportunities" in data
        assert len(data["opportunities"]) == 8


def test_html_report_renders():
    with tempfile.TemporaryDirectory() as tmp:
        path = write_html(KPIS, ENRICHED, output_dir=tmp)
        with open(path) as f:
            html = f.read()
        assert "sf-opportunity-health" in html
        assert "Likely to Win" in html
        assert "Chart.js" in html or "chart.umd" in html
