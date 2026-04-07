"""
report.py -- Write JSON report and HTML dashboard from analysed KPI data.
"""

from __future__ import annotations
import json
import os
from datetime import datetime
from jinja2 import Environment, FileSystemLoader


def _timestamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def write_json(kpis: dict, opportunities: list[dict],
               output_dir: str = "outputs") -> str:
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, f"sf_health_report_{_timestamp()}.json")
    payload = {
        "generated_at": datetime.now().isoformat(),
        "kpis": kpis,
        "opportunities": sorted(opportunities,
                                key=lambda r: r.get("health_score", 50)),
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)
    return path


def write_html(kpis: dict, opportunities: list[dict],
               template_dir: str = "templates",
               output_dir: str = "outputs") -> str:
    os.makedirs(output_dir, exist_ok=True)
    env = Environment(loader=FileSystemLoader(template_dir), autoescape=True)
    template = env.get_template("dashboard.html.j2")

    path = os.path.join(output_dir, f"sf_health_dashboard_{_timestamp()}.html")
    html = template.render(
        kpis=kpis,
        opportunities=sorted(opportunities,
                             key=lambda r: r.get("health_score", 50)),
        generated_at=datetime.now().strftime("%d %b %Y %H:%M"),
        kpis_json=json.dumps(kpis, default=str),
        opps_json=json.dumps(
            sorted(opportunities, key=lambda r: r.get("health_score", 50)),
            default=str),
    )
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)
    return path
