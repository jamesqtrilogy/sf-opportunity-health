#!/usr/bin/env python3
"""
run_analysis.py -- CLI entry point for the sf-opportunity-health pipeline.

Usage:
  python scripts/run_analysis.py
  python scripts/run_analysis.py --from-file outputs/sf_opportunities_20260407.json
  python scripts/run_analysis.py --json-only
  python scripts/run_analysis.py --html-only --redistribute
"""

import argparse
import sys
import os

# Make src/ importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from sf_health.query   import fetch_opportunities, save_raw_export, load_from_file
from sf_health.enrich  import enrich
from sf_health.analyse import analyse
from sf_health.report  import write_json, write_html


def _fmt_arr(v: float) -> str:
    return f"${v:>12,.0f}"


def main():
    parser = argparse.ArgumentParser(description="Run Salesforce opportunity health analysis")
    parser.add_argument("--from-file",    metavar="PATH", help="Load from saved JSON export")
    parser.add_argument("--json-only",    action="store_true")
    parser.add_argument("--html-only",    action="store_true")
    parser.add_argument("--output-dir",   default="outputs")
    parser.add_argument("--no-save-raw",  action="store_true")
    parser.add_argument("--redistribute", action="store_true",
                        help="Redistribute weight from missing domains")
    args = parser.parse_args()

    # -- 1. Fetch ---------------------------------------------------------------
    if args.from_file:
        print(f"Loading from file: {args.from_file}")
        records = load_from_file(args.from_file)
    else:
        print("Querying Salesforce via MCP...")
        records = fetch_opportunities()
        if not args.no_save_raw:
            raw_path = save_raw_export(records, args.output_dir)
            print(f"Raw export saved : {raw_path}")

    print(f"Records fetched  : {len(records)}")

    # -- 2. Enrich --------------------------------------------------------------
    enriched = enrich(records, redistribute_missing=args.redistribute)

    # -- 3. Analyse -------------------------------------------------------------
    kpis = analyse(enriched)

    # -- 4. Write outputs -------------------------------------------------------
    json_path = html_path = None
    if not args.html_only:
        json_path = write_json(kpis, enriched, args.output_dir)
    if not args.json_only:
        html_path = write_html(kpis, enriched, output_dir=args.output_dir)

    # -- 5. Print summary -------------------------------------------------------
    bd = kpis.get("band_distribution", {})
    rt = kpis.get("renewal_timeline", {})
    ds = kpis.get("avg_domain_scores", {})
    sk = kpis.get("status_kpis", {})

    print(f"""
-- sf-opportunity-health --------------------------------------------------
  Opportunities   : {kpis['total_count']}
  Total ARR       : {_fmt_arr(kpis['total_arr'])}
  Avg Health      : {kpis['avg_health_score']:.1f} / 100
  Data Confidence : {kpis['avg_data_confidence']:.0%}

  Band distribution:
    Healthy  : {bd.get('Healthy',{}).get('count',0):>4}  {_fmt_arr(bd.get('Healthy',{}).get('arr',0))}
    Caution  : {bd.get('Caution',{}).get('count',0):>4}  {_fmt_arr(bd.get('Caution',{}).get('arr',0))}
    At Risk  : {bd.get('At Risk',{}).get('count',0):>4}  {_fmt_arr(bd.get('At Risk',{}).get('arr',0))}

  Outcome:
    Likely to Win   : {kpis['likely_win_count']:>4}  {_fmt_arr(kpis['likely_win_arr'])}
    Likely to Churn : {kpis['likely_churn_count']:>4}  {_fmt_arr(kpis['likely_churn_arr'])}
    Undetermined    : {kpis['undetermined_count']:>4}  {_fmt_arr(kpis['undetermined_arr'])}

  Status:
    On Track           : {sk.get('On Track',{}).get('count',0)}
    Warning            : {sk.get('Warning',{}).get('count',0)}
    Attention Required : {sk.get('Attention Required',{}).get('count',0)}

  Flags:
    No follow-up set       : {kpis['no_follow_up_set']}
    Overdue follow-ups     : {kpis['overdue_follow_ups']}
    Churn risk flagged     : {kpis['churn_risk_flagged']}
    Stale activity >30d    : {kpis['stale_activity']}
    No auto-renewal clause : {kpis['no_auto_renewal_count']}  ({_fmt_arr(kpis['no_auto_renewal_arr'])} ARR)
    Renewals overdue       : {rt.get('Overdue',{}).get('count',0)}
    Renewals <= 90 days    : {rt.get('0-30d',{}).get('count',0) + rt.get('31-90d',{}).get('count',0)}

  Domain averages:
    Engagement : {ds.get('engagement',0):.1f}
    Renewal    : {ds.get('renewal',0):.1f}
    Commercial : {ds.get('commercial',0):.1f}
    Risk       : {ds.get('risk',0):.1f}

  Overrides fired  : {sum(kpis.get('override_summary',{}).values())}

  Outputs:
    {json_path or '(skipped)'}
    {html_path or '(skipped)'}
---------------------------------------------------------------------------
""")


if __name__ == "__main__":
    main()
