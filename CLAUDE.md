# sf-opportunity-health — Agent Constitution

## Purpose
Query open Salesforce renewal opportunities via MCP, score each account using a
4-domain weighted health engine, aggregate pipeline KPIs, and produce an interactive
HTML dashboard and JSON report.

## Hard rules
- NEVER modify Salesforce records — this project is strictly read-only
- NEVER commit raw opportunity data, JSON exports, or HTML reports to git
- NEVER query fields not listed in config/fields.yaml — org schema is confirmed
- NEVER add Support, NetSuite, or Legal fields as Salesforce query fields; those
  systems are not surfaced as structured SF fields in this org
- NEVER alter class structure in kpi_tools/engine.py without explicit user instruction
- ALWAYS save raw Salesforce export before processing (unless --no-save-raw is passed)
- NEVER delete files in outputs/ without user confirmation

## Pipeline

```
scripts/run_analysis.py
  └── query.py        → fetch_opportunities() or load_from_file()
  └── enrich.py       → enrich() — runs HealthScoreEngine per record
  └── analyse.py      → analyse() — aggregates KPIs
  └── report.py       → write_json() + write_html()
```

## Engine — 4-domain model (adapted from kpi-tools.py)

| Domain     | Weight | Scorer function     | Key signals                                          |
|------------|--------|---------------------|------------------------------------------------------|
| Engagement | 35%    | score_engagement()  | Opportunity_Status__c, Probable_Outcome__c,          |
|            |        |                     | LastActivityDate, Next_Follow_Up_Date__c              |
| Renewal    | 30%    | score_renewal()     | Renewal_Date__c, StageName,                          |
|            |        |                     | CurrentContractHasAutoRenewalClause__c                |
| Commercial | 20%    | score_commercial()  | ARR_Increase__c, Success_Level__c, High_Value_Opp__c |
| Risk       | 15%    | score_risk()        | Churn_Risks__c, Late_Status__c, Win_Type__c          |

## Score bands
- Healthy  80–100  → quarterly check-in; focus on expansion
- Caution  50–79   → bi-weekly review; diagnose by domain score
- At Risk  0–49    → weekly action required; executive escalation

## Override rules (applied after composite, in priority order)
1. StageName == "Closed Lost"                    → set to 5
2. Renewal_Date__c < today                       → cap at 30
3. Opportunity_Status__c == "Attention Required" → cap at 35
4. Churn_Risks__c is non-empty                   → cap at 40
5. Late_Status__c is non-empty                   → cap at 45
6. auto_renewal_clause True + Likely to Win       → floor at 75

## Confirmed Salesforce fields
See config/fields.yaml — 19 custom fields + 6 standard/relational = 25 total.
No external system fields (Kayako, NetSuite, Legal) are in scope.

## Output file conventions
- Raw exports  : outputs/sf_opportunities_YYYYMMDD_HHMMSS.json   (.gitignored)
- JSON reports : outputs/sf_health_report_YYYYMMDD_HHMMSS.json   (.gitignored)
- Dashboards   : outputs/sf_health_dashboard_YYYYMMDD_HHMMSS.html (.gitignored)

## Methodology reference
customer-health-score-guidelines.html — full rubric, governance, calibration
procedures, normalization rules, and implementation checklist. Open in a browser.
