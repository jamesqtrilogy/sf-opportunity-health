# sf-opportunity-health

A Python pipeline that queries open Salesforce renewal opportunities via MCP,
scores each account using a 4-domain weighted health engine, and produces an
interactive HTML dashboard and JSON report for the renewal team.

## Prerequisites
- Python 3.11+
- Salesforce MCP connection active in Claude Code (`/mcp` to verify)
- `pip install -r requirements.txt`

## Quick start

### Run against live Salesforce data (Claude Code only)
```bash
python scripts/run_analysis.py
```

### Run from a saved export (any environment)
```bash
python scripts/run_analysis.py --from-file outputs/sf_opportunities_YYYYMMDD_HHMMSS.json
```

## Project structure

```
sf-opportunity-health/
├── README.md
├── CLAUDE.md                          agent constitution + hard rules
├── .gitignore
├── pyproject.toml
├── requirements.txt
│
├── kpi_tools/
│   ├── __init__.py
│   └── engine.py                      HealthScoreEngine — 4-domain scorer
│
├── config/
│   └── fields.yaml                    source of truth for all SF field names
│
├── src/
│   └── sf_health/
│       ├── __init__.py
│       ├── query.py                   SOQL + MCP fetch + file I/O
│       ├── enrich.py                  per-record health scoring
│       ├── analyse.py                 KPI aggregation
│       └── report.py                  JSON + HTML output writers
│
├── scripts/
│   ├── run_analysis.py                CLI entry point
│   └── build_dashboard.py
│
├── templates/
│   └── dashboard.html.j2              Chart.js dashboard template
│
├── tests/
│   ├── fixtures/
│   │   └── sample_opportunities.json  8 records covering all override cases
│   ├── test_engine.py
│   ├── test_enrich.py
│   ├── test_analyse.py
│   └── test_report.py
│
└── outputs/                           gitignored — all generated files land here
```

## Health score methodology

### Domain weights

| Domain     | Weight | Primary signals |
|------------|--------|-----------------|
| Engagement | 35%    | Opportunity_Status__c (35%), Probable_Outcome__c (30%), LastActivityDate (20%), Next_Follow_Up_Date__c (15%) |
| Renewal    | 30%    | Renewal_Date__c days remaining (35%), StageName (30%), CurrentContractHasAutoRenewalClause__c (20%), Auto_Renewed_Last_Term__c (15%) |
| Commercial | 20%    | ARR_Increase__c (40%), Success_Level__c (35%), High_Value_Opp__c (25%) |
| Risk       | 15%    | Churn_Risks__c (45%), Late_Status__c (30%), Win_Type__c (25%) |

### Score bands

| Band    | Range  | Recommended action                                  |
|---------|--------|-----------------------------------------------------|
| Healthy | 80–100 | Quarterly check-in; focus on expansion              |
| Caution | 50–79  | Bi-weekly review; diagnose by domain score          |
| At Risk | 0–49   | Weekly war-room; executive escalation               |

### Override rules

| Rule | Condition | Action |
|------|-----------|--------|
| Closed Lost | StageName == "Closed Lost" | Set to 5 |
| Renewal overdue | Renewal_Date__c < today | Cap at 30 |
| Attention Required | Opportunity_Status__c == "Attention Required" | Cap at 35 |
| Churn risk logged | Churn_Risks__c non-empty | Cap at 40 |
| Late status | Late_Status__c non-empty | Cap at 45 |
| Auto-renewal + Win | auto_renewal True AND outcome Likely to Win | Floor at 75 |

### Field coverage note

This project queries 25 confirmed Salesforce fields only. Support ticket data
(Kayako), financial data (NetSuite), and legal data are not available as structured
Salesforce fields in this org. Data confidence below 70% indicates records where
Commercial or Risk domain fields are sparsely populated.

## Running the tests

```bash
pytest tests/ -v
```

## CLI options

```
--from-file PATH     Load from saved JSON export instead of live MCP query
--json-only          Write JSON report only
--html-only          Write HTML dashboard only
--output-dir DIR     Output directory (default: outputs/)
--no-save-raw        Skip saving the raw Salesforce export
--redistribute       Redistribute weight from missing domains to populated ones
```

## Contributing

Branch naming: `feat/`, `fix/`, `chore/`

PRs require passing `pytest tests/` before merge.
