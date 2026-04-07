"""
query.py -- Salesforce MCP query and raw export utilities.

Queries ONLY the 25 confirmed fields for this Salesforce org.
Do not add fields not listed in config/fields.yaml.
"""

from __future__ import annotations
import json
import os
from datetime import datetime

# Confirmed SOQL -- 25 fields only
SOQL = """
SELECT
  Id,
  Name,
  Owner.Name,
  Account.Name,
  Amount,
  StageName,
  CloseDate,
  LastActivityDate,
  Opportunity_Status__c,
  Probable_Outcome__c,
  Churn_Risks__c,
  ARR__c,
  ARR_Increase__c,
  Renewal_Date__c,
  Opportunity_Term__c,
  Next_Follow_Up_Date__c,
  Success_Level__c,
  Current_Success_Level__c,
  CurrentContractHasAutoRenewalClause__c,
  Auto_Renewed_Last_Term__c,
  Product__c,
  Late_Status__c,
  High_Value_Opp__c,
  Win_Type__c,
  Opportunity_Status_Notes__c,
  Opportunity_Report__c
FROM Opportunity
WHERE IsClosed = false
  AND StageName IN ('Outreached', 'Engaged', 'Proposal', 'Quote Follow-Up')
  AND Renewal_Date__c >= 2026-01-01
ORDER BY Renewal_Date__c ASC NULLS LAST
""".strip()


def _normalise(raw) -> list[dict]:
    """Accept both {"records": [...]} envelope and raw list."""
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict):
        return raw.get("records", raw.get("Records", []))
    return []


def fetch_opportunities() -> list[dict]:
    """
    Query Salesforce via MCP sf_query tool.
    Returns a list of opportunity dicts with confirmed fields only.
    """
    # MCP call -- Claude Code will resolve this via the connected Salesforce MCP
    result = sf_query(SOQL)  # noqa: F821  (sf_query injected by MCP runtime)
    return _normalise(result)


def save_raw_export(records: list[dict], output_dir: str = "outputs") -> str:
    """Save raw Salesforce export as a date-stamped JSON file."""
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(output_dir, f"sf_opportunities_{timestamp}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, default=str)
    return path


def load_from_file(path: str) -> list[dict]:
    """Load and normalise opportunities from a saved JSON export."""
    with open(path, encoding="utf-8") as f:
        raw = json.load(f)
    return _normalise(raw)
