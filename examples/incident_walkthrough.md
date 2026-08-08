# Incident walkthrough — `inc-freshness_violation-0b17067135fb`

This is one real incident captured end-to-end from the live pipeline run (see `pipeline_run.log`).

- **Dataset:** `order_details`
- **URN:** `urn:li:dataset:(urn:li:dataPlatform:dbt,b2fd91.ORDER_ENTRY_DB.analytics.order_details,PROD)`
- **Failure type:** `FRESHNESS_VIOLATION`
- **Final status:** `READY_TO_DEPLOY`

## 1. Detect — SentryAgent
Sentry scanned DataHub metadata and raised a `FRESHNESS_VIOLATION` alert for the dataset above.

## 2. Diagnose — DetectiveAgent
Detective traced lineage and produced a diagnosis:
- **Root cause URN:** `urn:li:dataset:(urn:li:dataPlatform:dbt,b2fd91.ORDER_ENTRY_DB.analytics.order_details,PROD)`
- **Root cause type:** `FRESHNESS_VIOLATION`
- **Summary:** Freshness violation on urn:li:dataset:(urn:li:dataPlatform:dbt,b2fd91.ORDER_ENTRY_DB.analytics.order_details,PROD) (last modified 1970-01-01T00:00:00+00:00): no upstream source is older, the whole chain is stale or lineage is missing (11 upstream sources checked).
- **Confidence:** 0.8
- **Owner:** b2fd91.brock1@example.com
- **Recommended fix type:** `FRESHNESS_RERUN`

## 3. Engineer — EngineerAgent
Engineer generated a `FRESHNESS_RERUN` SQL fix (see `sample_fix.sql`):

```sql
-- FRESHNESS_RERUN: refresh order_details and confirm recent data
SELECT MAX(order_date) AS latest_ingested_ts FROM order_details;
```

## 4. Validate — ValidatorAgent
Validator checked the fix against the dataset schema and downstream lineage:

- **Safety score:** 1.0
- **Recommendation:** `DEPLOY`
- **Syntax check:** {'passed': True, 'errors': [], 'details': '1 statement(s), 1 executable'}
- **Schema check:** {'passed': True, 'missing_columns': [], 'type_concerns': [], 'details': 'checked 1 referenced, 0 added, 0 altered, 0 dropped column(s) against 55 schema field(s)'}
- **Lineage check:** {'passed': True, 'affected_downstream': [], 'details': '0 dropped column(s), checked 13 downstream dataset schema(s); adding/altering columns is non-breaking'}
- **Breaking changes:** []

The incident is now `READY_TO_DEPLOY` — an operator can approve it in the Mission Control UI, or escalate it.

