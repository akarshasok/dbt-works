{{ config(
    materialized='table',
    cluster_by=['ledger_business_date','merchant_id','currency'],
    partition_by={'field': 'ledger_business_date', 'data_type': 'date'}
) }}

{% set finance_tz = var('finance_tz', 'America/New_York') %}

SELECT
  -- stable event key at the grain you defined (settlement_id + status)
  {{ dbt_utils.generate_surrogate_key(['settlement_id', 'status']) }} AS settlement_event_sk,
  settlement_id,
  merchant_id,
  settled_at AS settlement_ts,
  ingested_at,
  gross_amount,
  fee_amount,
  CASE
    WHEN status = 'SETTLED' THEN net_amount
    WHEN status = 'REVERSED' THEN -net_amount
    ELSE CAST(0 AS NUMERIC)
  END AS signed_net_amount,
  currency,
  UPPER(status) AS status,

  -- finance-local ledger date (per your final draft)
  DATE(DATETIME(settled_at, '{{ finance_tz }}')) AS ledger_business_date
FROM {{ source('raw', 'settlement_events') }}
QUALIFY
  ROW_NUMBER() OVER (
    PARTITION BY settlement_id, status
    ORDER BY settled_at DESC, ingested_at DESC
  ) = 1
