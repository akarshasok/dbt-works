{{ config(
  materialized='table',
  cluster_by=['ledger_business_date', 'entity', 'currency'],
  partition_by={'field': 'ledger_business_date', 'data_type': 'date'}
) }}

SELECT
  s.settlement_event_sk,
  s.settlement_id,
  s.merchant_id,
  s.settlement_ts,
  s.ledger_business_date,
  s.currency,
  s.status,
  s.gross_amount,
  s.fee_amount,
  s.signed_net_amount,
  m.merchant_sk,
  m.merchant_name,
  m.legal_entity,
  m.country,
  m.merchant_sk IS NULL AS is_missing_merchant_dim
FROM {{ ref('stg_settlement_events') }} AS s
  LEFT JOIN {{ ref('dim_merchant_history') }} AS m
    ON
      s.merchant_id = m.merchant_id
      AND s.ledger_business_date BETWEEN m.valid_from AND m.valid_to
