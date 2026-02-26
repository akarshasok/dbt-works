{{ config(
    materialized='table',
    cluster_by=['merchant_id', 'valid_from'],
    partition_by={'field': 'valid_from', 'data_type': 'date'}
) }}

SELECT
  merchant_sk,
  merchant_id,
  merchant_name,
  legal_entity,
  country,
  valid_from,
  valid_to
FROM {{ ref('stg_merchants_history') }}
