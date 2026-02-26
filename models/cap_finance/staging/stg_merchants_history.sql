{{ config(
    materialized='table',
    cluster_by=['merchant_id'],
    partition_by={'field': 'valid_from', 'data_type': 'date'}
) }}

SELECT
  {{ dbt_utils.generate_surrogate_key(['merchant_id', 'DATE(onboarded_at)']) }} AS merchant_sk,
  merchant_id,
  merchant_name,
  legal_entity,
  UPPER(country) AS country,
  DATE(onboarded_at) AS valid_from,
  COALESCE(
    DATE_SUB(
      LEAD(DATE(onboarded_at)) OVER (
        PARTITION BY merchant_id
        ORDER BY DATE(onboarded_at)
      ),
      INTERVAL 1 DAY
    ),
    DATE '9999-12-31'
  ) AS valid_to
FROM {{ source('raw', 'merchants') }}
QUALIFY
  ROW_NUMBER() OVER (
    PARTITION BY merchant_id, DATE(onboarded_at)
    ORDER BY merchant_name, legal_entity, country
  ) = 1