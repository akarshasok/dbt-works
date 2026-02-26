{{ config(
    materialized='table',
    cluster_by=['posting_date', 'entity', 'account'],
    partition_by={'field': 'posting_date', 'data_type': 'date'}
) }}

SELECT
  {{ dbt_utils.generate_surrogate_key([
    'je_id',
    'posting_date',
    'account',
    'entity',
    'debit',
    'credit',
    'memo'
  ]) }} AS je_line_sk,
  je_id,
  posting_date,
  UPPER(account) AS account,
  UPPER(entity) AS entity,
  debit,
  credit,
  credit - debit AS gl_net_movement,
  memo,
  ingested_at
FROM {{ source('raw', 'journal_entries') }}
QUALIFY
  ROW_NUMBER() OVER (
    PARTITION BY je_line_sk
    ORDER BY ingested_at DESC
  ) = 1
