{{ config(
    materialized='table',
    cluster_by=['posting_date', 'legal_entity', 'account'],
    partition_by={'field': 'posting_date', 'data_type': 'date'}
) }}

SELECT
  {{ dbt_utils.generate_surrogate_key([
    'je_id',
    'posting_date',
    '`account`',
    '`entity`',
    '`debit`',
    '`credit`',
    'memo'
  ]) }} AS je_line_sk,
  p.je_id,
  p.posting_date,
  UPPER(p.`account`) AS `account`,
  UPPER(p.`entity`) AS legal_entity,
  p.`debit` AS debit,
  p.`credit` AS credit,
  p.`credit` - p.`debit` AS gl_net_movement,
  p.memo,
  p.ingested_at
FROM {{ source('raw', 'journal_entries') }} AS p
QUALIFY
  ROW_NUMBER() OVER (
    PARTITION BY je_line_sk
    ORDER BY p.ingested_at DESC
  ) = 1
