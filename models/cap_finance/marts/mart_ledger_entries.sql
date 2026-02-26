{{ config(
    materialized='table',
    cluster_by=['posting_date', 'legal_entity', 'account'],
    partition_by={'field': 'posting_date', 'data_type': 'date'}
) }}

SELECT *
FROM {{ ref('stg_journal_entries') }}
