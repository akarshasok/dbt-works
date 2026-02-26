{{ config(
  materialized='table',
  cluster_by=['calendar_date', 'entity', 'currency'],
  partition_by={'field': 'calendar_date', 'data_type': 'date'}
) }}

WITH RECURSIVE
  date_spine AS (
    SELECT
      {{ var('calendar_start_date', "DATE '2026-01-01'") }} AS calendar_date
    UNION ALL
    SELECT DATE_ADD(calendar_date, INTERVAL 1 DAY)
    FROM date_spine
    WHERE calendar_date < {{ var('calendar_end_date', "DATE '2026-12-31'") }}
  ),

  entity_currency AS (
    SELECT DISTINCT
      entity,
      currency
    FROM {{ ref('mart_payment_events') }}
    WHERE
      entity IS NOT NULL
      AND currency IS NOT NULL
  )

SELECT
  ds.calendar_date,
  ec.entity,
  ec.currency,

  -- business day flag (Mon–Fri default)
  NOT COALESCE(EXTRACT(DAYOFWEEK FROM ds.calendar_date) IN (1, 7), FALSE) AS is_business_day,

  -- standard period attributes
  EXTRACT(YEAR FROM ds.calendar_date) AS period_year,
  EXTRACT(QUARTER FROM ds.calendar_date) AS period_quarter,
  EXTRACT(MONTH FROM ds.calendar_date) AS period_month,
  EXTRACT(WEEK FROM ds.calendar_date) AS period_week,

  -- ISO-safe attributes (important for finance week reporting)
  EXTRACT(ISOYEAR FROM ds.calendar_date) AS iso_year,
  EXTRACT(ISOWEEK FROM ds.calendar_date) AS iso_week,

  -- commonly used rollup keys
  FORMAT_DATE('%Y-%m', ds.calendar_date) AS period_year_month,
  FORMAT_DATE('%G-W%V', ds.calendar_date) AS period_year_week

FROM date_spine AS ds
  CROSS JOIN entity_currency AS ec
