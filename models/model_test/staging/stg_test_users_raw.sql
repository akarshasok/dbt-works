SELECT
  id,
  name,
  created_at
FROM {{ source('raw', 'users_raw') }}
