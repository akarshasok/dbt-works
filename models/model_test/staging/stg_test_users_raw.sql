select
    id,
    name,
    created_at
from {{ source('raw', 'users_raw') }}