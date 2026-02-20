

  create or replace view `ak-data-warehouse`.`dbt_akarsh_staging_layer`.`stg_test_users_raw`
  OPTIONS()
  as select
    id,
    name,
    created_at
from `ak-data-warehouse`.`raw_data`.`users_raw`;

