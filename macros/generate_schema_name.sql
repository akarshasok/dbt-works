{% macro generate_schema_name(custom_schema_name, node) -%}
  {# custom_schema_name is what you set with +schema in dbt_project.yml #}

  {% if target.name == 'dev' %}
    {# per-dev isolation: dbt_<user>_staging_layer, dbt_<user>_mart_layer, etc. #}
    {{ "dbt_" ~ env_var('DBT_USER') ~ "_" ~ custom_schema_name }}

  {% elif target.name == 'ci' %}
    {# stable CI datasets #}
    {{ "dbt_ci_" ~ custom_schema_name }}

  {% else %}
    {# prod uses the real shared datasets you created #}
    {{ custom_schema_name }}
  {% endif %}
{%- endmacro %}