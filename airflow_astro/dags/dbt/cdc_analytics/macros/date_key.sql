{# YYYYMMDD integer key joining facts to dim_date. #}
{% macro date_key(column) %}
    cast(to_char(cast({{ column }} as date), 'YYYYMMDD') as integer)
{% endmacro %}
