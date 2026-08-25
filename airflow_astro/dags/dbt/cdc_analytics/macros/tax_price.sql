{% macro tax_price(extended_price, tax_amount, scale = 2) %}
    ({{ extended_price }} + {{ tax_amount }}::numeric(16, {{ scale }}))
{% endmacro %}