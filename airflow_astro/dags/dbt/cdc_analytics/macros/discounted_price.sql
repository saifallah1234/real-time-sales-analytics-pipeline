{% macro discounted_price(extended_price, discount_amount, scale = 2) %}
    ({{ extended_price }} * (1 - coalesce({{ discount_amount }}, 0) / 100.0))::numeric(16, {{ scale }})
{% endmacro %}