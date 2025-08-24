{% macro clean_text(expr) -%}
case
  when {{ expr }} ~* '^\s*n\.a\.\s*$' then null
  else nullif(btrim({{ expr }}), '')
end
{%- endmacro %}
