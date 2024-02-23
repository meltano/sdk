{{ fullname }}
{{ "=" * fullname|length }}

.. currentmodule:: {{ module }}

.. autoclass:: {{ name }}
    :members:
    :special-members: __init__, __call__
    {%- if name in ('IntegerType', 'NumberType') %}
    :inherited-members: JSONTypeHelper
    {%- endif %}
