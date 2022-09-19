import pytest

from singer_sdk.singer import Schema

STRING_SCHEMA = Schema(type="string", maxLength=32)
STRING_DICT = {"type": "string", "maxLength": 32}
INTEGER_SCHEMA = Schema(type="integer", maximum=1000000)
INTEGER_DICT = {"type": "integer", "maximum": 1000000}
ARRAY_SCHEMA = Schema(type="array", items=INTEGER_SCHEMA)
ARRAY_DICT = {"type": "array", "items": INTEGER_DICT}
OBJECT_SCHEMA = Schema(
    type="object",
    properties={
        "a_string": STRING_SCHEMA,
        "an_array": ARRAY_SCHEMA,
    },
    additionalProperties=True,
    required=["a_string"],
)
OBJECT_DICT = {
    "type": "object",
    "properties": {
        "a_string": STRING_DICT,
        "an_array": ARRAY_DICT,
    },
    "additionalProperties": True,
    "required": ["a_string"],
}


@pytest.mark.parametrize(
    "schema,expected",
    [
        pytest.param(
            STRING_SCHEMA,
            STRING_DICT,
            id="string_to_dict",
        ),
        pytest.param(
            INTEGER_SCHEMA,
            INTEGER_DICT,
            id="integer_to_dict",
        ),
        pytest.param(
            ARRAY_SCHEMA,
            ARRAY_DICT,
            id="array_to_dict",
        ),
        pytest.param(
            OBJECT_SCHEMA,
            OBJECT_DICT,
            id="object_to_dict",
        ),
    ],
)
def test_schema_to_dict(schema, expected):
    assert schema.to_dict() == expected


@pytest.mark.parametrize(
    "pydict,expected",
    [
        pytest.param(
            STRING_DICT,
            STRING_SCHEMA,
            id="schema_from_string_dict",
        ),
        pytest.param(
            INTEGER_DICT,
            INTEGER_SCHEMA,
            id="schema_from_integer_dict",
        ),
        pytest.param(
            ARRAY_DICT,
            ARRAY_SCHEMA,
            id="schema_from_array_dict",
        ),
        pytest.param(
            OBJECT_DICT,
            OBJECT_SCHEMA,
            id="schema_from_object_dict",
        ),
    ],
)
def test_schema_from_dict(pydict, expected):
    assert Schema.from_dict(pydict) == expected
