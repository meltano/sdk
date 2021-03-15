"""Test sample sync."""

import json
from pathlib import Path
from typing import Optional

from singer_sdk.helpers.typing import ArrayType, ComplexType, ObjectType, StringType, PropertiesList, Property


def test_nested_complex_objects():
    test1a = ArrayType(
        StringType,
        name="Datasets",
    )
    test1b = test1a.to_dict()
    test2a = Property(
        "Datasets",
        ArrayType(
            ObjectType(
                Property("DatasetId", StringType),
                Property("DatasetName", StringType),
            )
        )
    )
    test2b = test2a.to_dict()
