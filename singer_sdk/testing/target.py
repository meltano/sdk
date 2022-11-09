import os

import pytest
from jsonschema import ValidationError
from sqlalchemy import inspect

from .testing import target_sync_test


def load_stream(filename):
    myDir = os.path.dirname(os.path.abspath(__file__))
    stream = os.path.join(myDir, "target_test_streams", filename)
    with open(stream) as f:
        return [line for line in f]


SDC_METADATA_COLUMNS = {
    "_sdc_extracted_at",
    "_sdc_received_at",
    "_sdc_batched_at",
    "_sdc_deleted_at",
    "_sdc_sequence",
    "_sdc_table_version",
}


class StandardSqlTargetTests:
    """Standard Target Tests.

    Requires 3 fixtures:

        1. A `Target` fixture that returns the Target class to be tested.
        2. A `config` fixture containing the Target config to use during testing.
        3. A `sqlalchemy_connection` fixture that returns a SQLAlchemy Connection to use
            for interacting with the target database to verify written streams.
    """

    def test_record_before_schema(self, Target, config):
        test_stream = "record_before_schema.singer"
        target = Target(config)
        stream = load_stream(test_stream)

        # with pytest.raises(Exception) as excinfo:
        target_sync_test(target, stream)
        # assert "encountered before a corresponding schema" in str(excinfo.value)

    def test_invalid_schema(self, Target, config):
        test_stream = "invalid_schema.singer"
        target = Target(config)
        stream = load_stream(test_stream)

        with pytest.raises(ValidationError) as excinfo:
            target_sync_test(target, stream)
        assert "Not supported schema" in str(excinfo.value)

    def test_record_missing_key_property(self, Target, config, sqlalchemy_connection):
        test_stream = "record_missing_key_property.singer"
        target = Target(config)
        stream = load_stream(test_stream)

        with pytest.raises(ValidationError) as excinfo:
            target_sync_test(target, stream)
        assert "id" in str(excinfo.value)

        # Drop the Test Tables
        # for stream, loader in target.loaders.items():
        #     loader.table.drop(loader.engine)

    def test_record_missing_required_property(
        self, Target, config, sqlalchemy_connection
    ):
        test_stream = "record_missing_required_property.singer"
        target = Target(config)
        stream = load_stream(test_stream)

        with pytest.raises(ValidationError) as excinfo:
            target_sync_test(target, stream)
        assert "'id' is a required property" in str(excinfo.value)

        # Drop the Test Tables
        # for stream, loader in target.loaders.items():
        #     loader.table.drop(loader.engine)

    def test_camelcase(self, Target, config, sqlalchemy_connection):
        # The expected results to compare
        expected_results = {
            "state": None,
            "tables": ["test_camelcase"],
            "columns": {"test_camelcase": ["id", "client_name"]},
            "total_records": {"test_camelcase": 2},
        }

        test_stream = "camelcase.singer"

        self.integration_test(
            Target,
            config,
            sqlalchemy_connection,
            expected_results,
            test_stream,
            drop_schema=False,
        )

        # We also need to test that the record has data in the camelcased field
        item_query = f"SELECT client_name FROM test_camelcase"
        item_result = sqlalchemy_connection.execute(item_query).fetchone()
        assert item_result[0] == "Gitter Windows Desktop App"

    def test_special_chars_in_attributes(self, Target, config, sqlalchemy_connection):
        # The expected results to compare
        expected_results = {
            "state": None,
            "tables": ["test_special_chars_in_attributes"],
            "columns": {
                "test_special_chars_in_attributes": [
                    "_id",
                    "d__env",
                    "d__agent_type",
                    "d__agent_os_version",
                ]
            },
            "total_records": {"test_special_chars_in_attributes": 1},
        }

        test_stream = "special_chars_in_attributes.singer"

        self.integration_test(
            Target, config, sqlalchemy_connection, expected_results, test_stream
        )

    def test_optional_attributes(self, Target, config, sqlalchemy_connection):
        # The expected results to compare
        expected_results = {
            "state": {"test_optional_attributes": 4},
            "tables": ["test_optional_attributes"],
            "columns": {
                "test_optional_attributes": [
                    "id",
                    "optional",
                ]
            },
            "total_records": {"test_optional_attributes": 4},
        }

        test_stream = "optional_attributes.singer"

        self.integration_test(
            Target, config, sqlalchemy_connection, expected_results, test_stream
        )

    def test_schema_no_properties(self, Target, config, sqlalchemy_connection):
        # The expected results to compare
        expected_results = {
            "state": None,
            "tables": [
                "test_object_schema_with_properties",
                "test_object_schema_no_properties",
            ],
            "columns": {
                "test_object_schema_with_properties": [
                    "object_store__id",
                    "object_store__metric",
                ],
                "test_object_schema_no_properties": [
                    "object_store",
                ],
            },
            "total_records": {
                "test_object_schema_with_properties": 2,
                "test_object_schema_no_properties": 2,
            },
        }

        test_stream = "schema_no_properties.singer"

        self.integration_test(
            Target,
            config,
            sqlalchemy_connection,
            expected_results,
            test_stream,
            drop_schema=False,
        )

        # We also need to test that the proper data records were stored
        query = (
            "SELECT COUNT(*) "
            f" FROM test_object_schema_with_properties "
            " WHERE object_store__id = 1 AND object_store__metric = 187"
        )
        result = sqlalchemy_connection.execute(query).fetchone()
        assert result[0] == 1

        query = (
            "SELECT COUNT(*) "
            f" FROM test_object_schema_no_properties "
            " WHERE object_store = \"{'id': 1, 'metric': 1}\""
        )
        result = sqlalchemy_connection.execute(query).fetchone()
        assert result[0] == 1

        # Drop the Test Tables
        sqlalchemy_connection.execute(f"DROP TABLE test_object_schema_with_properties")
        sqlalchemy_connection.execute(f"DROP TABLE test_object_schema_no_properties")

    def test_schema_updates(self, Target, config, sqlalchemy_connection):
        # The expected results to compare
        expected_results = {
            "state": {"test_schema_updates": 6},
            "tables": ["test_schema_updates"],
            "columns": {
                "test_schema_updates": [
                    "id",
                    "a1",
                    "a2",
                    "a3",
                    "a4__id",
                    "a4__value",
                    "a5",
                    "a6",
                ]
            },
            "total_records": {"test_schema_updates": 6},
        }

        test_stream = "schema_updates.singer"

        self.integration_test(
            Target, config, sqlalchemy_connection, expected_results, test_stream
        )

    def test_multiple_state_messages(
        self, Target, capsys, config, sqlalchemy_connection
    ):
        # The expected results to compare
        expected_results = {
            "state": {
                "test_multiple_state_messages_a": 5,
                "test_multiple_state_messages_b": 6,
            },
            "tables": [
                "test_multiple_state_messages_a",
                "test_multiple_state_messages_b",
            ],
            "columns": {
                "test_multiple_state_messages_a": [
                    "id",
                    "metric",
                ],
                "test_multiple_state_messages_b": [
                    "id",
                    "metric",
                ],
            },
            "total_records": {
                "test_multiple_state_messages_a": 6,
                "test_multiple_state_messages_b": 6,
            },
        }

        test_stream = "multiple_state_messages.singer"

        updated_config = {**config, "batch_size": 3}
        self.integration_test(
            Target, updated_config, sqlalchemy_connection, expected_results, test_stream
        )

        # Check that the expected State messages where flushed
        expected_stdout = [
            '{"test_multiple_state_messages_a": 1, "test_multiple_state_messages_b": 0}',
            '{"test_multiple_state_messages_a": 3, "test_multiple_state_messages_b": 2}',
            '{"test_multiple_state_messages_a": 5, "test_multiple_state_messages_b": 6}',
            "",
        ]

        captured = capsys.readouterr()
        assert captured.out == "\n".join(expected_stdout)

    def test_relational_data(self, Target, config, sqlalchemy_connection):
        # Start with a simple initial insert for everything

        # The expected results to compare
        expected_results = {
            "state": {"test_users": 5, "test_locations": 3, "test_user_in_location": 3},
            "tables": ["test_users", "test_locations", "test_user_in_location"],
            "columns": {
                "test_users": ["id", "name"],
                "test_locations": ["id", "name"],
                "test_user_in_location": [
                    "id",
                    "user_id",
                    "location_id",
                    "info__weather",
                    "info__mood",
                ],
            },
            "total_records": {
                "test_users": 5,
                "test_locations": 3,
                "test_user_in_location": 3,
            },
        }

        test_stream = "user_location_data.singer"

        # We are not dropping the schema after the first integration test
        #  in order to also test UPSERTing records to SQLite
        self.integration_test(
            Target,
            config,
            sqlalchemy_connection,
            expected_results,
            test_stream,
            drop_schema=False,
        )

        # And then test Upserting (Combination of Updating already available
        #   rows and inserting a couple new rows)
        expected_results["state"] = {
            "test_users": 13,
            "test_locations": 8,
            "test_user_in_location": 14,
        }

        expected_results["total_records"] = {
            "test_users": 8,
            "test_locations": 5,
            "test_user_in_location": 5,
        }
        test_stream = "user_location_upsert_data.singer"

        self.integration_test(
            Target,
            config,
            sqlalchemy_connection,
            expected_results,
            test_stream,
            drop_schema=True,
        )

    def test_no_primary_keys(self, Target, config, sqlalchemy_connection):
        # The expected results to compare
        expected_results = {
            "state": {"test_no_pk": 3},
            "tables": ["test_no_pk"],
            "columns": {
                "test_no_pk": [
                    "id",
                    "metric",
                ]
            },
            "total_records": {"test_no_pk": 3},
        }

        test_stream = "no_primary_keys.singer"

        # We are not dropping the schema after the first integration test
        #  in order to also test APPENDING records when no PK is defined
        self.integration_test(
            Target,
            config,
            sqlalchemy_connection,
            expected_results,
            test_stream,
            drop_schema=False,
        )

        # And then test Upserting
        # The Total Records in this case should be 8 (5+3) due to the records
        #  being appended and not UPSERTed
        expected_results["state"] = {"test_no_pk": 5}
        expected_results["total_records"] = {"test_no_pk": 8}

        test_stream = "no_primary_keys_append.singer"

        self.integration_test(
            Target,
            config,
            sqlalchemy_connection,
            expected_results,
            test_stream,
            drop_schema=True,
        )

    def test_duplicate_records(self, Target, config, sqlalchemy_connection):
        # The expected results to compare
        expected_results = {
            "state": {"test_duplicate_records": 2},
            "tables": ["test_duplicate_records"],
            "columns": {"test_duplicate_records": ["id", "metric"]},
            "total_records": {"test_duplicate_records": 2},
        }

        test_stream = "duplicate_records.singer"

        self.integration_test(
            Target, config, sqlalchemy_connection, expected_results, test_stream
        )

    def test_array_data(self, Target, config, sqlalchemy_connection):
        # The expected results to compare
        expected_results = {
            "state": {"test_carts": 4},
            "tables": ["test_carts"],
            "columns": {"test_carts": ["id", "fruits"]},
            "total_records": {"test_carts": 4},
        }

        test_stream = "array_data.singer"

        self.integration_test(
            Target, config, sqlalchemy_connection, expected_results, test_stream
        )

    def test_encoded_string_data(self, Target, config, sqlalchemy_connection):
        # The expected results to compare
        expected_results = {
            "state": {
                "test_strings": 11,
                "test_strings_in_objects": 11,
                "test_strings_in_arrays": 6,
            },
            "tables": [
                "test_strings",
                "test_strings_in_objects",
                "test_strings_in_arrays",
            ],
            "columns": {
                "test_strings": ["id", "info"],
                "test_strings_in_objects": [
                    "id",
                    "info__name",
                    "info__value",
                ],
                "test_strings_in_arrays": ["id", "strings"],
            },
            "total_records": {
                "test_strings": 11,
                "test_strings_in_objects": 11,
                "test_strings_in_arrays": 6,
            },
        }

        test_stream = "encoded_strings.singer"

        self.integration_test(
            Target, config, sqlalchemy_connection, expected_results, test_stream
        )

    def integration_test(
        self,
        Target,
        config,
        sqlalchemy_connection,
        expected,
        stream_file,
        drop_schema=True,
    ):
        try:
            # Create the Target and fully run it using the user_location_data
            target = Target(config)

            stream = load_stream(stream_file)

            target_sync_test(target, stream)

            # Check that the final state is the expected one
            assert target.last_emitted_state == expected["state"]

            # Check that the requested schema has been created
            inspector = inspect(sqlalchemy_connection)

            all_table_names = inspector.get_table_names()

            for table in expected["tables"]:
                # Check that the Table has been created in SQLite
                assert table in all_table_names

                # Check that the Table created has the requested attributes
                db_columns = []
                columns = inspector.get_columns(table)
                for column in columns:
                    db_columns.append(column["name"].lower())
                    assert column["name"].lower() in expected["columns"][table]

                for column in expected["columns"][table]:
                    assert column in db_columns

                # Check that the correct number of rows were inserted
                query = f"SELECT COUNT(*) FROM {table}"

                results = sqlalchemy_connection.execute(query).fetchone()
                assert results[0] == expected["total_records"][table]
        finally:
            pass
            # # Drop the Test Tables
            # if drop_schema:
            #     for stream, loader in target.loaders.items():
            #         loader.table.drop(loader.engine)
