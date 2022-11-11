from .templates import TargetFileTestTemplate, TargetTestTemplate


class TargetArrayData(TargetFileTestTemplate):
    name = "array_data"


class TargetCamelcaseTest(TargetFileTestTemplate):
    name = "camelcase"


class TargetCliPrintsTest(TargetTestTemplate):
    name = "cli_prints"

    def test(self):
        self.target.print_version()
        self.target.print_about()
        self.target.print_about(format="json")


class TargetDuplicateRecords(TargetFileTestTemplate):
    name = "duplicate_records"


class TargetEncodedStringData(TargetFileTestTemplate):
    name = "encoded_string_data"


class TargetInvalidSchemaTest(TargetFileTestTemplate):
    name = "invalid_schema"


class TargetMultipleStateMessages(TargetFileTestTemplate):
    name = "multiple_state_messages"

    def test(self):
        self.runner.sync_all()
        state_messages = self.runner.state_messages
        assert state_messages == [
            '{"test_multiple_state_messages_a": 1, "test_multiple_state_messages_b": 0}',
            '{"test_multiple_state_messages_a": 3, "test_multiple_state_messages_b": 2}',
            '{"test_multiple_state_messages_a": 5, "test_multiple_state_messages_b": 6}',
        ]


class TargetNoPrimaryKeys(TargetFileTestTemplate):
    name = "no_primary_keys"


class TargetOptionalAttributes(TargetFileTestTemplate):
    name = "optional_attributes"


class TargetRecordBeforeSchemaTest(TargetFileTestTemplate):
    name = "record_before_schema"


class TargetRecordMissingKeyProperty(TargetFileTestTemplate):
    name = "record_missing_key_property"


class TargetRecordMissingRequiredProperty(TargetFileTestTemplate):
    name = "record_missing_required_property"


class TargetSchemaNoProperties(TargetFileTestTemplate):
    name = "schema_no_properties"


class TargetSchemaUpdates(TargetFileTestTemplate):
    name = "schema_updates"


class TargetSpecialCharsInAttributes(TargetFileTestTemplate):
    name = "special_chars_in_attributes"
