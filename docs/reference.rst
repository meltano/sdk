Classes Reference
=================

.. currentmodule:: singer_sdk

.. note::
   **Maintaining this file:**

   This file is manually maintained to provide semantic organization of the SDK's public API.
   When adding new public classes or functions:

   1. **Use canonical module paths** - Document classes at their canonical location, not re-export locations.
      Example: Use ``schema.source.SchemaSource`` not ``SchemaSource``

   2. **Add to the appropriate section** - Place classes in semantic sections (Plugin Classes,
      Stream Classes, Testing, etc.) rather than by module structure

   3. **Update redirects** - If a class is re-exported from the main package or other ``__init__.py``
      files, add a redirect in ``conf.py`` to point from the re-export path to the canonical path

   4. **Choose the right template**:

      - ``plugin_class.rst`` - For Tap, Target, and related plugin classes
      - ``stream_class.rst`` - For Stream subclasses (shows inherited members from Stream)
      - ``class.rst`` - For all other classes (default)

Plugin Classes
--------------

.. autosummary::
    :toctree: classes
    :template: plugin_class.rst

    Tap
    Target
    InlineMapper
    sql.SQLTap
    sql.SQLTarget

Stream Classes
--------------

.. autosummary::
    :toctree: classes
    :template: stream_class.rst

    Stream
    RESTStream
    GraphQLStream
    sql.SQLStream

Sink Classes
------------

.. autosummary::
    :toctree: classes
    :template: class.rst

    Sink
    RecordSink
    BatchSink
    sql.SQLSink

SQL Connector for SQL Taps and Targets
--------------------------------------

.. autosummary::
    :toctree: classes
    :template: class.rst

    sql.SQLConnector

Authenticator Classes
---------------------

.. autosummary::
    :toctree: classes
    :template: class.rst

    authenticators.APIAuthenticatorBase
    authenticators.APIKeyAuthenticator
    authenticators.BasicAuthenticator
    authenticators.BearerTokenAuthenticator
    authenticators.OAuthAuthenticator
    authenticators.OAuthJWTAuthenticator
    authenticators.SimpleAuthenticator

Exception Types
---------------

.. autosummary::
    :toctree: classes
    :template: class.rst

    exceptions.ConfigValidationError
    exceptions.FatalAPIError
    exceptions.InvalidStreamSortException
    exceptions.MapExpressionError
    exceptions.MaxRecordsLimitException
    exceptions.RecordsWithoutSchemaException
    exceptions.RetriableAPIError
    exceptions.StreamMapConfigError
    exceptions.TapStreamConnectionFailure
    exceptions.TooManyRecordsException
    exceptions.DiscoveryError
    exceptions.InvalidReplicationKeyException
    exceptions.RequestedAbortException
    exceptions.AbortedSyncExceptionBase
    exceptions.AbortedSyncFailedException
    exceptions.AbortedSyncPausedException
    exceptions.ConformedNameClashException
    exceptions.MissingKeyPropertiesError
    exceptions.InvalidJSONSchema
    exceptions.InvalidRecord

JSON Schema builder classes
---------------------------

.. autosummary::
    :toctree: classes/typing
    :template: class.rst

    typing.PropertiesList
    typing.ArrayType
    typing.BooleanType
    typing.Constant
    typing.CustomType
    typing.DateTimeType
    typing.DateType
    typing.DecimalType
    typing.DiscriminatedUnion
    typing.DurationType
    typing.EmailType
    typing.HostnameType
    typing.IntegerType
    typing.IPv4Type
    typing.IPv6Type
    typing.JSONPointerType
    typing.ObjectType
    typing.OneOf
    typing.Property
    typing.RegexType
    typing.RelativeJSONPointerType
    typing.StringType
    typing.TimeType
    typing.URITemplateType
    typing.URIType
    typing.UUIDType

Pagination
----------

.. autosummary::
    :toctree: classes
    :template: class.rst

    pagination.BaseAPIPaginator
    pagination.SinglePagePaginator
    pagination.BaseHATEOASPaginator
    pagination.HeaderLinkPaginator
    pagination.JSONPathPaginator
    pagination.SimpleHeaderPaginator
    pagination.BasePageNumberPaginator
    pagination.BaseOffsetPaginator
    pagination.LegacyPaginatedStreamProtocol
    pagination.LegacyStreamPaginator

Batch Encoding
--------------

.. autosummary::
    :toctree: classes
    :template: class.rst

    batch.BaseBatcher
    batch.Batcher

Schema Sources
--------------

.. autosummary::
    :toctree: classes
    :template: class.rst

    schema.source.SchemaSource
    schema.source.SchemaDirectory
    schema.source.StreamSchema
    schema.source.OpenAPISchema

Testing
-------

Test Runners
~~~~~~~~~~~~

.. autosummary::
    :toctree: classes
    :template: class.rst

    testing.runners.SingerTestRunner
    testing.runners.TapTestRunner
    testing.runners.TargetTestRunner

Factory Functions
~~~~~~~~~~~~~~~~~

.. autosummary::
    :toctree: classes

    testing.factory.get_tap_test_class
    testing.factory.get_target_test_class

Test Configuration
~~~~~~~~~~~~~~~~~~

.. autosummary::
    :toctree: classes
    :template: class.rst

    testing.config.SuiteConfig

Standard Tap Tests
~~~~~~~~~~~~~~~~~~

.. autosummary::
    :toctree: classes
    :template: class.rst

    testing.tap_tests.TapCLIPrintsTest
    testing.tap_tests.TapDiscoveryTest
    testing.tap_tests.TapStreamConnectionTest
    testing.tap_tests.TapValidFinalStateTest
    testing.tap_tests.StreamSchemaIsValidTest
    testing.tap_tests.StreamReturnsRecordTest
    testing.tap_tests.StreamCatalogSchemaMatchesRecordTest
    testing.tap_tests.StreamRecordMatchesStreamSchema
    testing.tap_tests.StreamRecordSchemaMatchesCatalogTest
    testing.tap_tests.StreamPrimaryKeysTest
    testing.tap_tests.AttributeIsBooleanTest
    testing.tap_tests.AttributeIsDateTimeTest
    testing.tap_tests.AttributeIsIntegerTest
    testing.tap_tests.AttributeIsNumberTest
    testing.tap_tests.AttributeIsObjectTest
    testing.tap_tests.AttributeNotNullTest

Standard Target Tests
~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
    :toctree: classes
    :template: class.rst

    testing.target_tests.TargetCliPrintsTest
    testing.target_tests.TargetArrayData
    testing.target_tests.TargetCamelcaseTest
    testing.target_tests.TargetCamelcaseComplexSchema
    testing.target_tests.TargetDuplicateRecords
    testing.target_tests.TargetEncodedStringData
    testing.target_tests.TargetInvalidSchemaTest
    testing.target_tests.TargetMultipleStateMessages
    testing.target_tests.TargetNoPrimaryKeys
    testing.target_tests.TargetOptionalAttributes
    testing.target_tests.TargetPrimaryKeyUpdates
    testing.target_tests.TargetRecordBeforeSchemaTest
    testing.target_tests.TargetRecordMissingKeyProperty
    testing.target_tests.TargetRecordMissingOptionalFields
    testing.target_tests.TargetSchemaNoProperties
    testing.target_tests.TargetSchemaUpdates
    testing.target_tests.TargetSpecialCharsInAttributes

Test Templates
~~~~~~~~~~~~~~

.. autosummary::
    :toctree: classes
    :template: class.rst

    testing.templates.TestTemplate
    testing.templates.TapTestTemplate
    testing.templates.TargetTestTemplate
    testing.templates.StreamTestTemplate
    testing.templates.AttributeTestTemplate

Test Suites
~~~~~~~~~~~

.. autosummary::
    :toctree: classes
    :template: class.rst

    testing.suites.SingerTestSuite

Singer Specification Types
---------------------------

Message Types
~~~~~~~~~~~~~

.. autosummary::
    :toctree: classes
    :template: class.rst

    singerlib.messages.Message
    singerlib.messages.RecordMessage
    singerlib.messages.SchemaMessage
    singerlib.messages.StateMessage
    singerlib.messages.ActivateVersionMessage
    singerlib.messages.SingerMessageType

Catalog Types
~~~~~~~~~~~~~

.. autosummary::
    :toctree: classes
    :template: class.rst

    singerlib.catalog.Catalog
    singerlib.catalog.CatalogEntry
    singerlib.catalog.Metadata
    singerlib.catalog.MetadataMapping
    singerlib.catalog.StreamMetadata
    singerlib.catalog.SelectionMask

Schema Utilities
~~~~~~~~~~~~~~~~

.. autosummary::
    :toctree: classes
    :template: class.rst

    singerlib.schema.Schema

.. autosummary::
    :toctree: classes

    singerlib.schema.resolve_schema_references

Message Utilities
~~~~~~~~~~~~~~~~~

.. autosummary::
    :toctree: classes

    singerlib.messages.exclude_null_dict
    singerlib.messages.format_message
    singerlib.messages.write_message

Contributed Modules
-------------------

Filesystem Components
~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
    :toctree: classes
    :template: class.rst

    contrib.filesystem.stream.FileStream
    contrib.filesystem.tap.FolderTap

Batch Encoders
~~~~~~~~~~~~~~

.. autosummary::
    :toctree: classes
    :template: class.rst

    contrib.batch_encoder_jsonl.JSONLinesBatcher
    contrib.batch_encoder_parquet.ParquetBatcher

Helper Utilities
----------------

Capabilities
~~~~~~~~~~~~

.. autosummary::
    :toctree: classes
    :template: class.rst

    helpers.capabilities.CapabilitiesEnum
    helpers.capabilities.PluginCapabilities
    helpers.capabilities.TapCapabilities
    helpers.capabilities.TargetCapabilities
    helpers.capabilities.TargetLoadMethods

JSONPath
~~~~~~~~

.. autosummary::
    :toctree: classes

    helpers.jsonpath.extract_jsonpath

Plugin Information
------------------

About Classes
~~~~~~~~~~~~~

.. autosummary::
    :toctree: classes
    :template: class.rst

    about.AboutInfo
    about.AboutFormatter
    about.JSONFormatter
    about.MarkdownFormatter

Metrics Classes
~~~~~~~~~~~~~~~

.. autosummary::
    :toctree: classes
    :template: class.rst

    metrics.Point
    metrics.Metric
    metrics.Tag
    metrics.Status

Other
-----

.. autosummary::
    :toctree: classes
    :template: class.rst

    sql.connector.SQLToJSONSchema
    sql.connector.JSONSchemaToSQL
