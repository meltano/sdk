Classes Reference
=================

.. currentmodule:: singer_sdk

Plugin Classes
--------------

.. autosummary::
    :toctree: classes
    :template: plugin_class.rst

    Tap
    Target
    SQLTap
    SQLTarget
    InlineMapper

Stream Classes
--------------

.. autosummary::
    :toctree: classes
    :template: stream_class.rst

    Stream
    RESTStream
    GraphQLStream
    SQLStream

Sink Classes
------------

.. autosummary::
    :toctree: classes
    :template: class.rst

    Sink
    RecordSink
    BatchSink
    SQLSink

SQL Connector for SQL Taps and Targets
--------------------------------------

.. autosummary::
    :toctree: classes
    :template: class.rst

    SQLConnector

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
    typing.DiscriminatedUnion
    typing.DurationType
    typing.EmailType
    typing.HostnameType
    typing.IntegerType
    typing.IPv4Type
    typing.IPv6Type
    typing.JSONPointerType
    typing.NumberType
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

Batch
-----

.. autosummary::
    :toctree: classes
    :template: class.rst

    batch.BaseBatcher
    batch.JSONLinesBatcher

Other
-----

.. autosummary::
    :toctree: classes
    :template: class.rst

    connectors.sql.SQLToJSONSchema
    connectors.sql.JSONSchemaToSQL
