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
    :template: class.rst

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

    typing.StringType
    typing.DateTimeType
    typing.TimeType
    typing.DateType
    typing.DurationType
    typing.EmailType
    typing.HostnameType
    typing.IPv4Type
    typing.IPv6Type
    typing.UUIDType
    typing.URIType
    typing.URITemplateType
    typing.JSONPointerType
    typing.RelativeJSONPointerType
    typing.RegexType
    typing.BooleanType
    typing.IntegerType
    typing.NumberType
    typing.ArrayType
    typing.Property
    typing.ObjectType
    typing.CustomType
    typing.PropertiesList

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
