SDK Reference
=============

.. currentmodule:: singer_sdk

Plugin Classes
--------------

.. autosummary::
    :toctree: classes
    :template: class.rst

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
    exceptions.RecordsWitoutSchemaException
    exceptions.RetriableAPIError
    exceptions.StreamMapConfigError
    exceptions.TapStreamConnectionFailure
    exceptions.TooManyRecordsException

JSON Schema builder classes
---------------------------

.. autosummary::
    :template: module.rst

    typing
