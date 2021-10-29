Reference
=========

.. currentmodule:: singer_sdk

Plugin Classes
--------------

.. autosummary::
    :toctree: _autosummary
    :template: class.rst

    Tap
    Target

Stream Classes
--------------

.. autosummary::
    :toctree: _autosummary
    :template: class.rst

    Stream
    RESTStream
    GraphQLStream
    SQLStream

Sink Classes
--------------

.. autosummary::
    :toctree: _autosummary
    :template: class.rst

    Sink
    RecordSink
    BatchSink

Authenticator Classes
---------------------

.. autosummary::
    :toctree: _autosummary
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
    :toctree: _autosummary
    :template: class.rst

    exceptions.ConfigValidationError
    exceptions.InvalidStreamSortException
    exceptions.MapExpressionError
    exceptions.MaxRecordsLimitException
    exceptions.RecordsWitoutSchemaException
    exceptions.StreamMapConfigError
    exceptions.TapStreamConnectionFailure
    exceptions.TooManyRecordsException
