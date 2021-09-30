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

    exceptions.TooManyRecordsException
    exceptions.TapStreamConnectionFailure
    exceptions.InvalidStreamSortException
    exceptions.MaxRecordsLimitException
    exceptions.RecordsWitoutSchemaException
