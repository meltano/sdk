Reference
=========

.. currentmodule:: singer_sdk

Plugin Classes
--------------

.. autosummary::
    :toctree: _autosummary
    :template: class.rst

    Tap

Stream Classes
--------------

.. autosummary::
    :toctree: _autosummary
    :template: class.rst

    Stream
    RESTStream
    GraphQLStream

Authenticator Classes
---------------------

.. autosummary::
    :toctree: _autosummary
    :template: class.rst

    authenticators.SimpleAuthenticator
    authenticators.OAuthAuthenticator
    authenticators.OAuthJWTAuthenticator


Exception Types
---------------

.. autosummary::
    :toctree: _autosummary
    :template: class.rst

    exceptions.TooManyRecordsException
    exceptions.TapStreamConnectionFailure
    exceptions.InvalidStreamSortException
    exceptions.MaxRecordsLimitException
