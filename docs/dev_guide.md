# Singer SDK Development Docs

## SDK Overview

Create taps with `singer-sdk` requires overriding just two or three classes:

1. The `Tap` class. This class governs configuration, validation,
      and stream discovery.
2. The stream class. For the stream base class, you have different options depending on the type of data
   source you are working with:
    - `Stream` - The **generic** base class for streams.
    - `RESTStream` - The base class for **REST**-type streams.
    - `GraphQLStream` - The base class for **GraphQL**-type streams. This class inherits
      from `RESTStream`, since GraphQL is built upon REST.
3. The optional authenticator class. You can omit this class entirely if you do not require authentication or if you prefer to write custom authentication logic. The supported authenticator classes are:
    - `SimpleAuthenticator` - This class is functionally equivalent to overriding
      `http_headers` property in the stream class.
    - `OAuthAuthenticator` - This class performs an OAuth 2.0 authentication flow.
    - `OAuthJWTAuthenticator` - This class performs an JWT (Java Web Token) authentication
       flow.

## Building a New Tap

The best way to get started is by building a new project from the
[cookiecutter tap template](../cookiecutter/tap-template).

## Detailed Class Reference

For a detailed reference, please see the [SDK Reference Guide](./reference.md)

## Singer SDK Implementation Details

For more detailed information about the Singer SDK implementation, please see the 
[Singer SDK Implementation Details](./implementation/README.md) section.
