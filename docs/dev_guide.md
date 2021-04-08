# Singer SDK Development Docs

## SDK Overview

Create taps with `singer-sdk` requires overriding just two or three classes:

1. The `Tap` class. This class governs configuration, validation,
   and stream discovery.
2. The stream class. You have different options for your base class depending on the type
   of data source you are working with:
    - `Stream` - The **generic** base class for streams.
    - `RESTStream` - The base class for **REST**-type streams.
    - `GraphQLStream` - The base class for **GraphQL**-type streams. This class inherits
      from `RESTStream`, since GraphQL is built upon REST.
3. An optional authenticator class. You can omit this class entirely if you do not require authentication or if you prefer to write custom authentication logic. The supported authenticator classes are:
    - `SimpleAuthenticator` - This class is functionally equivalent to overriding
      `http_headers` property in the stream class.
    - `OAuthAuthenticator` - This class performs an OAuth 2.0 authentication flow.
    - `OAuthJWTAuthenticator` - This class performs an JWT (Java Web Token) authentication
       flow.

### Detailed Class Reference

For a detailed reference, please see the [SDK Reference Guide](./reference.md)

### Singer SDK Implementation Details

For more detailed information about the Singer SDK implementation, please see the 
[Singer SDK Implementation Details](./implementation/README.md) section.

## Building a New Tap

The best way to get started is by building a new project from the
[cookiecutter tap template](../cookiecutter/tap-template).

## Additional Resources

### Code Samples

For a list of code samples solving a variety of different scenarios, please see our [Code Samples](./code_samples.md) page.

### CLI Samples

For a list of sample CLI commands you can run, [click here](./cli_commands.md).

## Python Tip: Two Ways to Define Properties

In Python, properties within classes like Stream and Tap can generally be overriden
in two ways: _statically_ or _dynamically_. For instance, `primary_keys` and 
`replication_key` should be declared statically if their values are known ahead of time
(during development), and they should be declared dynamically if they vary from one 
environment to another or if they can change at runtime.

### Static example

Here's a simple example of static definitions based on the 
[cookiecutter template](../cookiecutter/tap-template/). This example defines the 
primary key and replication key as fixed values which will not change.

```python
class SimpleSampleStream(Stream):
    primary_keys = ["id"]
    replication_key = None
```

### Dynamic property example

Here is a similar example except that the same properties are calculated dynamically based
on user-provided inputs:

```python
class DynamicSampleStream(Stream):
    @property
    def primary_keys(self):
        """Return primary key dynamically based on user inputs."""
        return self.config["primary_key"]
    
    @property
    def replication_key(self):
        """Return replication key dynamically based on user inputs."""
        result = self.config.get("replication_key")
        if not result:
            self.logger.warning("Danger: could not find replication key!")
        return result
```

Note that the first static example was more concise while this second example is more extensible.

### In summary

- Use the static syntax whenever you are dealing with stream properties that won't change
and use dynamic syntax whenever you need to calculate the stream's properties or discover them dynamically.
- For those new to Python, note that the dynamic syntax is identical to declaring a function or method, with
the one difference of having the `@property` decorator directly above the method definition. This one change
tells Python that you want to be able to access the method as a property (as in `pk = stream.primary_key`)
instead of as a callable function (as in `pk = stream.primary_key()`).

For more examples, please see the [Code Samples](./code_samples.md) page.
