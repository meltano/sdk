# SDK Dev Guide

## Overview

### Tap Development Overview

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

## Target Development Overview

Create targets with `singer-sdk` requires overriding just two classes:

1. The `Target` class. This class governs configuration, validation,
   and stream discovery.
2. The `Sink` class. This class is responsible for writing records to the target
   and keeping tally of written records. Each `Sink` implementation my write
   records immediately in `Sink.load_record()` or in batches during `Sink.drain()`.

### SDK Implementation Details

For more detailed information about the SDK implementation, please see the
[SDK Implementation Details](./implementation/README.md) section.

## Building a New Tap

The best way to get started is by building a new project from the
[cookiecutter](https://cookiecutter.readthedocs.io)
[tap template](https://gitlab.com/meltano/singer-sdk/-/tree/main/cookiecutter/tap-template).

## Building a New Target

- [ ] TODO: The target cookiecutter implementation is not yet built.

## Additional Resources

### Detailed Class Reference

For a detailed reference, please see the [SDK Reference Guide](./reference.md)

### Singer SDK Implementation Details

For more detailed information about the Singer SDK implementation, please see the
[Singer SDK Implementation Details](./implementation/README.md) section.

### Code Samples

For a list of code samples solving a variety of different scenarios, please see our [Code Samples](./code_samples.md) page.
To use the cookiecutter template:

```bash
# Install pipx if you haven't already
pip3 install pipx
pipx ensurepath
# Restart your terminal here, if needed, to get the updated PATH
pipx install cookiecutter
```

Initialize Cookiecutter template:

```bash
cookiecutter https://gitlab.com/meltano/singer-sdk --directory="cookiecutter/tap-template"
```

Once you've answered the cookiecutter prompts, follow the instructions in the
generated `README.md` file to complete your new tap. You can also reference the
[Meltano Tutorial](https://meltano.com/tutorials/create-a-custom-extractor.html) for a more
detailed guide.

## Additional Resources

### Code Samples

For a list of code samples solving a variety of different scenarios, please see our
[Code Samples](./code_samples.md) page.

### CLI Samples

For a list of sample CLI commands you can run, [click here](./cli_commands.md).

### Python Tips

We've collected some [Python tips](python_tips.md) which may be helpful for new SDK users.
