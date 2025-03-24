# Getting Started

## Tap Development Overview

Create taps with the SDK requires overriding just two or three classes:

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
   - `OAuthJWTAuthenticator` - This class performs an JWT (JSON Web Token) authentication
     flow. Requires installing the `singer-sdk[jwt]` extra.

## Target Development Overview

Create targets with the SDK requires overriding just two classes:

1. The `Target` class. This class governs configuration, validation,
   and stream discovery.
2. The `Sink` class. You have two different options depending on whether your target
   prefers writing one record at a time versus writing in batches:
   - `RecordSink` writes one record at a time, via the `process_record()`
     method.
   - `BatchSink` writes one batch at a time. Important class members include:
     - `start_batch()` to (optionally) initialize a new batch.
     - `process_record()` to enqueue a record to be written.
     - `process_batch()` to write any queued records and cleanup local resources.

Note: The `Sink` class can receive records from one stream or from many. See the [Sink documentation](./sinks.md)
for more information on differences between a target's `Sink` class versus a tap's `Stream` class.

## Building a New Tap or Target

First, install [uv](https://docs.astral.sh/uv/), [cookiecutter](https://cookiecutter.readthedocs.io), and optionally [Tox](https://tox.wiki/):

```bash
uv tool install cookiecutter

# Optional: Install Tox if you want to use it to run auto-formatters, linters, tests, etc.
uv tool install tox
```

:::{tip}
The minimum recommended version of cookiecutter is `2.2.0` (released 2023-07-06).
:::

Now you can initialize your new project with the Cookiecutter template for taps:

```bash
cookiecutter https://github.com/meltano/sdk --directory="cookiecutter/tap-template"
```

...or for targets:

```bash
cookiecutter https://github.com/meltano/sdk --directory="cookiecutter/target-template"
```

Note that you do not need to create the directory for the tap.
If you want want `/projects/tap-mytap`, then run the cookiecutter in `/projects` and the `tap-mytap`
project will be created.

Once you've answered the cookiecutter prompts, follow the instructions in the
generated `README.md` file to complete your new tap or target. You can also reference the
[Meltano Tutorial](https://docs.meltano.com/tutorials/custom-extractor) for a more
detailed guide.

````{admonition} Avoid repeating yourself
  If you find yourself repeating the same inputs to the cookiecutter, you can create a
  `cookiecutterrc` file in your home directory to set default values for the prompts.

  For example, if you want to set the default value for your name and email, and the
  default stream type and authentication method, you can add the following to your
  `~/.cookiecutterrc` file:

  ```yaml
  # ~/.cookiecutterrc
  default_context:
    admin_name: Johnny B. Goode
    admin_email: jbg@example.com
    stream_type: REST
    auth_method: Bearer Token
  ```
````

### Application configuration

The SDK lets you define a configuration schema for your tap or target with full support for JSON schema validation. Read the more in-depth guide on [defining a configuration schema](./guides/config-schema.md).

### Using an existing library

In some cases, there may already be a library that connects to the API and all you need the SDK for
is to reformat the data into the Singer specification.
The SDK is still a great choice for this.
The [Peloton tap](https://github.com/MeltanoLabs/tap-peloton) is an example of this.

### RESTful JSONPaths

By default, the Singer SDK for REST streams assumes the API responds with a JSON array of records, but you can easily override this behaviour by specifying the `records_jsonpath` expression in your `RESTStream` or `GraphQLStream` implementation:

```python
class EntityStream(RESTStream):
    """Entity stream from a generic REST API."""
    records_jsonpath = "$.data.records[*]"
```

You can test your JSONPath expressions with the [JSONPath Online Evaluator](https://jsonpath.com/).

#### Nested array example

Many APIs return the records in an array nested inside an JSON object key.

- Response:

  ```json
  {
    "data": {
      "records": [
        { "id": 1, "value": "abc" },
        { "id": 2, "value": "def" }
      ]
    }
  }
  ```

- Expression: `$.data.records[*]`

- Result:

  ```json
  [
    { "id": 1, "value": "abc" },
    { "id": 2, "value": "def" }
  ]
  ```

#### Nested object values example

Some APIs instead return the records as values inside an object where each key is some form of identifier.

- Response:

  ```json
  {
    "data": {
      "1": {
        "id": 1,
        "value": "abc"
      },
      "2": {
        "id": 2,
        "value": "def"
      }
    }
  }
  ```

- Expression: `$.data.*`

- Result:

  ```json
  [
    { "id": 1, "value": "abc" },
    { "id": 2, "value": "def" }
  ]
  ```

## Extra features

The following [extra features](https://packaging.python.org/en/latest/specifications/dependency-specifiers/#extras) are available for the Singer SDK:

- `faker` - Enables the use of [Faker](https://faker.readthedocs.io/en/master/) in [stream maps](stream_maps.md).
- `jwt` - Enables the `OAuthJWTAuthenticator` class for JWT (JSON Web Token) authentication.
- `s3` - Enables AWS S3 as a [BATCH storage](batch.md#the-batch-message).
- `parquet` - Enables as [BATCH encoding](batch.md#encoding).
- `testing` - Pytest dependencies required to use the [Tap & Target Testing Framework](testing.md).

## Resources

### Detailed Class Reference

For a detailed reference, please see the [SDK Reference Guide](./reference.rst)

### Implementation Details

For more information about the SDK's' Singer implementation details, please see the
[SDK Implementation Details](./implementation/index.md) section.

### Code Samples

For a list of code samples solving a variety of different scenarios, please see our
[Code Samples](./code_samples.md) page.

### CLI Samples

For a list of sample CLI commands you can run, [click here](./cli_commands.md).

### Python Tips

We've collected some [Python tips](python_tips.md) which may be helpful for new SDK users.

### IDE Tips

Using the debugger features of your IDE can help you develop and fix bugs easier and faster.
Also using breakpoints is a great way to become familiar with the internals of the SDK itself.

#### VSCode Debugging

Ensure the interpreter you're using in VSCode is set to use the one in the project's virtual environment (usually `.venv` in the project root).
You can change this by using the command palette to go to interpreter settings.
Doing this will also help with autocompletion.


In order to launch your plugin via it's CLI with the built-in debugger, VSCode requires a [Launch configuration](https://code.visualstudio.com/docs/editor/debugging#_launch-configurations).
An example launch configuration, added to your `launch.json`, might be as follows:

```js
{
  // launch.json
  "version": "0.2.0",
  "configurations": [
    {
      "name": "tap-snowflake discovery",
      "type": "python",
      "request": "launch",
      "module": "tap_snowflake.tap",
      "args": ["--config", "config.json", "--discover"],
      "python": "${command:python.interpreterPath}",
      // Set to true to debug third-party library code
      "justMyCode": false,
    }
  ]
}
```

#### PyCharm Debugging

See the JetBrain's [PyCharm documentation](https://www.jetbrains.com/help/pycharm/run-debug-configuration.html) for more detail

To launch the PyCharm debugger you can select "Edit Configuration" in the main menu to open the debugger configuration.
Click "Add new run configuration". Set the script path to the full path to your tap.py and parameters to something like `--config .secrets/config.json`.
You can pass in additional parameters like `--discover` or `--state my_state_file.json` to test the discovery or state workflows.

#### Main Method

The above debugging configurations rely on an equivalent to the following snippet being added to the end of your `tap.py` or `target.py` file:

```python
if __name__ == "__main__":
    TapSnowflake.cli()
```

This is automatically included in the most recent version of the tap and target cookiecutters.

### Testing performance

Moved to a separate page: [Measuring Performance](./guides/performance.md#measuring-performance).
