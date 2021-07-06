# SDK Dev Guide

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
       flow.

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

First, install [cookiecutter](https://cookiecutter.readthedocs.io) if you haven't
done so already:

```bash
# Install pipx if you haven't already
pip3 install pipx
pipx ensurepath
# Restart your terminal here, if needed, to get the updated PATH
pipx install cookiecutter
```

Now you can initialize your new project with the Cookiecutter template for taps:

```bash
cookiecutter https://gitlab.com/meltano/sdk --directory="cookiecutter/tap-template"
```

...or for targets:

```bash
cookiecutter https://gitlab.com/meltano/sdk --directory="cookiecutter/target-template"
```

Once you've answered the cookiecutter prompts, follow the instructions in the
generated `README.md` file to complete your new tap or target. You can also reference the
[Meltano Tutorial](https://meltano.com/tutorials/create-a-custom-extractor.html) for a more
detailed guide.


### RESTful JSONPaths

By default, the Singer SDK for REST streams assumes the API responds with a JSON array or records, but you can easily override this behaviour by specifying the `records_jsonpath` expression in your `RESTStream` implementation:

```python
class EntityStream(RESTStream):
    """Entity stream from a generic REST API."""
    records_jsonpath = "$.data.records[*]"
```

You can test your JSONPath expressions with the [JSONPath Online Evaluator](https://jsonpath.com/):

#### Nested array example

Many APIs return the records in an array nested inside an JSON object key.

- Object:

    ```json
    {
      "data": {
        "records": [
          {"id": 1, "value": "abc"},
          {"id": 2, "value": "def"}
        ]
      }
    }
    ```

- Expression: `$.data.records[*]`

- Result:

    ```json
    [
      {"id": 1, "value": "abc"},
      {"id": 2, "value": "def"}
    ]
    ```

#### Nested object values example

Some APIs instead return the records as values inside an object where each key is some form of identifier.

- Object:

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
      {"id": 1, "value": "abc"},
      {"id": 2, "value": "def"}
    ]
    ```

## Additional Resources

### Detailed Class Reference

For a detailed reference, please see the [SDK Reference Guide](./reference.md)

### SDK Implementation Details

For more information about the SDK's' Singer implementation details, please see the
[SDK Implementation Details](./implementation/README.md) section.

### Code Samples

For a list of code samples solving a variety of different scenarios, please see our 
[Code Samples](./code_samples.md) page.

### CLI Samples

For a list of sample CLI commands you can run, [click here](./cli_commands.md).

### Python Tips

We've collected some [Python tips](python_tips.md) which may be helpful for new SDK users.
