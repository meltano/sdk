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
pipx install poetry
```

Now you can initialize your new project with the Cookiecutter template for taps:

```bash
cookiecutter https://gitlab.com/meltano/sdk --directory="cookiecutter/tap-template"
```

...or for targets:

```bash
cookiecutter https://gitlab.com/meltano/sdk --directory="cookiecutter/target-template"
```

Note that you do not need to create the directory for the tap.
If you want want `/projects/tap-mytap`, then run the cookiecutter in `/projects` and the `tap-mytap`
project will be created.

Once you've answered the cookiecutter prompts, follow the instructions in the
generated `README.md` file to complete your new tap or target. You can also reference the
[Meltano Tutorial](https://meltano.com/tutorials/create-a-custom-extractor.html) for a more
detailed guide.

### Using an existing library

In some cases, there may already be a library that connects to the API and all you need the SDK for
is to reformat the data into the Singer specification. 
The SDK is still a great choice for this. 
The [Peloton tap](https://github.com/MeltanoLabs/tap-peloton) is an example of this. 

### RESTful JSONPaths

By default, the Singer SDK for REST streams assumes the API responds with a JSON array or records, but you can easily override this behaviour by specifying the `records_jsonpath` expression in your `RESTStream` implementation:

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

### VSCode Tips

Ensure the intrepreter you're using in VSCode is set to use poetry. 
You can change this by using the command pallete to go to intrepeter settings.
Doing this will also help with autocompletion.

### Testing performance

We've had success using [`viztracer`](https://github.com/gaogaotiantian/viztracer) to create flame graphs for SDK-based packages and find if there are any serious performance bottlenecks.

You can start doing the same in your package. Start by installing `viztracer`.

```console
$ poetry add --dev viztracer
```

Then simply run your package's CLI as normal, preceded by the `viztracer` command

```console
$ poetry run viztracer my-tap
```

That command will produce a `result.json` file which you can explore with the `vizviewer` tool.

```console
$ poetry run vizviewer result.json
```

Thet output should look like this

![SDK Flame Graph](https://gitlab.com/meltano/sdk/uploads/07633ba1217de6eb1bb0e018133c608d/_write_record_message.png)

**Note**: Chrome seems to work best for running the `vizviewer` app.

## Additional Resources

More links, resources, and example solutions are available from community
members in the [`#singer-tap-development`](https://meltano.slack.com/archives/C01PKLU5D1R)
and [`#singer-target-development`](https://meltano.slack.com/archives/C01RKUVUG4S)
channels on [Meltano Slack](https://meltano.com/slack).
