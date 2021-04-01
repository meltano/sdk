# Singer SDK Development Docs

**Development Overview:**

_Create with `singer-sdk` requires overriding just two classes:_

1. The tap:
    - `Tap` - _The core base class for all taps. This class governs configuration, validation,
      and stream discovery._
2. The stream. For the stream base class, you have different options depending on the type of data
   source you are working with.
    - `Stream` - _The **generic** base class for streams._
    - `RESTStream` - _The base class for **REST**-type streams._
    - `GraphQLStream` - _The base class for **GraphQL**-type streams._

**Detailed Instructions:**

1. [Step 1: Initialize a new tap repo](#step-1-initialize-a-new-tap-repo)
2. [Step 2: Write the tap class](#step-2-write-the-tap-class)
3. [Step 3: Write the stream class](#step-3-write-the-stream-class)
   1. ['Generic' stream classes](#generic-stream-classes)
   2. ['API' stream classes](#api-stream-classes)
   3. ['GraphQL' stream classes](#graphql-stream-classes)

## Step 1: Initialize a new tap repo

To get started, create a new project from the
[`tap-template` cookiecutter repo](../cookiecutter/tap-template):

1. Install [CookieCutter](https://cookiecutter.readthedocs.io) and it's dependencies:

    ```bash
    pip3 install pipx
    pipx ensurepath
    pipx install cookiecutter
    ```

2. Start a new project:

    ```bash
    cookiecutter https://gitlab.com/meltano/singer-sdk --directory="cookiecutter/tap-template"
    ```

## Step 2: Write the tap class

_To create a tap class, follow these steps:_

1. Override tap config:
   1. `name` - What to call your tap (for example, `tap-best-ever`)
   2. `config_jsonschema` - A JSON Schema object defining the config options that this tap will accept.
2. Override the `discover_streams` method.

## Step 3: Write the stream class

_Creating the stream class depends upon what type of tap you are creating._

### 'Generic' stream classes

_Generic (hand-coded) streams inherit from the class `Stream`. To create a generic
stream class, you only need to override a single method:_

1. `get_records()` - A method which should retrieve data from the source and return records 
incrementally with the `yield` python operator.
    - This method takes an optional `partition` argument, which you can safely disregard 
    unless you require partition handling.

**More info:**

- For more info, see the [Parquet](/singer_sdk/samples/sample_tap_parquet) sample.

### 'REST' stream classes

_REST streams inherit from the class `RESTStream`. To create an REST API-based
stream class, you will override one class property and three methods:_

1. **`url_base` property** - Returns the base URL, which generally is reflective of a 
specific API version.
   - For example: to connect to the GitLab v4 API, we use `"https://gitlab.com/api/v4"`.
2. **`authenticator` property** - Returns an `Authenticator` class to help with connecting
to the source API and negotiating access.
2. **`http_headers` property** - Build and return an authorization header which will be used
when making calls to your API.
   - For example: to connect to the GitLab API, we pass "Private-Token" and (optionally) "User-Agent".

_Depending upon your implementation, you may also want to override one or more of the following properties:_

1. `get_url_params` method - (Optional.) This method returns a map (or list of maps) whose values can be
   substituted into the query URLs. A list of maps is returned if multiple calls need to be made.
   - For example: in our GitLab example, we want the use to be able to specify multiple project IDs
     for extraction so we return multiple maps the URL parameters, each with a different
     `project_id` value.
   - If not provided, the user-provided config dictionary will automatically be scanned for possible
     query parameters.
2. `prepare_request_payload` method - (Optional.) Override this method if your API requires you to use
   submit a payload along with the request.
   - This is generally not needed for REST APIs which use the HTTP GET method.
3. `post_process` method - (Optional.) This method gives us an opportunity to "clean up" the results
   prior to returning them to the downstream tap - for instance: cleaning, renaming, or appending
   the list of properties returned by the API.
   - For our GitLab example, no cleansing was necessary and we passed along the result directly as
     received from the API endpoint.

**More info:**

- For more info, see the [GitLab](/singer_sdk/samples/sample_tap_gitlab) sample:
  - [GitLab tap](/singer_sdk/samples/sample_tap_gitlab/gitlab_tap.py)
  - [GitLab REST streams](singer_sdk/samples/sample_tap_gitlab/gitlab_rest_streams.py)

### 'GraphQL' stream classes

_GraphQL streams inherit from the class `GraphQLStream`._

GraphQL streams are very similar to REST API-based streams, but instead of specifying a `path` and `url_params`, you will override the GraphQL query text:

1. **`query` property** - This is where you specify your specific GraphQL query text.

**More info:**

- For more info, see the [GitLab](/singer_sdk/samples/sample_tap_gitlab) sample:
  - [GitLab tap](/singer_sdk/samples/sample_tap_gitlab/gitlab_tap.py)
  - [GitLab GraphQL streams](/singer_sdk/samples/sample_tap_gitlab/gitlab_rest_streams.py)
- Or the [Countries API](/singer_sdk/samples/sample_tap_countries) Sample:
  - [Countries API Tap](/singer_sdk/samples/sample_tap_countries/countries_tap.py)
  - [Countries API Streams](/singer_sdk/samples/sample_tap_countries/countries_streams.py)

## Singer SDK Implementation Details

For more detailed information about the Singer SDK implementation, please see the 
[Singer SDK Implementation Details](./implementation/README.md) section.
