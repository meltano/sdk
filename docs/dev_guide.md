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
    - `DatabaseStream` - _The base class for **database**-type streams - specifically those
      which support the SQL language._

**Detailed Instructions:**

1. [Step 1: Initialize a new tap repo](#step-1-initialize-a-new-tap-repo)
2. [Step 2: Write the tap class](#step-2-write-the-tap-class)
3. [Step 3: Write the stream class](#step-3-write-the-stream-class)
   1. ['Generic' stream classes](#generic-stream-classes)
   2. ['API' stream classes](#api-stream-classes)
   3. ['GraphQL' stream classes](#graphql-stream-classes)
   4. ['Database' stream classes](#database-stream-classes)

## Step 1: Initialize a new tap repo

To get started, create a new project from the
[`tap-template` cookiecutter repo](https://gitlab.com/meltano/tap-template):

1. Install [CookieCutter](https://cookiecutter.readthedocs.io) and it's dependencies:

    ```bash
    pip3 install pipx
    pipx ensurepath
    pipx install cookiecutter
    ```

2. Start a new project:

    ```bash
    cookiecutter https://gitlab.com/meltano/tap-template
    ```

## Step 2: Write the tap class

_To create a tap class, follow these steps:_

1. Map your Connection class to the `_conn` type.
2. Override tap config:
   1. `name` - What to call your tap (for example, `tap-best-ever`)
   2. `accepted_config_keys` - A lit of all config options that this tap will accept.
   3. `required_config_options` - (Optional.) One or more required sets of options.
3. Override the `discover_streams` method.

## Step 3: Write the stream class

_Creating the stream class depends upon what type of tap you are creating._

### 'Generic' stream classes

_Generic (hand-coded) streams inherit from the class `Stream`. To create a generic
stream class, you only need to override a single method:_

1. **`tap_name`** - The same name used in your tap class (for logging purposes).
2. `records()` - Property should generate records (rows) and return them incrementally with the
   `yield` python operator.

**More info:**

- For more info, see the [Parquet](/singer_sdk/samples/sample_tap_parquet) sample.

### 'REST' stream classes

_REST streams inherit from the class `RESTStream`. To create an REST API-based
stream class, you will override one class property and three methods:_

1. **`tap_name`** - The same name used in your tap class (for logging purposes).
2. **`url_base` property** - Returns the base URL, which generally is reflective of a specific API version.
   - For example: to connect to the GitLab v4 API, we use `"https://gitlab.com/api/v4"`.
3. **`http_headers` property** - Build and return an authorization header which will be used when
   making calls to your API.
   - For example: to connect to the GitLab API, we pass "Private-Token" and (optionally) "User-Agent".

_Depending upon your implementation, you may also want to override one or more of the following properties:_

1. `get_query_params` method - (Optional.) This method returns a map (or list of maps) whose values can be
   substituted into the query URLs. A list of maps is returned if multiple calls need to be made.
   - For example: in our GitLab example, we want the use to be able to specify multiple project IDs
     for extraction so we return multiple maps the URL parameters, each with a different
     `project_id` value.
   - If not provided, the user-provided config dictionary will automatically be scanned for possible
     query parameters.
2. `get_request_payload` method - (Optional.) Override this method if your API requires you to use
   submit a "POST" a query payload along with the request.
   - This is not needed for REST APIs which use the HTTP GET method.
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

_GraphQL streams inherit from the class `GraphQLStream`. GraphQL streams are very similar toREST API-based streams, but instead of a `path`, you will override the GraphQL query text._

1. **`tap_name`** - The same name used in your tap class (for logging purposes).
2. **`url_base` property** - Returns the base URL, which generally is reflective of a specific API version.
   - For example: to connect to the GitLab v4 API, we use `"https://gitlab.com/graphql"`.
3. **`get_http_headers` method** - Build and return an authorization header which will be used when
   making calls to your API.
   - For example: to connect to the GitLab API, we pass "Private-Token" and (optionally) "User-Agent".
4. **`query` property** - This is where you specify your specific GraphQL query text.

_Depending upon your implementation, you may also want to override one or more of the following properties:_

1. **`post_process` method** - (Optional.) This method gives us an opportunity to "clean up" the results prior
   to returning them to the downstream tap - for instance: cleaning, renaming, or appending the list
   of properties returned by the API.
   - For our GitLab example, no cleansing was necessary and we passed along the result directly as
     received from the API endpoint.

**More info:**

- For more info, see the [GitLab](/singer_sdk/samples/sample_tap_gitlab) sample:
  - [GitLab tap](/singer_sdk/samples/sample_tap_gitlab/gitlab_tap.py)
  - [GitLab GraphQL streams](/singer_sdk/samples/sample_tap_gitlab/gitlab_rest_streams.py)
- Or the [Countries API](/singer_sdk/samples/sample_tap_countries) Sample:
  - [Countries API Tap](/singer_sdk/samples/sample_tap_countries/countries_tap.py)
  - [Countries API Streams](/singer_sdk/samples/sample_tap_countries/countries_streams.py)

### 'Database' stream classes

_Database streams inherit from the class `DatabaseStream`. To create a database
stream class, you will first override the `execute_query()` method. Depending upon how closely your
source complies with standard `information_schema` conventions, you may also override between
one and four class properties, in order to override specific metadata queries._

**All database stream classes override:**

1. **`tap_name`** - The same name used in your tap class (for logging purposes).
2. **`execute_query()` method** - This method should run a give SQL statement and incrementally return a dictionary
   object for each resulting row.

_Depending upon your implementation, you may also want to override one or more of the following properties:_

1. `open_connection()` method - (Optional.) Open a connection to the database and return a connection object.
2. `table_scan_sql` - A SQL string which should query for all tables, returning three columns: `database_name`, `schema_name`, and `table_name`.
3. `view_scan_sql` - A SQL string which should query for all views, returning three columns: `database_name`, `schema_name`, and `view_name`.
4. `column_scan_sql` - A SQL string which should query for all columns, returning five columns: `database_name`, `schema_name`, `table_or_view_name`, `column_name`, and `data_type`.
5. `primary_key_scan_sql` - Optional. A SQL string which should query for the list of primary keys, returning five columns: `database_name`, `schema_name`, `table_name`, `pk_column_name`.

**More info:**

- For more info, see the [Snowflake](/singer_sdk/samples/sample_tap_snowflake) sample:
  - [Snowflake tap](/singer_sdk/samples/sample_tap_snowflake/snowflake_tap.py)
  - [Snowflake streams](/singer_sdk/samples/sample_tap_snowflake/snowflake_tap_stream.py)
