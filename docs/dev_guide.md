# Tap-Base Development Docs

**Development Overview:**

_Create with `tap-base` requires overriding just two classes:_

1. The tap:
    - `TapBase` - _The core base class for all taps. This class governs configuration, validation, and stream discovery._
2. The stream. For the stream base class, you have three options depending on the type of data source you are working with.
    - `TapStreamBase` - _The **generic** base class for streams. This class is responsible for replication and bookmarking._
    - `RESTStreamBase` - _The base class for REST API-base streams. This class is responsible for replication and bookmarking._
    - `DatabaseStreamBase` - _The base class for database-type streams - specifically those which support the SQL language._

**Detailed Instructions:**

1. [Step 1: Initialize a new tap repo](#step-1-initialize-a-new-tap-repo)
2. [Step 2: Write the tap class](#step-2-write-the-tap-class)
3. [Step 3: Write the stream class](#step-3-write-the-stream-class)
   1. ['Generic' stream classes](#generic-stream-classes)
   2. ['API' stream classes](#api-stream-classes)
   3. ['Database' stream classes](#database-stream-classes)

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
2. Override three properties:
   1. `name` - What to call your tap (for example, `tap-bestever`)
   2. `accepted_config_keys` - A lit of all config options that this tap will accept.
   3. `required_config_options` - One or more required sets of options.
   4. `stream_class` - A reference to your stream class (see below)
3. Override the method `discover_catalog_streams`.

**Parquet sample tap class:**

```py
class SampleTapParquet(TapBase):
    """Sample tap for Parquet."""

    name: str = "sample-tap-parquet"
    accepted_config_keys: List[str] = ["filepath", "file_naming_scheme]
    required_config_options: Optional[List[List[str]]] = [["filepath"], ["file_naming_schema"]]

    def discover_catalog_streams(self) -> None:
        """Initialize self._streams with a dictionary of all streams."""
        # TODO: automatically infer this from the parquet schema
        for tap_stream_id in ["ASampleTable"]:
            schema = Schema(
                properties={
                    "f0": Schema(type=["string", "None"]),
                    "f1": Schema(type=["string", "None"]),
                    "f2": Schema(type=["string", "None"]),
                }
            )
            new_stream = SampleTapParquetStream(
                tap_stream_id=tap_stream_id,
                schema=schema,
                state=self.state,
                config=self.config,
            )
            new_stream.primary_keys = ["f0"]
            new_stream.replication_key = "f0"
            self._streams[tap_stream_id] = new_stream
```

## Step 3: Write the stream class

_Creating the stream class depends upon what type of tap you are creating._

### 'Generic' stream classes

_Generic (hand-coded) streams inherit from the class `TapStreamBase`. To create a generic
stream class, you only need to override a single method:_

1. `get_row_generator()` - This method should generate rows and return them incrementally with the
   `yield` python operator.

**An example using the `Parquet` sample:**

```py
class SampleTapParquetStream(TapStreamBase):
    """Sample tap test for parquet."""

    def get_row_generator(self) -> Iterable[dict]:
        """Return a generator of row-type dictionary objects."""
        filepath = self.get_config("filepath")
        if not filepath:
            raise ValueError("Parquet 'filepath' config cannot be blank.")
        try:
            parquet_file = pq.ParquetFile(filepath)
        except Exception as ex:
            raise IOError(f"Could not read from parquet filepath '{filepath}': {ex}")
        for i in range(parquet_file.num_row_groups):
            table = parquet_file.read_row_group(i)
            for batch in table.to_batches():
                for row in zip(*batch.columns):
                    yield {
                        table.column_names[i]: val.as_py()
                        for i, val in enumerate(row, start=0)
                    }
```

### 'API' stream classes

_API streams inherit from the class `RESTStreamBase`. To create an API-based
stream class, you will override one class property and three methods:_

1. **`site_url_base` property** - Returns the base URL, which generally is reflective of a specific API version.
   - For example: to connect to the GitLab v4 API, we use `"https://gitlab.com/api/v4"`.
2. **`get_auth_header` method** - Build and return an authorization header which will be used when
   making calls to your API.
   - For example: to connect to the GitLab API, we pass "Private-Token" and (optionally) "User-Agent".
3. **`get_url` method** - This method returns the concatenates and parameterizes the final URL which
   will be sent to the python `requests` library.
   - For example: in our GitLab example, we pass some config setting along within as URL parameters,
     and then we call to the base class which automatically escapes the URL parameters and
     concatenates our provided URL with the `site_url_base` property we provided earlier.
4. **`post_process` method** - This method gives us an opportunity to "clean up" the results prior
   to returning them to the downstream tap - for instance: cleaning, renaming, or appending the list
   of properties returned by the API.
   - For our GitLab example, no cleansing was necessary and we passed along the result directly as
     received from the API endpoint.

**An example using the `GitLab` sample:**

```py
class GitlabStream(RESTStreamBase):
    """Sample tap test for gitlab."""

    @property
    def site_url_base(self):
        return "https://gitlab.com/api/v4"

    def get_auth_header(self) -> Dict[str, Any]:
        """Return an authorization header for REST API requests."""
        result = {"Private-Token": self.get_config("auth_token")}
        if self.get_config("user_agent"):
            result["User-Agent"] = self.get_config("user_agent")
        return result

    def get_url(self, url_suffix: str = None, extra_url_args: URLArgMap = None) -> str:
        replacement_map = {
            # TODO: Handle multiple projects:
            "project_id": self.get_config("project_id"),
            "start_date": self.get_config("start_date"),
        }
        if extra_url_args:
            replacement_map.update(extra_url_args)
        return super().get_url(url_suffix=url_suffix, extra_url_args=replacement_map)

    def post_process(self, row: dict) -> dict:
        """Transform raw data from HTTP GET into the expected property values."""
        return row
```

### 'Database' stream classes

_Database streams inherit from the class `DatabaseStreamBase`. To create a database
stream class, you will first override the `sql_query()` method. Depending upon how closely your
source complies with standard `information_schema` conventions, you may also override between
one and four class properties, in order to override specific metadata queries._

**All database stream classes override:**

1. `sql_query()` - This method should run a give SQL statement and incrementally return a dictionary
   object for each resulting row.

**Depending upon your implementation, you may also need to override one or more of the following properties:**

1. `table_scan_sql` - A SQL string which should query for all tables, returning three columns: `database_name`, `schema_name`, and `table_name`.
2. `view_scan_sql` - A SQL string which should query for all views, returning three columns: `database_name`, `schema_name`, and `view_name`.
3. `column_scan_sql` - A SQL string which should query for all columns, returning five columns: `database_name`, `schema_name`, and `table_or_view_name`, `column_name`, and `data_type`.

**An example using the `Snowflake` sample:**

`TODO: TK - Snowflake example coming soon...`
