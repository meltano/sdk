# Tap-Base Development Docs

**Development Overview:**

_Developing with `tap-base` requires overriding three classes:_

1. The tap:
    - `TapBase` - _The core base class for taps. This class governs naming, configuration, and core capability mapping._
2. The connection. You have three choices of connection base class when designing your tap:
    - `GenericConnectionBase` - _The base class for generic-type connections. This class is responsible for making a connection to the source, sending queries, and retrieving metadata._
    - `DiscoverableConnectionBase` - _The base class for 'discoverable' connections. Inherits from `GenericConnectionBase` and adds capabilities for stream metadata discovery._
    - `DatabaseConnectionBase` - _The base class for database-type connections. Inherits from `DiscoverableConnectionBase` and sdds specialized functionality for database-type connections._
3. The stream.
    - `TapStreamBase` - _The base class for streams. This class is responsible for replication and bookmarking._

**Detailed Instructions:**

1. [Initializing a new tap repo](#initializing-a-new-tap-repo)
2. [Developing a new tap](#developing-a-new-tap)
   1. [Step 1: Write the tap class](#step-1-write-the-tap-class)
   2. [Step 2: Write the connection class](#step-2-write-the-connection-class)
   3. [Step 3: Write the stream class](#step-3-write-the-stream-class)
3. [Adding more tests](#adding-more-tests)
4. [Troubleshooting Tips](#troubleshooting-tips)

## Initializing a new tap repo

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

## Developing a new tap

### Step 1: Write the tap class

_To create a tap class, follow these steps:_

1. Map your Connection class to the `_conn` type.
2. Override the constructor "`__init__()`" and call the base class constructor.

**Parquet sample tap class:**

```py
class SampleTapParquet(TapBase):
    """Sample tap for Parquet."""

    _conn: SampleParquetConnection

    def __init__(self, config: dict, state: dict = None) -> None:
        """Initialize the tap."""
        vers = Path(PLUGIN_VERSION_FILE).read_text()
        super().__init__(
            plugin_name=PLUGIN_NAME,
            version=vers,
            capabilities=PLUGIN_CAPABILITIES,
            accepted_options=ACCEPTED_CONFIG,
            option_set_requirements=REQUIRED_CONFIG_SETS,
            connection_class=SampleParquetConnection,
            stream_class=SampleTapParquetStream,
            config=config,
            state=state,
        )
```

### Step 2: Write the connection class

To create a generic connection class, follow these steps:

1. Create the `open_connection()` method. This method performs any needed functions to connect to the data source and store a connection handle for future operations.
2. Create the `discover_available_stream_ids()` method. This method returns a list of unique stream IDs.
3. Create the `discover_stream()` method. This method will be called with the inputs provided by the step above.

_**NOTE:**_

- If your source is not discoverable, you can skip the two discover methods.
- If your source is a databases which contains an `information_schema` metadata schema, you may also be able to skip these two methods.

**Parquet sample connection class:**

```py
class SampleParquetConnection(DiscoverableConnectionBase):
    """Parquet Tap Connection Class."""

    _conn: Any

    def open_connection(self) -> Any:
        """Connect to parquet database."""
        self._conn = "placeholder"
        return self._conn

    def discover_available_stream_ids(self) -> List[str]:
        return ["placeholder"]

    def discover_stream(self, tap_stream_id) -> CatalogEntry:
        """Return a list of all streams (tables)."""
        _schema = Schema(
            properties=[
                Schema(description="f0", type=["string", "None"]),
                Schema(description="f1", type=["string", "None"]),
                Schema(description="f2", type=["string", "None"]),
            ]
        )
        return CatalogEntry(
            tap_stream_id=tap_stream_id,
            stream=tap_stream_id,
            key_properties=[],
            schema=_schema,
            replication_key=None,
            is_view=None,
            database=None,
            table=None,
            row_count=None,
            stream_alias=None,
            metadata=None,
            replication_method=None,
        )
```

### Step 3: Write the stream class

_To create a connection class, follow these steps:_

1. Create the `get_row_generator()` method. This method will pass rows from the source connection when a sync is requested.

**Parquet sample stream class:**

```py
class SampleTapParquetStream(TapStreamBase):
    """Sample tap test for parquet."""

    def get_row_generator(self) -> Iterable[Dict[str, Any]]:
        """Return a generator of row-type dictionary objects."""
        filepath = self._conn.get_config("filepath")
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
                        table.column_names[i]: val for i, val in enumerate(row, start=0)
                    }
```

## Adding more tests

`TODO: TK - write test writing instructions`

## Troubleshooting Tips

`TODO: TK - write troubleshooting tips`
