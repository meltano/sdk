# Deprecation Timeline

This page outlines when various features of the Singer SDK will be removed or changed in a backward
incompatible way, following their deprecation, as indicated in the
[deprecation policy](./release_process.md#deprecation-policy).

## v0.54

- Remove the `PluginBase.config_from_cli_args` method, which has not been used for a while.
- Remove the `Stream.reset_state_progress_markers` method, whose logic was never used at the stream level.
- `JSONLinesBatcher` was moved to `singer_sdk.contrib.batch_encoder_jsonl`. Trying to import it from `singer_sdk.batch` will now raise an `AttributeError`.

## v0.56

### Batch file encoding

`JSONLinesEncoding` and `ParquetEncoding` are thin wrappers over `BaseBatchFileEncoding`
that add no behaviour. They will be removed in v0.56.

```python
# Old (deprecated)
from singer_sdk.helpers._batch import JSONLinesEncoding, ParquetEncoding

# New
from singer_sdk.helpers._batch import BaseBatchFileEncoding

encoding = BaseBatchFileEncoding(format="jsonl")
```

## v0.57

### SQL module reorganization

The `singer_sdk.connectors.sql`, `singer_sdk.sinks.sql`, `singer_sdk.streams.sql`, and
top-level `singer_sdk` module no longer exports SQL classes directly. Import them from
`singer_sdk.sql` instead. The shim modules will be removed in v0.57.

```python
# Old (deprecated)
from singer_sdk import SQLConnector, SQLSink, SQLStream, SQLTap, SQLTarget
from singer_sdk.connectors.sql import SQLConnector
from singer_sdk.sinks.sql import SQLSink
from singer_sdk.streams.sql import SQLStream

# New
from singer_sdk.sql import SQLConnector, SQLSink, SQLStream, SQLTap, SQLTarget
```

See the [migration guide](./guides/consolidate-sql-imports.md) for more information.

## 1.0

- The `RESTStream.get_next_page_token` method will no longer be called
  as part of the stream pagination process. It is replaced by the
  [`RESTStream.get_new_paginator`](singer_sdk.RESTStream.get_new_paginator).

  See the [migration guide](./guides/pagination-classes.md) for more information.

- The `singer_sdk.testing.get_standard_tap_tests` and `singer_sdk.testing.get_standard_target_tests` functions will be removed. Replace them with `singer_sdk.testing.get_tap_test_class` and `singer_sdk.testing.get_target_test_class` functions respective to generate a richer test suite.

- The `PyJWT` and `cryptography` libraries will be no longer be installed by default. If you are using the `OAuthJWTAuthenticator` you will need to install [`singer-sdk[jwt]`](./dev_guide.md#extra-features).
