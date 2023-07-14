# Deprecation Timeline

This page outlines when various features of the Singer SDK will be removed or changed in a backward
incompatible way, following their deprecation, as indicated in the
[deprecation policy](./release_process.md#deprecation-policy).

## 1.0

- The `RESTStream.get_next_page_token` method will no longer be called
  as part of the stream pagination process. It is replaced by the
  [`RESTStream.get_new_paginator`](singer_sdk.RESTStream.get_new_paginator).

  See the [migration guide](./guides/pagination-classes.md) for more information.

- The `singer_sdk.testing.get_standard_tap_tests` and `singer_sdk.testing.get_standard_target_tests` functions will be removed. Replace them with `singer_sdk.testing.get_standard_tap_target_tests` and `singer_sdk.testing.get_standard_tap_target_tests` functions respective to generate a richer test suite.
