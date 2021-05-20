# SDK Reference Guide

The below reference guide should give an overview of how to use each type of class. **Please note that this is not intended to be a full or exhaustive list of all methods and properties.**

- [SDK Reference Guide](#sdk-reference-guide)
  - [`Tap` Class](#tap-class)
    - [`Tap.name` Property](#tapname-property)
    - [`Tap.config_jsonschema` Property](#tapconfig_jsonschema-property)
    - [`Tap.discover_streams()` Method](#tapdiscover_streams-method)
  - [`Stream` Class](#stream-class)
    - [`Stream.is_sorted` Property](#streamis_sorted-property)
    - [`Stream.get_records()` Method](#streamget_records-method)
    - [`Stream.get_replication_key_signpost()` Method](#streamget_replication_key_signpost-method)
  - [`RESTStream` Class](#reststream-class)
    - [`RESTStream.url_base` Property](#reststreamurl_base-property)
    - [`RESTStream.authenticator` Property](#reststreamauthenticator-property)
    - [`RESTStream.http_headers` Property](#reststreamhttp_headers-property)
    - [`RESTStream.get_url_params()` Method](#reststreamget_url_params-method)
    - [`RESTStream.prepare_request_payload()` Method](#reststreamprepare_request_payload-method)
    - [`RESTStream.post_process()` Method](#reststreampost_process-method)
  - [`GraphQLStream` Class](#graphqlstream-class)
    - [`GraphQL.query` Property](#graphqlquery-property)

## `Tap` Class

The base class for all taps. This class governs configuration, validation,
      and stream discovery.

### `Tap.name` Property

What to call your tap (for example, `tap-best-ever`).

### `Tap.config_jsonschema` Property

A JSON Schema object defining the config options that this tap will accept.

### `Tap.discover_streams()` Method

Initializes the full collection of available streams and returns them as a list.

## `Stream` Class

As noted above, there are different subclasses of Stream class specialized for specific source types.

### `Stream.is_sorted` Property

Set to `True` if stream is sorted. Defaults to `False`.

When `True`, incremental streams will attempt to resume if unexpectedly interrupted.

This setting enables additional checks which may trigger
`InvalidStreamSortException` if records are found which are unsorted.

### `Stream.get_records()` Method

A method which should retrieve data from the source and return records. To optimize performance, should generally returned incrementally using the python `yield` operator. (See the samples or the cookiecutter template for specific examples.)

Note:

- This method takes an optional `context` argument, which can be safely ignored unless
the stream is a child stream or requires [partitioning](./partitioning.md).
- Only custom stream types need to define this method. REST and GraphQL streams do not.
- [Parent streams](./parent_streams.md) can optionally return a tuple, in which case the
  second item in the tuple being a `child_context` dictionary for the stream's `context`.

### `Stream.get_replication_key_signpost()` Method

Return the max allowable bookmark value for this stream's replication key.

For timestamp-based replication keys, this defaults to `utcnow()`. For
non-timestamp replication keys, default to `None`. For consistency in subsequent
calls, the value will be frozen (cached) at its initially called state, per
partition argument if applicable.

Override this value to prevent bookmarks from being advanced in cases where we
may only have a partial set of records.

## `RESTStream` Class

REST streams inherit from the class `RESTStream`. To create an REST API-based
stream class.

### `RESTStream.url_base` Property

Returns the base URL, which generally is reflective of a
specific API version or on-premise deployment.

- For example: to connect to the GitLab v4 API, we use `"https://gitlab.com/api/v4"`.

### `RESTStream.authenticator` Property

Returns an optional `Authenticator` class to help with connecting
to the source API and negotiating access. If an authenticator is not specified, REST-based taps will simply pass `http_headers` as defined in the stream class.

### `RESTStream.http_headers` Property

Return an HTTP request header which will be used
when making calls to your API.

- If authenticator is also specified, the two sets of resulting headers will be combined when making HTTP requests.

### `RESTStream.get_url_params()` Method

Optional. This method returns a map whose values should be passed as URL parameters.

### `RESTStream.prepare_request_payload()` Method

(Optional.) Override this method if your API requires you to submit a payload along with the request.

Note:

- This is generally not needed for REST APIs which use the HTTP GET method.

### `RESTStream.post_process()` Method

Optional. This method gives us an opportunity to "clean up" the results prior to returning them to the downstream tap - for instance: cleaning, renaming, or appending the list of properties returned by the API.

## `GraphQLStream` Class

GraphQL streams inherit from the class `GraphQLStream`, which in turn inherits from the `RESTStream` class. GraphQL streams are very similar to REST API-based streams, but instead of specifying a `path` and `url_params`, you will override the GraphQL query text:

### `GraphQL.query` Property

This is where you specify your specific GraphQL query text.

Examples:

- For more info, see the [GitLab](/singer_sdk/samples/sample_tap_gitlab) sample:
  - [GitLab GraphQL streams](/singer_sdk/samples/sample_tap_gitlab/gitlab_rest_streams.py)
- Or the [Countries API](/singer_sdk/samples/sample_tap_countries) Sample:
  - [Countries API Streams](/singer_sdk/samples/sample_tap_countries/countries_streams.py)
