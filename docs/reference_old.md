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
  - [`Target` Class](#target-class)
    - [`Target.default_sink_class` Property](#targetdefault_sink_class-property)
    - [`Target.get_sink_class` Method](#targetget_sink_class-method)
    - [`Target.get_sink` Method](#targetget_sink-method)
  - [`Sink` Class](#sink-class)
    - [`Sink.tally_record_written()` Method](#sinktally_record_written-method)
    - [`Sink.tally_duplicate_merged()` Method](#sinktally_duplicate_merged-method)
    - [`Sink.process_record()` Method](#sinkprocess_record-method)
    - [`Sink.drain()` Method](#sinkdrain-method)

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
  - [GitLab GraphQL streams](/singer_sdk/samples/sample_tap_gitlab/gitlab_graphql_streams.py)
- Or the [Countries API](/singer_sdk/samples/sample_tap_countries) Sample:
  - [Countries API Streams](/singer_sdk/samples/sample_tap_countries/countries_streams.py)

## `Target` Class

The `Target` class inherits from PluginBase and is analogous to the `Tap` class.

The `Target` class manages config information and is responsible for processing the
incoming Singer data stream and orchestrating any needed target `Sink` objects. As messages
are received, the `Target` class will automatically create any needed target `Sink` objects
and send records along to the appropriate `Sink` object for that record.

### `Target.default_sink_class` Property

Optionally set a default `Sink` class for this target. This is required if `Target.get_sink_class()` is not defined.

### `Target.get_sink_class` Method

Return the appropriate sink class for the given stream name. This is required if
`Target.default_sink_class` is not defined.

Raises: ValueError if no Sink class is defined.

### `Target.get_sink` Method

For most implementations, this can be left as the default behavior.

In advanced implementations, developers may optionally override this method to return
a different sink object based on the record data.

The default behavior will return a sink object based upon the name of the stream, unless
a new schema message is received, in which case a new sink will be initialized based upon
the updated schema and the old sink will be marked to be drained along with its
already-received records.

## `Sink` Class


### `Sink.tally_record_written()` Method

Increment the records written tally.

This method should be called directly by the Target implementation whenever a record is
confirmed permanently written to the target. This may be called from from within
`Sink.process_record()` or `Sink.drain()`, depending on when the record is permanently written.

### `Sink.tally_duplicate_merged()` Method

If your target merges records based upon duplicates in primary key, you can optionally
use this tally to help end-users reconcile record tallies. If not implemented, warnings
may be logged if records written is less than the number of records loaded after
`Sink.drain()` is completed.

### `Sink.process_record()` Method

This method will be called once per received record.

Targets which prefer to write records in batches should use `Sink.process_record()` to
add the record to an internal buffer or queue, then use `Sink.drain()` to write all
records in the most efficient method.

Targets which prefer to write records one at a time should use `Sink.process_record()` to
permanently store the record. (`Sink.drain()` is then not needed.)

If duplicates are merged, these can optionally be tracked via
`Sink.tally_duplicates_merged()`.

### `Sink.drain()` Method

Drain all loaded records, returning only after records are validated and permanently written
to the target.

Developers should call `Sink.tally_record_written()` here or in `Sink.process_record()`
to confirm total number of records permanently written.

If duplicates are merged, these can optionally be tracked via
`Sink.tally_duplicates_merged()`.
- For more info, see the [GitLab](../singer_sdk/samples/sample_tap_gitlab) sample:
  - [GitLab GraphQL streams](../singer_sdk/samples/sample_tap_gitlab/gitlab_graphql_streams.py)
- Or the [Countries API](../singer_sdk/samples/sample_tap_countries) Sample:
  - [Countries API Streams](../singer_sdk/samples/sample_tap_countries/countries_streams.py)
