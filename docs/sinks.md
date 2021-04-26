# How to design a Sink

While the default implementation will create one `Sink` per stream, this behavior can be
customized or overridden in a number of ways:

## `1:1` Mapping

This is the default, where only one sink is active for each incoming stream name:

- The exception to this rule is when a new STATE message is received for an
  already-active stream. In this case, the existing sink will be marked to be drained
  and a new sink will be initialized to receive the next incoming records.
- In the case that a sink is archived because of a superseding STATE message, all
  prior version(s) of the stream's sink are guaranteed to be drained in creation order.

### Database sink example

A database-type target where each stream will land in a dedicated table. Each sink is of the same class, with a different target table based on stream_name.

### SaaS sink example

A SaaS-type target where each stream will be uploaded to a different REST endpoint based on stream name. Each sink class is specialized based on the requirements of the target API endpoint.

## `1:many` Mapping

In this scenario, the target intentionally creates multiple sinks per stream:

- The developer may override `Target.get_sink()` and use details within the record (or a
  randomization algorithm) to send records to multiple sinks all corresponding to the same stream.

### Data lake ingestion example

A `1:many` relationship may be used for a data lake target where output files should be pre-partitioned according to
one or more attributes within the record. Importing multiple smaller files, named according to
their partition key values are more efficient than loading fewer larger files.

## `many:1` Mapping

In this scenario, the target intentionally sends all records to the same sink regardless of
stream name:

- The stream name will likely be made an attribute of the
  final output, but records do not need to be segregated by the stream name.

### JSON file writer example

A json file writer where the desired output is a single combined json file with all records from all streams.
