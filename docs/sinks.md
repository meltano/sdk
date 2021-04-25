# How to design a Sink

While the default implementation will create one `Sink` per stream, this behavior can be
overwritten in the following ways:

- `1:1`: This is the default, where only one sink is active for each incoming stream name.
  - The exception to this rule is when a knew STATE message is received for an
    already-active stream. In this case, the existing sink will be marked to be flushed
    and a new sink will be initialized to receive the next incoming records.
  - In the case that a sink is archived because of a superseding STATE message, all
    prior version(s) of the stream's sink are guaranteed to be flushed in creation order.
  - _Example: a database-type target where each stream will land in a dedicated table._
- `1:many`: In this scenario, the target intentionally creates multiple sinks per stream.
  The developer may override `Target.get_sink()` and use details within the record (or a
  randomization algorithm) to send records to multiple sinks all for the same stream.
  - _Example: a data lake target where output files should be pre-partitioned according to
    one or more attributes within the record. Multiple smaller files, named according to
    their partition key values are more efficient than fewer larger files._
- `many:1`: In this scenario, the target intentionally sends all records to the same sink,
  regardless of the stream name. The stream name will likely be made an attribute of the
  final output, but records do not need to be segregated by the stream name.
  - _Example: a json file writer where the desired output is a single combined json file
    with all records from all streams._
