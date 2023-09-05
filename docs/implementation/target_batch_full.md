# Target: Batch Full

The SDK automatically handles creating and releasing properly sized batches.

## The Basics

A Tap sends records messages to the Target.  Those messages are grabbed, decoded, and sent to the Target method `_process_record_message`.  After each record is processed one question is aksed.

1. Is the batch full, `Sink.is_full`

If the answer is `True` then the records currently held in the `Sink._pending_batch` dict are drained.  The drain process is managed by the `Target.drain_one` method.  Records get written, counters get reset, and on to filling the next batch with records.

## How a Batch is Measured and Full is Defined

You need to know three things to determine if somthing is full

1. The unit of measure
2. The level at which an object is determined full
3. The current mesurment of the object

The units of measure used with `Sink._pending_batch` are **rows** and **time** in **seconds**.  The full level marks come from the Meltano Target configuration options of `batch_size_rows`, `batch_wait_limit_seconds`.  If the configuration options are not present, the defined constants of `MAX_SIZE_DEFAULT` and `WAIT_LIMIT_SECONDS_DEFAULT` are used.  How is `Sink._pending_batch` measured?  To measure **rows** the count of records read from the Tap is used.  The current row count is available via the property `Sink.current_size`. To measure **seconds** a batch timer is utilized.

There are four “is full” scenarios and each one has a function that looks at the batch and returns `True` if it is full or `False` if the batch can take more records.

1. Row limit has been reached. `Sink.is_full_rows`
2. Wait limit is seconds has been reached. `Sink.is_too_old`
3. Row limit or Wait limit is seconds has been reached. `Sink.is_full_rows_and_too_old`
4. Row limit managed by the batch timer has been reached. `Sink.is_full_dynamic`

Based on the Meltano Target configuration option(s) given the function `set_drain_function` places the appropriate function into the internal variable`_batch_drain_fucntion`.  This variable is what is run when the attribute `Sink.is_full` is called at the end of each Target method `_process_record_message` cycle. When `Sink.is_full` is checked and returns `True` the message:
```
"Target sink for [Stream Name] is full. Current size is [Current Size]. Draining...",
```
is logged to the console and `Target.drain_one(Sink)` is called.

## Explanation of the Four "is full" Scenarios

### Rows limit has been reached. `Sink.is_full_rows`

To know if something is full you need to know how much it currently holds and at what point to consider it full.  Sinks have the property `current_size` which gives you the number of records read and placed into the pending batch. The `current_size` gives us how much the current batch is holding.  Now that we know the `current_size` we need to determine if the current size is considered full.  The Sink property of `max_size` gives us the integer that defines the full mark. The property `max_size` returns the Sink’s internal variable `_batch_size_rows` if not `None` or the `DEFAULT_MAX_SIZE` constant which is `10000`.

Both the `Sink.current_size` and `Sink.max_size` are used to calculate the `bool` value returned by the property `Sink.is_full`.  If `Sink.current_size` is greater than or equal to `Sink.max_size` the batch is full and `Sink.is_full` would return `True`.  When `Sink.is_full` is checked at the end of `Target._process_record_message` and returns `True` the message "Target sink for 'stream_name' is full.  Current size is 'current_size'Draining..." is logged to the console and `Target.drain_one(Sink)` is called.

### Wait limit in seconds has been reached. `Sink.is_too_old`

To know if something is too old you need to know how much time has passed and how much time needs to pass to be considered old.  When the Meltano Target configuration option of `batch_wait_limit_seconds` is present and set the internal variable `_sink_timer` is initialized with an instance of a `BatchPerfTimer`.  The `BatchPerfTimer` Class is a batch specific stop watch.  The timer is accessible via the property `Sink.sink_timer`.  Right after the timer is initialized the stop watch is started `Sink.sink_timer.start()`.  We can see how much time has passed by running `Sink.sink_timer.on_the_clock()`.  The property `Sink.batch_wait_limit_seconds` hold how much time needs to pass to be considered old.

Both the `Sink.sink_timer.on_the_clock()` and `Sink.batch_wait_limit_seconds` are used to calculate the `bool` value returned by the function`Sink.is_too_old`.   If `Sink.sink_timer.on_the_clock()` is greater than or equal to `Sink.batch_wait_limit_seconds`  the batch is full and `Sink.is_full` would return `True`. When `Sink.is_full` is checked at the end of `Target._process_record_message` and returns `True` the message ""Target sink for 'stream_name' is to full. Current size is 'current_size'. Draining..."" is logged to the console and `Target.drain_one(Sink)` is called.  The `Target.drain_one(Sink)` method calls `Sink._lap_manager` which stops the timer, calculates the lap time, and starts the timer again.

### Rows or Wait limit has been reached. `Sink.is_full_rows_and_too_old`

The previously described `is_full_rows` and `is_too_old` functions are run and their results are held in a tuple.  If `True` is present in the tuple the function returns `True` so `is_full` will return `True`.  When `Sink.is_full` is checked at the end of `Target._process_record_message` and returns `True` Then the message "Target sink for 'stream_name' is full.  Current size is 'current_size' Draining..." is logged to the console and `Target.drain_one()` is called.  The `Target.drain_one(Sink)` method calls `Sink._lap_manager` which stops the timer, calculates the lap time, and starts the timer again.

### Rows limit managed by `Sink.sink_timer.counter_based_max_size` has been reached. `Sink.is_full_dynamic`

When the Meltano Target configuration option `batch_dynamic_management` is set to `True` you are asking the `Sink.sink_timer` to find the maximum rows is full mark that keeps the time to fill a batch with records and write those records to the Target's target within the time in seconds given.

The `Sink.sink_timer` is passed the given `batch_size_rows` or the `DEFAULT_MAX_SIZE` constant which is `10000` if it is `None` and is also passed the given `batch_wait_limit_seconds` if present or the `WAIT_LIMIT_SECONDS_DEFAULT` constant which is `30` if it is `None`.  Internally the `rows` passed turns into `Sink.sink_timer.SINK_MAX_SIZE_CEILING` which is the max size a batch can reach.  The `time` in `seconds` passed turns into `Sink.sink_timer.max_perf_counter` which is the time in seconds a full cycle should take.  The attribute `Sink.sink_timer.sink_max_size` starts at a predefined size of `100`.  During the `Target.drain_one(Sink)` process `Sink._lap_manager` is called and the timer method `counter_based_max_size` runs and checks if `Sink.sink_timer.perf_diff`, which is `max_perf_counter` - `lap_time`, is greater than `Sink.sink_timer.perf_diff_allowed_max` or less than `Sink.sink_timer.perf_diff_allowed_min`.  If `Sink.sink_timer.perf_diff` is greater than `Sink.sink_timer.perf_diff_allowed_max` the `Sink.sink_timer.sink_max_size` is increased as long as the `Sink.sink_timer.sink_max_size` is less than `Sink.sink_timer.SINK_MAX_SIZE_CEILING`. If `Sink.sink_timer.perf_diff` is less than `Sink.sink_timer.perf_diff_allowed_min` the `Sink.sink_timer.sink_max_size` is reduced.  If the `Sink.sink_timer.perf_diff` is between `Sink.sink_timer.perf_diff_allowed_max` and `Sink.sink_timer.perf_diff_allowed_min` no correction to `Sink.sink_timer.sink_max_size` is made since the optimal rows size has been reached.  This process is repeated when each `Sink` is initialized and starts processing records.
