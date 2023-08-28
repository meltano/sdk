# Target: Batch Drain

The SDK automatically handles when to send a batch of records to the target.

## The Basics

A Tap sends records messages to the Target.  Those messages are grabbed, decoded, and sent to the Target method `_process_record_message`.  After each record is processed one question is aksed.

1. Is the batch full, `Sink.is_full`

If the answer is `True` then the records currently held in the `Sink._pending_batch` dict are drained.  The drain process is managed by the `Target.drain_one` method.  Records get written, counters get reset, and on to filling the next batch with records.

## How a Batch is Measured and How Is Full Defined

You need to know three things to determine if somthing is full

1. The unit of measure
2. The level at which an object is determined full
3. The current mesurment of the object

The units of measure used with `Sink._pending_batch` are `rows` and `time` in `seconds`.  We gather the full level marks from the Meltano Target configuation options of `batch_size_rows`, `batch_wait_limit_seconds`.  If the configuration options are not present we fall back to the defined constants `MAX_SIZE_DEFAULT` and `WAIT_LIMIT_SECONDS_DEFAULT`.  How do we meaure `Sink._pending_batch`?  To meausre the `rows` the count of records read from the Tap is used.  This is held in the internal variable `Sink._batch_records_read` that is aviable via the property `Sink.current_size`. To measure `seconds` a batch timer is utlized.

There are four full senarios and each one has a function that looks at the batch and returns `True` if it is full or `False` if the batch can take more records.

1. `Rows` limit has been reached. `Sink.is_full_rows`
2. `Wait` limit is `seconds` has been reached. `Sink.is_too_old`
3. `Rows` limit or `Wait` limit is seconds has been reached. `Sink.is_full_rows_and_too_old`
4. `Rows` limit managed by `Sink.sink_timer.counter_based_max_size` has been reached. `Sink.is_full_dynamic`

Based on the Meltano Target configuration options given the function `set_drain_function` places the appropriate function into the internal variable`_batch_drain_fucntion`.  This variable is what is run when the attribute `Sink.is_full` is called at the end of each Target method `_process_record_message` cycle. When `Sink.is_full` is checked at the end of `Target._process_record_message` and returns `True` Then the message "Target sink for 'stream_name' is full. Draining..." is logged to the console and `Target.drain_one()` is called.

## `Rows` limit has been reached. `Sink.is_full_rows`

To know if something is full you need to know how much it currently holds and at what point to consider it full.  Sinks have the property `current_size` which gives you the number of records read and placed into the pending batch. The `current_size` gives us how much the current batch is holding.  Now that we know the `current_size` we need to determing if the current size is considered full.  The Sink property of `max_size` gives us the integer that defines the full mark. The property `max_size` is returns the Sink internal variable `_batch_size_rows` if not `None` or the `DEFAULT_MAX_SIZE` constant which is `10000`.

Both the `Sink.current_size` and `Sink.max_size` are used to calculate the `bool` value returned by the property `Sink.is_full`.  When `Sink.is_full` is checked at the end of `Target._process_record_message` and returns `True` Then the message "Target sink for 'stream_name' is full.  Current size is 'current_size'Draining..." is logged to the console and `Target.drain_one()` is called.

## `Wait` limit is `seconds` has been reached. `Sink.is_too_old`

To know if something is too old you need to know how much time has passed and how much time needs to pass to be considered old.  When the Meltano Target configuration option of `batch_wait_limit_seconds` is present and set the internal variable `_sink_timer` is initialized with an instance of a `BatchPerfTimer`.  The `BatchPerfTimer` Class is a batch specific stop watch.  The timer can be accessable via the property `sink_timer`.  Right after the timer is initalized the stop watch is started `sink_timer.start()`.  We can see how much time has passed by running `sink_timer.on_the_clock()`.  The property `Sink.batch_wait_limit_seconds` hold how much time needs to pass to be considered old.

Both the `sink_timer.on_the_clock()` and `Sink.batch_wait_limit_seconds` are used to calcualte the bool value returned by the function`Sink.is_too_old`.  When `Sink.is_full` is checked at the end of `Target._process_record_message` and returns `True` the message ""Target sink for 'stream_name' is to full. Current size is 'current_size'. Draining..."" is logged to the console and `Target.drain_one(Sink)` is called.  The `Target.drain_one(Sink)` method calls `Sink._lap_manager` which stops the timer, calcuates the lap time, and starts the timer again.

The timer is an instance of `BatchPerfTimer` which is a batch specific stop watch.  When `batch_wait_limits_seconds` is reached on `Sink.sink_timer.on_the_clock()`

## `Rows` limit or `Wait` limit is seconds has been reached. `Sink.is_full_rows_and_too_old`

The previously described `is_full_rows` and `is_too_old` functions are run and their results are held in a tuple.  If `True` is present in the tuple the function returns `True` so `is_full` will return `Ture`.  When `Sink.is_full` is checked at the end of `Target._process_record_message` and returns `True` Then the message "Target sink for 'stream_name' is full.  Current size is 'current_size' Draining..." is logged to the console and `Target.drain_one()` is called.  The `Target.drain_one(Sink)` method calls `Sink._lap_manager` which stops the timer, calcuates the lap time, and starts the timer again.

## `Rows` limit managed by `Sink.sink_timer.counter_based_max_size` has been reached. `Sink.is_full_dynamic`

When the Meltano Target configuration option `batch_dynamic_management` is set to `True` you are asking the `Sink.sink_timer` to find the maxiumum `rows` limit that keeps the time to fill a batch with records and write those records to the Target's target within the `time` in `seconds` given.

The `Sink.sink_timer` is passed the given `batch_size_rows` or the `DEFAULT_MAX_SIZE` constant which is `10000` if it is `None` and is passed the given `batch_wait_limit_seconds` if present or the `WAIT_LIMIT_SECONDS_DEFAULT` constant which is `30` if it is `None`.  Interally the `rows` passed turns into `Sink.sink_timer.SINK_MAX_SIZE_CEILING` which is the max size a batch can reach.  The `time` in `seconds` passed turns into `Sink.sink_timer.max_perf_counter` which is the time in seconds a full cycle should take.  The attribute `Sink.sink_timer.sink_max_size` starts at a predefined size of `100`.  During the `Target.drain_one(Sink)` process `Sink._lap_manager` is called and the timer method `counter_based_max_size` runs and checks if `Sink.sink_timer.perf_diff`, which is the `max_perf_counter` - `lap_time`, is greater than `Sink.sink_timer.perf_diff_allowed_max` or less than `Sink.sink_timer.perf_diff_allowed_min`.  If `Sink.sink_timer.perf_diff` is greater than `Sink.sink_timer.perf_diff_allowed_max` the `Sink.sink_timer.sink_max_size` is increased as long as the `Sink.sink_timer.sink_max_size` is less than `Sink.sink_timer.SINK_MAX_SIZE_CEILING`. If `Sink.sink_timer.perf_diff` is less than `Sink.sink_timer.perf_diff_allowed_min` the `Sink.sink_timer.sink_max_size` is reduced.  If the `Sink.sink_timer.perf_diff` is between `Sink.sink_timer.perf_diff_allowed_max` and `Sink.sink_timer.perf_diff_allowed_min` no correction to `Sink.sink_timer.sink_max_size` is made since the optimal maximum rows has been reached.  This process is repeated when each `Sink` is initalized and starts processing records.