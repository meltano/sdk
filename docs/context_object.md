# The Context Object

Many of the methods in the [Stream](classes/singer_sdk.Stream) class and its subclasses accept
a `context` parameter, which is a dictionary that contains information about the stream
partition or parent stream.

## Best practices for using context

- The context object MUST NOT contain any sensitive information, such as API keys or secrets.
  This is because the context is<br><br>

  1) sent to the target,
  2) stored in the state file and
  3) logged to the console as a tag in metrics and logs.<br><br>

- The context object SHOULD NOT be mutated during the stream's lifecycle. This is because the
  context is stored in the state file, and mutating it will cause the state file to be
  inconsistent with the actual state of the stream.
