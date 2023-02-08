# Catalog Discovery

All taps developed using the SDK will automatically support `discovery` as a base
capability, which is the process of generating and emitting a catalog that describes the
available streams and stream types.

The catalog generated is automatically populated by a small number of developer inputs. Most
importantly:

- `Tap.discover_streams()` - Should return a list of available "discovered" streams.
- `Stream.schema` or `Stream.schema_filepath` - The JSON Schema definition of each stream,
provided either directly as a Python `dict` or indirectly as a `.json` filepath.
- `Stream.primary_keys` - a list of strings indicating the primary key(s) of the stream.
- `Stream.replication_key` - a single string indicating the name of the stream's replication
key (if applicable).

## Additional Discovery Mode References

- See the [Dev Guide](../dev_guide.md) and [Code Samples](../code_samples.md) for more
information on working with dynamic stream schemas.
- [Singer Spec: Discovery (meltano.com)](https://hub.meltano.com/singer/spec#discovery-mode)
- [Singer Spec: Discovery (singer-io)](https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md)
