# [Singer SDK Implementation Details](/.README.md) - Catalog Discovery

All taps developed using the SDK will support `discovery` as a base capability.

## Streams with Static Schema

Streams with static, deterministic schemas can simply override `Stream.schema` with an
appropriate json schema dict. Alternatively, developers can instead also point to a JSON
Schema text file by means of overriding `Stream.schema_filepath`.

## Streams with Dynamic Schema

Streams with dynamic or non-deterministic schemas can either send `schema` to the Stream
constructor or else override `Stream.schema` with a dynamic property method instead of a
static property declaration.

## See Also

- [Singer Spec: Discovery](https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md)
