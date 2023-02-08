# Singer Implementation Details

This section documents certain behaviors and expectations of the SDK framework.

```{toctree}
:caption: Implementation Details

cli
discovery
catalog_metadata
record_metadata
metrics
logging
state
at_least_once
```

## How to use the implementation reference material

_**Note:** You should not need to master all of the details here in order
to build your tap, and the behaviors described here should be automatic
and/or intuitive. For general guidance on tap development, please instead refer to our
[Dev Guide](../dev_guide.md)._

The specifications provided in this section are documented primarily to support
advanced use cases, behavior overrides, backwards compatibility with legacy taps,
debugging unexpected behaviors, or contributing back to the SDK itself.
