# Singer SDK Implementation Details

This section documents certain behaviors and expectations of the Singer SDK framework.

1. [CLI](./cli.md)
2. [State](./state.md)
3. [Paritioning](./partitioning.md)

## How to use the implementation reference material

The specifications provided in this section are documented primary to support
advaned use cases, behavior overrides, backwards compatibility with legacy taps,
debugging unexpected behaviors, or contributing back to the SDK itself.

The SDK was designed to significantly reduce the need of developers to deeply 
understand the Singer spec, and the behaviors described here should be automatic
and/or intuitive. You sould not need to master all of the details here in order
to build your tap. For general instructions on development, please instead refer to our
[Dev Guide](../dev_guide.md).
