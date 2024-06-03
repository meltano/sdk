# Using a custom connector class

The Singer SDK has a few built-in connector classes that are designed to work with a variety of sources:

* [`SQLConnector`](../../classes/singer_sdk.SQLConnector) for SQL databases

If you need to connect to a source that is not supported by one of these built-in connectors, you can create your own connector class. This guide will walk you through the process of creating a custom connector class.

## Subclass `BaseConnector`

The first step is to create a subclass of [`BaseConnector`](../../classes/singer_sdk.connectors.BaseConnector). This class is responsible for creating streams and handling the connection to the source.

```python
from singer_sdk.connectors import BaseConnector


class MyConnector(BaseConnector):
    pass
```

## Implement `get_connection`

The [`get_connection`](http://127.0.0.1:5500/build/classes/singer_sdk.connectors.BaseConnector.html#singer_sdk.connectors.BaseConnector.get_connection) method is responsible for creating a connection to the source. It should return an object that implements the [context manager protocol](https://docs.python.org/3/reference/datamodel.html#with-statement-context-managers), e.g. it has `__enter__` and `__exit__` methods.

```python
from singer_sdk.connectors import BaseConnector


class MyConnector(BaseConnector):
    def get_connection(self):
        return MyConnection()
```
