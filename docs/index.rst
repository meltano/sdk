Meltano Singer SDK
==================

Build powerful data extractors and loaders with minimal code. The Singer SDK is the fastest way to create custom data connectors that are automatically compliant with the `Singer Spec`_, the open source standard for ELT pipelines.

Quick Start
-----------

Create your first tap in seconds:

.. code-block:: bash

   cookiecutter https://github.com/meltano/sdk --directory="cookiecutter/tap-template"
   cd tap-myapi  # or whatever you named it
   uv sync

Build a Simple REST API Tap
---------------------------

Create a data extractor in just a few lines of code:

.. code-block:: python

   from singer_sdk import Tap, Stream
   from singer_sdk.streams import RESTStream
   import singer_sdk.typing as th

   class UsersStream(RESTStream):
       """Users stream."""
       name = "users"
       url_base = "https://api.example.com"
       path = "/users"
       primary_keys = ["id"]
       records_jsonpath = "$.data[*]"

       schema = th.PropertiesList(
           th.Property("id", th.IntegerType),
           th.Property("name", th.StringType),
           th.Property("email", th.StringType),
       ).to_dict()

   class MyTap(Tap):
       """My custom tap."""
       name = "tap-myapi"
       config_jsonschema = th.PropertiesList(
           th.Property("api_url", th.StringType, required=True),
           th.Property("api_key", th.StringType, required=True, secret=True),
       ).to_dict()

       def discover_streams(self):
           return [UsersStream(self)]

   if __name__ == "__main__":
       MyTap.cli()

Build a Simple Target
---------------------

Create a data loader to write records to any destination:

.. code-block:: python

   from singer_sdk import Target
   from singer_sdk.sinks import RecordSink

   class MySink(RecordSink):
       """My custom sink."""

       def process_record(self, record: dict, context: dict) -> None:
           """Process a single record."""
           # Your custom logic here
           print(f"Processing: {record}")

   class MyTarget(Target):
       """My custom target."""
       name = "target-mydb"
       config_jsonschema = th.PropertiesList(
           th.Property("host", th.StringType, required=True),
           th.Property("port", th.IntegerType, default=5432),
           th.Property("database", th.StringType, required=True),
           th.Property("username", th.StringType, required=True),
           th.Property("password", th.StringType, required=True, secret=True),
       ).to_dict()
       default_sink_class = MySink

   if __name__ == "__main__":
       MyTarget.cli()

Key Features
------------

- **Less Code**: Focus on your business logic, not boilerplate
- **Auto-Compliant**: Singer Spec compliance built-in
- **Battle-Tested**: Powers millions of pipeline runs monthly
- **Rich Ecosystem**: 600+ existing connectors on `Meltano Hub`_
- **Advanced Features**: Authentication, pagination, incremental sync, batching, and more

Core Classes
------------

The SDK provides these main building blocks:

**Taps (Data Extractors)**
   - ``Tap`` - Base class for all data extractors
   - ``SQLTap`` - For database sources
   - ``RESTStream`` - For REST APIs
   - ``GraphQLStream`` - For GraphQL APIs
   - ``SQLStream`` - For SQL databases

**Targets (Data Loaders)**
   - ``Target`` - Base class for all data loaders
   - ``SQLTarget`` - For database destinations
   - ``RecordSink`` - Process one record at a time
   - ``BatchSink`` - Process records in batches

**Additional Tools**
   - Built-in authentication classes (OAuth, JWT, API Key, etc.)
   - Automatic pagination handling
   - Schema validation and typing
   - Incremental replication support

What's a Tap vs Target?
-----------------------

- **Tap**: Extracts data *from* a source (API, database, file)
- **Target**: Loads data *to* a destination (database, warehouse, file)

They work together in ELT pipelines: ``tap-source | target-destination``

Next Steps
----------

- :doc:`dev_guide` - Complete development guide
- :doc:`code_samples` - Copy-paste examples
- :doc:`guides/index` - In-depth tutorials
- `Meltano Tutorial`_ - End-to-end walkthrough

Built by Meltano and the Singer Community
-----------------------------------------

The SDK is built with love by the Meltano_ core
team and contributors, with contributions from developers across the Singer_
open source community. SDK-based plugins can easily be `integrated with Meltano`_,
but they can also be used in any data pipeline tool that supports the
`Singer Spec`_.

The SDK project is 100% open source, licensed under the `Apache 2.0`_
permissive license. Please visit our code repo `here in GitHub`_, where you can
read the `source code`_, `log an issue or feature request`_, and `contribute back`_
to the SDK. We also regularly discuss SDK topics in our `Meltano Slack`_ community,
within the `#singer-tap-development`_ and `#singer-target-development`_ Slack channels.


.. toctree::
   :caption: Developer Resources
   :maxdepth: 2
   :hidden:

   dev_guide
   guides/index
   Python Tips <python_tips>
   Code Samples <code_samples>
   cli_commands
   faq

.. toctree::
   :caption: Reference
   :maxdepth: 2
   :hidden:

   reference
   implementation/index
   typing
   capabilities

.. toctree::
   :caption: Advanced Concepts
   :maxdepth: 2
   :hidden:

   incremental_replication
   parent_streams
   partitioning
   context_object
   stream_maps
   batch
   sinks
   testing

.. toctree::
   :caption: SDK Development
   :maxdepth: 1
   :hidden:

   CONTRIBUTING
   release_process
   deprecation

.. _Singer: https://singer.io
.. _Singer Spec: https://hub.meltano.com/singer/spec
.. _Meltano: https://www.meltano.com
.. _Meltano EDK: https://edk.meltano.com
.. _Meltano Hub: https://hub.meltano.com/utilities/
.. _Meltano Tutorial: https://docs.meltano.com/tutorials/custom-extractor
.. _integrated with Meltano: https://docs.meltano.com/tutorials/custom-extractor#add-the-plugin-to-your-meltano-project
.. _contribute back: https://github.com/meltano/sdk/contribute
.. _source code: https://github.com/meltano/sdk
.. _log an issue or feature request: https://github.com/meltano/sdk/-/issues
.. _Apache 2.0: https://en.wikipedia.org/wiki/Apache_License
.. _here in GitHub: https://github.com/meltano/sdk
.. _Meltano Slack: https://join.slack.com/t/meltano/shared_invite/zt-obgpdeba-7yrqKhwyMBfdHDXsZY8G7Q
.. _#singer-tap-development: https://meltano.slack.com/archives/C01PKLU5D1R
.. _#singer-target-development: https://meltano.slack.com/archives/C01RKUVUG4S

Index and Search
----------------

* :ref:`genindex`
* :ref:`search`
