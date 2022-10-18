.. Meltano Singer SDK documentation master file, created by
   sphinx-quickstart on Thu Jun  3 14:38:15 2021.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Meltano Singer SDK
==================

The Meltano_ Singer SDK for Taps and Targets is the fastest way to build custom
data extractors and loaders! Taps and targets built on the SDK are automatically compliant with the
`Singer Spec`_, the de-facto open source standard for extract and load pipelines.

Future-proof extractors and loaders, with less code
---------------------------------------------------

On average, developers tell us that they write about 70% less code by using the SDK, which makes
learning the SDK a great investment. Furthermore, as new features and capabilities are added
to the SDK, your taps and targets can always take advantage of the latest capabilities and
bug fixes, simply by updating your SDK dependency to the latest version.

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

Developer Resources
-------------------

.. toctree::
   :maxdepth: 2

   dev_guide
   python_tips
   code_samples
   cli_commands
   capabilities
   faq

Reference
---------

.. toctree::
   :maxdepth: 2

   reference


Advanced Topics
---------------

.. toctree::
   :maxdepth: 1

   parent_streams
   partitioning
   context_object
   stream_maps
   batch
   porting
   sinks
   CONTRIBUTING
   implementation/index
   typing

.. _Singer: https://singer.io
.. _Singer Spec: https://hub.meltano.com/singer/spec
.. _Meltano: https://www.meltano.com
.. _integrated with Meltano: https://docs.meltano.com/tutorials/custom-extractor#add-the-plugin-to-your-meltano-project
.. _contribute back: https://github.com/meltano/sdk/issues?q=is%3Aopen+is%3Aissue+label%3A%22accepting+merge+requests%22
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
