.. Meltano SDK documentation master file, created by
   sphinx-quickstart on Thu Jun  3 14:38:15 2021.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Meltano SDK for Taps and Targets
=======================================

The [Meltano](https://www.meltano.com) SDK for Taps and Targets is the fastest way to build custom 
data extractors and loaders! Taps and targets built on the SDK are automatically compliant with the 
`Singer Spec`_, the de-facto open source standard for extract and load pipelines.

Future-proof extractors and loaders, with less code
---------------------------------------------------

On average, developers tell us that they write about 70% less code by using the SDK, which makes
learning the SDK a great investment. Furthermore, as new features and capabilities are added
to the SDK, your taps and targets can always take advantage of the latest capabilities and
bug fixes, simply by updating your SDK dependency to the latest version.

Build by Meltano and the Singer Community
-----------------------------------------

The SDK is built with love by the <a href="https://meltano.com">Meltano</a> core
team and contributors, with contributions from developers across the <a href="https://singer.io">Singer</a> 
open source community. SDK-based plugins will work great with
[Meltano](https://meltano.com/tutorials/create-a-custom-extractor.html#add-the-plugin-to-your-meltano-project),
but they can also be used in any Singer data pipeline tool which supports the
[Singer Spec](https://hub.meltano.com/singer/spec).

The SDK project is 100% open source, licensed under the [Apache 2.0](https://en.wikipedia.org/wiki/Apache_License)
permissive license. Please visit our code repo [here in Gitlab](https://gitlab.com/meltano/sdk) if you are interested in
[contributing back](https://gitlab.com/meltano/sdk/-/merge_requests?scope=all&state=opened&label_name[]=Accepting%20Merge%20Requests)
reading the [source code](https://gitlab.com/meltano/sdk), or 
[logging an issue or feature request](https://gitlab.com/meltano/sdk/-/issues).

Developer Resources
-------------------

.. toctree::
   :maxdepth: 2

   dev_guide
   python_tips
   code_samples
   cli_commands

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
   porting
   CONTRIBUTING
   implementation/README

.. _Singer: https://singer.io
.. _Singer Spec: https://hub.meltano.com/singer/spec
.. _Meltano: https://www.meltano.com


Index and Search
----------------

* :ref:`genindex`
* :ref:`search`
