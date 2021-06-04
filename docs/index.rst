.. Meltano SDK documentation master file, created by
   sphinx-quickstart on Thu Jun  3 14:38:15 2021.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Meltano SDK for Taps and Targets
=======================================

The Tap and Target SDKs are the fastest way to build custom data extractors
and loaders! Taps and targets built on the SDK are automatically compliant with the `Singer Spec`_,
the de-facto open source standard for extract and load pipelines.

Future-proof extractors and loaders, with less code
---------------------------------------------------

On average, developers tell us that they write about 70% less code by using the SDK, which makes
learning the SDK a great investment. Furthermore, as new features and capabilities are added
to the SDK, your taps and targets can always take advantage of the latest capabilities and
bug fixes, simply by updating your SDK dependency to the latest version.


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
.. _Singer Spec: https://meltano.com/docs/singer-spec.html
.. _Meltano: https://www.meltano.com


Index and Search
----------------

* :ref:`genindex`
* :ref:`search`
