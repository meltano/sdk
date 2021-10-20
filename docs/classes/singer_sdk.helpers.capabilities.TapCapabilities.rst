singer_sdk.helpers.capabilities.TapCapabilities
===============================================

.. currentmodule:: singer_sdk.helpers.capabilities

.. autoclass:: TapCapabilities

========
Built-in
========

The following capabilities are supported by default.

.. autoattribute:: TapCapabilities.DISCOVER
.. autoattribute:: TapCapabilities.CATALOG
.. autoattribute:: TapCapabilities.STATE
.. autoattribute:: TapCapabilities.TEST

======
Custom
======

The following capabilities have to be implemented in the tap.

.. autoattribute:: TapCapabilities.LOG_BASED
.. autoattribute:: TapCapabilities.PROPERTIES
