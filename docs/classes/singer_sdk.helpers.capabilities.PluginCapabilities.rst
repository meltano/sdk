singer_sdk.helpers.capabilities.PluginCapabilities
==================================================

.. currentmodule:: singer_sdk.helpers.capabilities

.. autoclass:: PluginCapabilities

========
Built-in
========

The following capabilities are supported by default.

.. autoattribute:: PluginCapabilities.ABOUT
.. autoattribute:: PluginCapabilities.STREAM_MAPS

======
Custom
======

The following capabilities have to be implemented in the plugin.

.. autoattribute:: PluginCapabilities.ACTIVATE_VERSION
.. autoattribute:: PluginCapabilities.BATCH
