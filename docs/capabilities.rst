Plugin Capabilities
===================
.. currentmodule:: singer_sdk.helpers.capabilities

Base Capabilities
-------------------

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

Tap Capabilities
----------------

.. currentmodule:: singer_sdk.helpers.capabilities

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

Target Capabilities
-------------------

======
Custom
======

The following capabilities have to be implemented in the target.

.. autoattribute:: TargetCapabilities.SOFT_DELETE
.. autoattribute:: TargetCapabilities.HARD_DELETE
.. autoattribute:: TargetCapabilities.DATATYPE_FAILSAFE
.. autoattribute:: TargetCapabilities.RECORD_FLATTENING
.. autoattribute:: TargetCapabilities.TARGET_SCHEMA
