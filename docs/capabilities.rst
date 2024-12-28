Plugin Capabilities
===================
.. currentmodule:: singer_sdk.helpers.capabilities

Base Capabilities
-------------------

==========================
Built-in base capabilities
==========================

The following capabilities are supported by default.

.. autoattribute:: PluginCapabilities.ABOUT
.. autoattribute:: PluginCapabilities.BATCH
.. autoattribute:: PluginCapabilities.STREAM_MAPS
.. autoattribute:: PluginCapabilities.FLATTENING

========================
Custom base capabilities
========================

The following capabilities have to be implemented in the plugin.

.. autoattribute:: PluginCapabilities.ACTIVATE_VERSION

Tap Capabilities
----------------

.. currentmodule:: singer_sdk.helpers.capabilities

=========================
Built-in tap capabilities
=========================

The following capabilities are supported by default.

.. autoattribute:: TapCapabilities.DISCOVER
.. autoattribute:: TapCapabilities.CATALOG
.. autoattribute:: TapCapabilities.STATE
.. autoattribute:: TapCapabilities.TEST

=======================
Custom tap capabilities
=======================

The following capabilities have to be implemented in the tap.

.. autoattribute:: TapCapabilities.LOG_BASED
.. autoattribute:: TapCapabilities.PROPERTIES

Target Capabilities
-------------------

==========================
Custom target capabilities
==========================

The following capabilities have to be implemented in the target.

.. autoattribute:: TargetCapabilities.SOFT_DELETE
.. autoattribute:: TargetCapabilities.HARD_DELETE
.. autoattribute:: TargetCapabilities.DATATYPE_FAILSAFE
.. autoattribute:: TargetCapabilities.TARGET_SCHEMA
