"""tap-base library for building singer-compliant taps"""

from tap_base.PluginBase import PluginBase
from tap_base.TapBase import TapBase
from tap_base.TapStreamBase import TapStreamBase

__all__ = ["PluginBase", "TapBase", "TapStreamBase"]
