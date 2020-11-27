"""TapBase abstract class"""

import abc

from tap_base import PluginBase

class TapBase(PluginBase, metaclass=abc.ABCMeta):
    """Abstract base class for taps"""
    
    @abc.abstractmethod
    def get_capabilities():
        pass

    @abc.abstractmethod
    def sync():
        pass
    
    @abc.abstractmethod
    def get_state():
        pass

    @abc.abstractmethod
    def get_state():
        pass
