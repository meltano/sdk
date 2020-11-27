"""TapBase abstract class"""

import abc

from tap_base.PluginBase import PluginBase


class TapBase(PluginBase, metaclass=abc.ABCMeta):
    """Abstract base class for taps"""

    @abc.abstractmethod
    def get_capabilities(self):
        pass

    @abc.abstractmethod
    def sync(self):
        pass

    @abc.abstractmethod
    def get_state(self):
        pass

