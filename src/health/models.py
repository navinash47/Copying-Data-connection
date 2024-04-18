from abc import ABC, abstractmethod
from dataclasses import dataclass
from .constants import HealthStatus


@dataclass
class Health:
    """
    Represents the health of an internal component of the system
    """
    name: str
    status: HealthStatus


class HealthIndicator(ABC):
    """
    Used to contribute Health status of components for the Health endpoint.
    """

    @abstractmethod
    def get_health(self) -> Health:
        """
        Return an indication of component health.
        :param self:
        :return: the component health
        """
        pass
