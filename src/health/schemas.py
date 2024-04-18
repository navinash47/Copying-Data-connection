from pydantic import BaseModel
from typing import Dict, Optional, Sequence
from .constants import HealthStatus


class ComponentHealthResponse(BaseModel):
    status: HealthStatus


class HealthResponse(BaseModel):
    status: HealthStatus
    components: Dict[str, ComponentHealthResponse] | None

    class Config:
        json_encoders = {HealthStatus: lambda h: h.name}