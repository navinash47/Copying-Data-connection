from typing import Dict, Sequence

from fastapi import APIRouter, Depends, status
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from loguru import logger

from helixplatform.service import HelixPlatformHealthIndicator
from opensearch.client import IndexHealthIndicator

from .constants import HealthStatus
from .models import HealthIndicator
from .schemas import ComponentHealthResponse, HealthResponse

health_router = APIRouter(
    prefix="/health",
    include_in_schema=False,
    tags=["health"]
)


def readiness_components() -> Sequence[HealthIndicator]:
    return [
        HelixPlatformHealthIndicator(),
        IndexHealthIndicator()
    ]


@health_router.get("/liveness", status_code=status.HTTP_200_OK, response_model_exclude_none=True)
def liveness() -> HealthResponse:
    return HealthResponse(status=HealthStatus.UP)


@health_router.get("/readiness", response_model=HealthResponse, response_model_exclude_none=True)
def readiness(components: Sequence[HealthIndicator] = Depends(readiness_components)) -> JSONResponse:
    ready = get_readiness_state(components)
    ready_status = status.HTTP_200_OK if ready.status is HealthStatus.UP else status.HTTP_503_SERVICE_UNAVAILABLE
    response = jsonable_encoder(ready)
    logger.info(f'readiness: {ready}')
    return JSONResponse(content=response, status_code=ready_status)


def get_readiness_state(components: Sequence[HealthIndicator]) -> HealthResponse:
    cumulative_status = 0
    component_details: Dict[str, ComponentHealthResponse] = {}

    for component in components:
        component_health = component.get_health()
        cumulative_status += component_health.status.value
        component_details[component_health.name] = ComponentHealthResponse(status=component_health.status)

    readiness_status = HealthStatus.UP if cumulative_status == HealthStatus.UP.value else HealthStatus.DOWN

    return HealthResponse(
        status=readiness_status,
        components=component_details
    )
