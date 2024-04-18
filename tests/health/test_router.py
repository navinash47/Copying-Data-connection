from typing import Sequence

from fastapi import FastAPI
from pytest_mock import MockerFixture

from health.router import health_router, readiness_components
from health.models import Health, HealthIndicator, HealthStatus


def test_liveness(client_factory):
    app = FastAPI()
    app.include_router(health_router)
    client = client_factory(app)

    response = client.get('/health/liveness')
    assert response.status_code == 200
    assert response.json() == {
        "status": "UP"
    }


def test_readiness_all_components_up(client_factory, mocker: MockerFixture):
    def component_overrides() -> Sequence[HealthIndicator]:
        component_one = mocker.Mock(HealthIndicator)
        component_one.get_health.return_value = Health(name='c1', status=HealthStatus.UP)
        component_two = mocker.Mock(HealthIndicator)
        component_two.get_health.return_value = Health(name='c2', status=HealthStatus.UP)

        return [component_one, component_two]

    app = FastAPI()
    app.include_router(health_router)
    app.dependency_overrides[readiness_components] = component_overrides

    client = client_factory(app)

    response = client.get('/health/readiness')
    assert response.status_code == 200
    assert response.json() == {
        "status": "UP",
        "components": {
            "c1": {
                "status": "UP"
            },
            "c2": {
                "status": "UP"
            }
        }
    }


def test_readiness_partial_components_up(client_factory, mocker: MockerFixture):
    def component_overrides() -> Sequence[HealthIndicator]:
        component_one = mocker.Mock(HealthIndicator)
        component_one.get_health.return_value = Health(name='c1', status=HealthStatus.UP)
        component_two = mocker.Mock(HealthIndicator)
        component_two.get_health.return_value = Health(name='c2', status=HealthStatus.DOWN)
        component_three = mocker.Mock(HealthIndicator)
        component_three.get_health.return_value = Health(name='c3', status=HealthStatus.UP)

        return [component_one, component_two, component_three]

    app = FastAPI()
    app.include_router(health_router)
    app.dependency_overrides[readiness_components] = component_overrides

    client = client_factory(app)

    response = client.get('/health/readiness')
    assert response.status_code == 503
    assert response.json() == {
        "status": "DOWN",
        "components": {
            "c1": {
                "status": "UP"
            },
            "c2": {
                "status": "DOWN"
            },
            "c3": {
                "status": "UP"
            }
        }
    }
