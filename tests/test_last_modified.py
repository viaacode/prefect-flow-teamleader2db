import asyncio
from datetime import datetime, timezone

import pytest
from prefect import flow
from prefect.client.orchestration import get_client
from prefect.testing.utilities import prefect_test_harness

from flows.utils.last_modified import clear_last_modified, save_last_modified


@pytest.fixture(autouse=True, scope="module")
def prefect_test_fixture():
    with prefect_test_harness():
        yield


@flow
def save_flow(scope: str, last_modified: datetime):
    save_last_modified(scope, last_modified)


@flow
def clear_flow():
    clear_last_modified()


async def create_deployment() -> str:
    async with get_client() as client:
        flow_id = await client.create_flow_from_name("save-flow")
        deployment_id = await client.create_deployment(
            flow_id=flow_id,
            name="test",
            parameters={"last_modified_scoped": [{"scope": "users", "last_modified": "2020-01-01T00:00:00+00:00"}]},
        )
    return str(deployment_id)


async def read_parameter(deployment_id: str):
    async with get_client() as client:
        deployment = await client.read_deployment(deployment_id)
    return deployment.parameters["last_modified_scoped"]


def test_save_and_clear_last_modified_in_deployment(monkeypatch):
    deployment_id = asyncio.run(create_deployment())
    monkeypatch.setenv("PREFECT__RUNTIME__DEPLOYMENT__ID", deployment_id)

    save_flow("companies", datetime(2024, 5, 1, tzinfo=timezone.utc))
    save_flow("companies", datetime(2024, 5, 3, tzinfo=timezone.utc))

    assert asyncio.run(read_parameter(deployment_id)) == [
        {"scope": "users", "last_modified": "2020-01-01T00:00:00+00:00"},
        {"scope": "companies", "last_modified": "2024-05-03T00:00:00+00:00"},
    ]

    clear_flow()
    assert asyncio.run(read_parameter(deployment_id)) == []


def test_save_last_modified_without_deployment_does_nothing():
    state = save_flow("companies", datetime(2024, 5, 1, tzinfo=timezone.utc), return_state=True)
    assert state.is_completed(), state.message
