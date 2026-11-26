from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest
from prefect.testing.utilities import prefect_test_harness

from flows.main_flow import DatabaseConfig, TeamleaderConfig, main_flow
from flows.utils.last_modified import ScopedLastModified
from flows.utils.models import DEFAULT_TABLE_NAMES, Resource, ResourceTableNames


def not_today() -> int:
    return datetime.now().day % 28 + 1


@pytest.fixture(autouse=True, scope="session")
def prefect_test_fixture():
    with prefect_test_harness():
        yield


@pytest.fixture(autouse=True)
def mock_save_last_modified(mocker):
    return mocker.patch("flows.main_flow.save_last_modified")


def test_main_flow_orchestration(mocker):
    mocker.patch("flows.main_flow.connect_database")
    mock_create_table = mocker.patch("flows.main_flow.create_teamleader_resource_table")
    mock_upsert = mocker.patch("flows.main_flow.upsert_into_table")

    mock_response_page_1 = MagicMock()
    mock_response_page_1.data = [{"id": "item_1"}]
    
    mock_response_page_2 = MagicMock()
    mock_response_page_2.data = []

    mock_req_list = mocker.patch("flows.main_flow.request_teamleader_list")
    mock_req_list.side_effect = [mock_response_page_1, mock_response_page_2]

    mock_info = MagicMock()
    mock_info.data = {"id": "item_1", "name": "Test Department"}
    mock_req_info = mocker.patch("flows.main_flow.request_teamleader_info")
    mock_req_info.return_value = mock_info

    state = main_flow(
        database_config=DatabaseConfig(credentials_block_name="etl-harvest"),
        teamleader_config=TeamleaderConfig(resources=[Resource.departments]),
        full_sync=False,
        full_sync_day=not_today(),
        return_state=True,
    )

    assert state.is_completed(), f"Flow failed with message: {state.message}"
    
    assert mock_create_table.called
    assert mock_upsert.called
    
    assert mock_req_list.call_count == 2
    assert mock_req_info.call_count == 1

def test_failing_resource_fails_the_run(mocker):
    mocker.patch("flows.main_flow.connect_database")
    mocker.patch("flows.main_flow.create_teamleader_resource_table")
    mocker.patch("flows.main_flow.upsert_into_table")
    mocker.patch("flows.main_flow.sync_teamleader_resource.retries", 0)

    empty_page = MagicMock()
    empty_page.data = []

    def fake_list(req, session):
        if req.resource == Resource.departments:
            raise RuntimeError("Teamleader is down")
        return empty_page

    mock_req_list = mocker.patch("flows.main_flow.request_teamleader_list", side_effect=fake_list)

    state = main_flow(
        database_config=DatabaseConfig(credentials_block_name="etl-harvest"),
        teamleader_config=TeamleaderConfig(resources=[Resource.departments, Resource.users]),
        full_sync_day=not_today(),
        return_state=True,
    )

    assert state.is_failed()
    requested = [call.args[0].resource for call in mock_req_list.call_args_list]
    assert Resource.users in requested


def test_table_names_override_default_table(mocker):
    mocker.patch("flows.main_flow.connect_database")
    mock_create_table = mocker.patch("flows.main_flow.create_teamleader_resource_table")
    mock_upsert = mocker.patch("flows.main_flow.upsert_into_table")

    page = MagicMock()
    page.data = [{"id": "item_1"}]
    empty_page = MagicMock()
    empty_page.data = []
    mocker.patch("flows.main_flow.request_teamleader_list", side_effect=[page, empty_page, page, empty_page])
    info = MagicMock()
    info.data = {"id": "item_1"}
    mocker.patch("flows.main_flow.request_teamleader_info", return_value=info)

    table_names = dict(DEFAULT_TABLE_NAMES)
    table_names["departments"] = "tmp_tl_departments"
    state = main_flow(
        database_config=DatabaseConfig(credentials_block_name="etl-harvest", table_names=table_names),
        teamleader_config=TeamleaderConfig(resources=[Resource.departments, Resource.users]),
        full_sync_day=not_today(),
        return_state=True,
    )

    assert state.is_completed(), state.message
    created = [call.args[0] for call in mock_create_table.call_args_list]
    assert created == ["tmp_tl_departments", "tl_users"]
    upserted = {call.args[1] for call in mock_upsert.call_args_list}
    assert upserted == {"tmp_tl_departments", "tl_users"}


def run_departments_sync(mocker, full_sync_day: int):
    mocker.patch("flows.main_flow.connect_database")
    mocker.patch("flows.main_flow.create_teamleader_resource_table")
    mocker.patch("flows.main_flow.upsert_into_table")
    mock_truncate = mocker.patch("flows.main_flow.truncate_table")
    empty_page = MagicMock()
    empty_page.data = []
    mocker.patch("flows.main_flow.request_teamleader_list", return_value=empty_page)

    state = main_flow(
        database_config=DatabaseConfig(credentials_block_name="etl-harvest"),
        teamleader_config=TeamleaderConfig(resources=[Resource.departments]),
        full_sync_day=full_sync_day,
        return_state=True,
    )
    assert state.is_completed(), state.message
    return mock_truncate


def test_full_sync_runs_on_full_sync_day(mocker):
    mock_truncate = run_departments_sync(mocker, full_sync_day=datetime.now().day)
    assert mock_truncate.called


def test_no_full_sync_on_other_days(mocker):
    mock_truncate = run_departments_sync(mocker, full_sync_day=not_today())
    assert not mock_truncate.called


def test_incremental_sync_uses_and_saves_last_modified(mocker, mock_save_last_modified):
    mocker.patch("flows.main_flow.connect_database")
    mocker.patch("flows.main_flow.create_teamleader_resource_table")
    mocker.patch("flows.main_flow.upsert_into_table")

    page = MagicMock()
    page.data = [{"id": "item_1"}, {"id": "item_2"}]
    empty_page = MagicMock()
    empty_page.data = []
    mock_req_list = mocker.patch("flows.main_flow.request_teamleader_list", side_effect=[page, empty_page])
    info_1 = MagicMock()
    info_1.data = {"id": "item_1", "updated_at": "2024-05-01T10:00:00+00:00"}
    info_2 = MagicMock()
    info_2.data = {"id": "item_2", "updated_at": "2024-05-03T10:00:00+00:00"}
    mocker.patch("flows.main_flow.request_teamleader_info", side_effect=[info_1, info_2])

    last_synced = datetime(2024, 4, 1, tzinfo=timezone.utc)
    state = main_flow(
        database_config=DatabaseConfig(credentials_block_name="etl-harvest"),
        teamleader_config=TeamleaderConfig(resources=[Resource.companies]),
        full_sync_day=not_today(),
        last_modified_scoped=[
            ScopedLastModified(scope="companies", last_modified=last_synced),
            ScopedLastModified(scope="users", last_modified=datetime(2020, 1, 1, tzinfo=timezone.utc)),
        ],
        return_state=True,
    )

    assert state.is_completed(), state.message
    assert mock_req_list.call_args_list[0].args[0].updated_since == last_synced
    mock_save_last_modified.assert_called_once_with(
        "companies", datetime(2024, 5, 3, 10, tzinfo=timezone.utc)
    )


def test_full_sync_ignores_last_modified(mocker):
    mock_clear = mocker.patch("flows.main_flow.clear_last_modified")
    mocker.patch("flows.main_flow.connect_database")
    mocker.patch("flows.main_flow.create_teamleader_resource_table")
    mocker.patch("flows.main_flow.truncate_table")
    empty_page = MagicMock()
    empty_page.data = []
    mock_req_list = mocker.patch("flows.main_flow.request_teamleader_list", return_value=empty_page)

    state = main_flow(
        database_config=DatabaseConfig(credentials_block_name="etl-harvest"),
        teamleader_config=TeamleaderConfig(resources=[Resource.companies]),
        full_sync=True,
        last_modified_scoped=[
            ScopedLastModified(scope="companies", last_modified=datetime(2024, 4, 1, tzinfo=timezone.utc)),
        ],
        return_state=True,
    )

    assert state.is_completed(), state.message
    assert mock_clear.called
    assert mock_req_list.call_args_list[0].args[0].updated_since is None


def test_resource_table_names_has_a_field_per_resource():
    assert set(ResourceTableNames.__annotations__) == {resource.name for resource in Resource}
