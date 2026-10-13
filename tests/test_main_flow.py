import pytest
from unittest.mock import MagicMock
from prefect.testing.utilities import prefect_test_harness

from flows.main_flow import main_flow
from flows.models import Resource


@pytest.fixture(autouse=True, scope="session")
def prefect_test_fixture():
    with prefect_test_harness():
        yield


def test_main_flow_orchestration(mocker):
    mocker.patch("flows.main_flow.connect_database")
    mock_create_table = mocker.patch("flows.main_flow.create_teamleader_resource_table")
    mocker.patch("flows.main_flow.get_last_modified_date", return_value="2023-01-01")
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
        db_block_name="etl-harvest",
        resources=[Resource.departments], 
        full_sync=False, 
        return_state=True
    )

    assert state.is_completed(), f"Flow failed with message: {state.message}"
    
    assert mock_create_table.called
    assert mock_upsert.called
    
    assert mock_req_list.call_count == 2
    assert mock_req_info.call_count == 1

def test_failing_resource_fails_the_run(mocker):
    mocker.patch("flows.main_flow.connect_database")
    mocker.patch("flows.main_flow.create_teamleader_resource_table")
    mocker.patch("flows.main_flow.get_last_modified_date", return_value=None)
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
        db_block_name="etl-harvest",
        resources=[Resource.departments, Resource.users],
        return_state=True,
    )

    assert state.is_failed()
    requested = [call.args[0].resource for call in mock_req_list.call_args_list]
    assert Resource.users in requested


def test_table_names_override_default_table(mocker):
    mocker.patch("flows.main_flow.connect_database")
    mock_create_table = mocker.patch("flows.main_flow.create_teamleader_resource_table")
    mocker.patch("flows.main_flow.get_last_modified_date", return_value=None)
    mock_upsert = mocker.patch("flows.main_flow.upsert_into_table")

    page = MagicMock()
    page.data = [{"id": "item_1"}]
    empty_page = MagicMock()
    empty_page.data = []
    mocker.patch("flows.main_flow.request_teamleader_list", side_effect=[page, empty_page, page, empty_page])
    info = MagicMock()
    info.data = {"id": "item_1"}
    mocker.patch("flows.main_flow.request_teamleader_info", return_value=info)

    state = main_flow(
        db_block_name="etl-harvest",
        resources=[Resource.departments, Resource.users],
        table_names={"departments": "tmp_tl_departments"},
        return_state=True,
    )

    assert state.is_completed(), state.message
    created = [call.args[0] for call in mock_create_table.call_args_list]
    assert created == ["tmp_tl_departments", "tl_users"]
    upserted = {call.args[1] for call in mock_upsert.call_args_list}
    assert upserted == {"tmp_tl_departments", "tl_users"}
