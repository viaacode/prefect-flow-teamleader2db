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
    mocker.patch("flows.main_flow.TL_Client.load")
    mocker.patch("flows.main_flow.get_auth_tokens_from_prefect", return_value="mocked_auth")
    
    mocker.patch("flows.main_flow.connect_database")
    mock_create_table = mocker.patch("flows.main_flow.create_teamleader_resource_table")
    mocker.patch("flows.main_flow.get_last_modified_date", return_value="2023-01-01")
    mock_upsert = mocker.patch("flows.main_flow.upsert_into_table")

    mock_response_page_1 = MagicMock()
    mock_response_page_1.data = [{"id": "item_1"}]
    
    mock_response_page_2 = MagicMock()
    mock_response_page_2.data = []

    mock_req_list = mocker.patch("flows.main_flow.request_teamleader_list")
    mock_req_list.side_effect = [
        (mock_response_page_1, "mocked_auth"), 
        (mock_response_page_2, "mocked_auth")  
    ]

    mock_info = MagicMock()
    mock_info.data = {"id": "item_1", "name": "Test Department"}
    mock_req_info = mocker.patch("flows.main_flow.request_teamleader_info")
    mock_req_info.return_value = (mock_info, "mocked_auth")

    state = main_flow(
        resources=[Resource.departments], 
        full_sync=False, 
        return_state=True
    )

    assert state.is_completed(), f"Flow failed with message: {state.message}"
    
    assert mock_create_table.called
    assert mock_upsert.called
    
    assert mock_req_list.call_count == 2
    assert mock_req_info.call_count == 1