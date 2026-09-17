import pytest
import responses
from fastapi.testclient import TestClient

from flows.authorization import (
    get_access_token_from_teamleader, 
    app, 
    global_state
)

client = TestClient(app)

########################################################
# Tests voor de Teamleader API aanroep
########################################################

@responses.activate 
def test_get_access_token_success(mocker):
    mock_string = mocker.patch("flows.authorization.String.load") 
    mock_string.return_value.value = "fake_client_id" 
    
    mock_secret = mocker.patch("flows.authorization.Secret.load") 
    mock_secret.return_value.get.return_value = "fake_client_secret" 

    responses.add( 
        responses.POST,
        "https://focus.teamleader.eu/oauth2/access_token",
        json={"access_token": "mock_token_123", "refresh_token": "mock_refresh_456"},
        status=200
    )

    result = get_access_token_from_teamleader("fake_auth_code") 

    assert result["access_token"] == "mock_token_123"
    assert result["refresh_token"] == "mock_refresh_456"

@responses.activate
def test_get_access_token_fails_on_error(mocker):
    mocker.patch("flows.authorization.String.load")
    mocker.patch("flows.authorization.Secret.load")

    responses.add(
        responses.POST,
        "https://focus.teamleader.eu/oauth2/access_token",
        status=401,
        body="Unauthorized"
    )

    with pytest.raises(RuntimeError) as exc_info:
        get_access_token_from_teamleader("bad_code")
    
    assert "Status code 401" in str(exc_info.value)

########################################################
# Tests voor de FastAPI /oauth endpoint (De Voordeur)
########################################################

def test_oauth_endpoint_missing_parameters():
    response = client.get("/oauth")
    
    assert response.status_code == 200
    assert response.json() == {"error": "Either code or state was not given."}

def test_oauth_endpoint_invalid_state():
    global_state["state"] = "echte_veilige_state"
    
    response = client.get("/oauth?code=testcode123&state=verkeerde_state")
    
    assert response.status_code == 200
    assert "do not match which might indicate tampering" in response.json()["error"]

def test_oauth_endpoint_teamleader_error():
    with pytest.raises(PermissionError) as exc_info:
        client.get("/oauth?error=access_denied")
        
    assert "access_denied" in str(exc_info.value)

def test_oauth_endpoint_success_flow(mocker):
    global_state["state"] = "geldige_state_123"
    
    mocker.patch(
        "flows.authorization.get_access_token_from_teamleader",
        return_value={"access_token": "nieuw_access", "refresh_token": "nieuw_refresh"}
    )
    
    mocker.patch("flows.authorization.String.load").return_value.value = "fake_id"
    mocker.patch("flows.authorization.Secret.load").return_value.get.return_value = "fake_secret"
    
    mocker.patch("flows.authorization.save_tokens_to_prefect")
    
    mocker.patch("os.kill")
    
    response = client.get("/oauth?code=geldige_code&state=geldige_state_123")
    
    assert response.status_code == 200
    assert response.json() == {"message": "Success - fetched tokens from Teamleader and saved to prefect"}