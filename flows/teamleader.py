from datetime import datetime
from time import sleep
from typing import Any, Union

import requests
from prefect import get_run_logger, task
from prefect.blocks.system import Secret
from requests import Response

from .database import Connection
from .models import (
    TL_Auth,
    TL_RequestInfo,
    TL_RequestList,
    TL_Response,
    TL_ResponseInfo,
    TL_ResponseList,
    serialize,
)


class TeamleaderRequestException(Exception):
    pass

def save_tokens_to_prefect(auth: TL_Auth):
    """
    Save the access and refresh tokens synchronously to Prefect secrets.
    """
    Secret(value=auth.access_token.get_secret_value()).save(
        name="teamleader-access-token", 
        overwrite=True
    )
    
    Secret(value=auth.refresh_token.get_secret_value()).save(
        name="teamleader-refresh-token", 
        overwrite=True
    )

@task
def refresh_auth_token(conn: Connection, auth: TL_Auth) -> TL_Auth:
    logger = get_run_logger()
    logger.info("Refreshing access token")

    # Check if the database is in a valid state
    # validate_db_auth_state(conn)

    # Refresh the authorization token
    response = requests.post(
        auth.uri + "/access_token",
        data={
            "client_id": auth.client_id,
            "client_secret": auth.client_secret.get_secret_value(),
            "refresh_token": auth.refresh_token.get_secret_value(),
            "grant_type": "refresh_token",
        },
    )
    if response.status_code != 200:
        raise RuntimeError(
            f"Could not refresh token. Status code {response.status_code} - {response.reason}"
        )
    response = response.json()

    auth = TL_Auth(
        uri=auth.uri,
        client_id=auth.client_id,
        client_secret=auth.client_secret,
        refresh_token=response["refresh_token"],  # Does this need a SecretStr() ?
        access_token=response["access_token"],  # idem
    )

    save_tokens_to_prefect(auth)
    logger.info("Updated access token and refresh token in database.")
    return auth


def get_request_headers(auth: TL_Auth):
    return {"Authorization": f"Bearer {auth.access_token.get_secret_value()}"}


def request_teamleader_info(
    req: TL_RequestInfo,
    auth: TL_Auth,
    conn: Connection,
) -> tuple[TL_ResponseInfo, TL_Auth]:
    response, auth = request_teamleader(req, auth, conn)
    if not isinstance(response.data, dict):
        raise TypeError(
            f"Expected a dictionary for TL_ResponseInfo, but got {type(response.data)}"
        )
    return (
        TL_ResponseInfo(
            resource=response.resource,
            ratelimit_remaining=response.ratelimit_remaining,
            ratelimit_reset=response.ratelimit_reset,
            data=response.data,
        ),
        auth,
    )

@task(retries=3, retry_delay_seconds=10)
def request_teamleader_list(
    req: TL_RequestList,
    auth: TL_Auth,
    conn: Connection,
) -> tuple[TL_ResponseList, TL_Auth]:
    response, auth = request_teamleader(req, auth, conn)
    if not isinstance(response.data, list):
        raise TypeError(
            f"Expected a list for TL_ResponseList, but got {type(response.data)}"
        )
    return (
        TL_ResponseList(
            resource=response.resource,
            ratelimit_remaining=response.ratelimit_remaining,
            ratelimit_reset=response.ratelimit_reset,
            data=response.data,
        ),
        auth,
    )


def requests_post(url: str, data: dict[str, Any], headers: dict[str, str]) -> Response:
    return requests.post(url, headers=headers, data=data)


def request_teamleader(
    req: Union[TL_RequestList, TL_RequestInfo],
    auth: TL_Auth,
    conn: Connection,
) -> tuple[TL_Response, TL_Auth]:
    logger = get_run_logger()
    logger.info(f"POST request - {req}")

    data = serialize(req)
    headers = get_request_headers(auth)
    response = requests_post(req.path, headers=headers, data=data)

    if response.status_code == 401:  # Unauthorized
        logger.info(f"{response.status_code} - {response.reason}")
        auth = refresh_auth_token(conn, auth)
        headers = get_request_headers(auth)
        sleep(3)
        response = requests_post(req.path, headers=headers, data=data)

    if response.status_code != 200:
        raise TeamleaderRequestException(
            f"Could not complete teamleader request. Status code {response.status_code} - {response.reason} - {response.text}"
        )

    response = TL_Response(
        resource=req.resource,
        ratelimit_remaining=int(response.headers["X-RateLimit-Remaining"]),
        ratelimit_reset=datetime.fromisoformat(response.headers["X-RateLimit-Reset"]),
        data=response.json()["data"],
    )

    if response.ratelimit_remaining < 5:
        logger.info("Requests rate limit low. Sleeping for 60 seconds...")
        sleep(60)

    return response, auth
