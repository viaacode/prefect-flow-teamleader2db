from datetime import datetime
from time import sleep
from typing import Any, Optional, Union

import requests
from prefect import get_run_logger
from pydantic.v1 import SecretStr
from requests import Response

from flows.blocks.teamleader import TeamleaderCredentials

from .models import (
    TeamleaderRequestInfo,
    TeamleaderRequestList,
    TeamleaderResponse,
    TeamleaderResponseInfo,
    TeamleaderResponseList,
    serialize,
)

REQUEST_TIMEOUT_SECONDS = 60


class TeamleaderRequestException(Exception):
    pass


class TeamleaderSession:
    """
    Keeps the Teamleader access token in memory and shares it between the resource tasks.
    The refresh token is stored in the TeamleaderCredentials block.

    https://developer.teamleader.eu/#/introduction/authentication
    """

    def __init__(self, auth_uri: str, credentials_block_name: str = "teamleader-credentials"):
        self.auth_uri = auth_uri
        self.credentials_block_name = credentials_block_name
        self._access_token: Optional[str] = None

    def get_access_token(self) -> str:
        if self._access_token is None:
            self.refresh_access_token()
        return self._access_token

    def refresh_access_token(self):
        logger = get_run_logger()
        logger.info("Refreshing access token")

        credentials = TeamleaderCredentials.load(self.credentials_block_name)
        response = requests.post(
            self.auth_uri + "/access_token",
            data={
                "client_id": credentials.client_id,
                "client_secret": credentials.client_secret.get_secret_value(),
                "refresh_token": credentials.refresh_token.get_secret_value(),
                "grant_type": "refresh_token",
            },
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        if response.status_code != 200:
            raise RuntimeError(
                f"Could not refresh token. Status code {response.status_code} - {response.reason}"
            )
        tokens = response.json()

        credentials.refresh_token = SecretStr(tokens["refresh_token"])
        credentials.save(self.credentials_block_name, overwrite=True)
        self._access_token = tokens["access_token"]
        logger.info("Saved new refresh token to Prefect.")


def get_request_headers(access_token: str):
    return {"Authorization": f"Bearer {access_token}"}


def request_teamleader_info(
    req: TeamleaderRequestInfo,
    session: TeamleaderSession,
) -> TeamleaderResponseInfo:
    response = request_teamleader(req, session)
    if not isinstance(response.data, dict):
        raise TypeError(
            f"Expected a dictionary for TeamleaderResponseInfo, but got {type(response.data)}"
        )
    return TeamleaderResponseInfo(
        resource=response.resource,
        ratelimit_remaining=response.ratelimit_remaining,
        ratelimit_reset=response.ratelimit_reset,
        data=response.data,
    )


def request_teamleader_list(
    req: TeamleaderRequestList,
    session: TeamleaderSession,
) -> TeamleaderResponseList:
    response = request_teamleader(req, session)
    if not isinstance(response.data, list):
        raise TypeError(
            f"Expected a list for TeamleaderResponseList, but got {type(response.data)}"
        )
    return TeamleaderResponseList(
        resource=response.resource,
        ratelimit_remaining=response.ratelimit_remaining,
        ratelimit_reset=response.ratelimit_reset,
        data=response.data,
    )


def requests_post(url: str, data: dict[str, Any], headers: dict[str, str]) -> Response:
    return requests.post(url, headers=headers, data=data, timeout=REQUEST_TIMEOUT_SECONDS)


def request_teamleader(
    req: Union[TeamleaderRequestList, TeamleaderRequestInfo],
    session: TeamleaderSession,
) -> TeamleaderResponse:
    logger = get_run_logger()
    logger.info(f"POST request - {req}")

    data = serialize(req)
    headers = get_request_headers(session.get_access_token())
    response = requests_post(req.path, headers=headers, data=data)

    if response.status_code == 401:  # Unauthorized
        logger.info(f"{response.status_code} - {response.reason}")
        session.refresh_access_token()
        sleep(3)
        headers = get_request_headers(session.get_access_token())
        response = requests_post(req.path, headers=headers, data=data)

    if response.status_code != 200:
        raise TeamleaderRequestException(
            f"Could not complete teamleader request. Status code {response.status_code} - {response.reason} - {response.text}"
        )

    response = TeamleaderResponse(
        resource=req.resource,
        ratelimit_remaining=int(response.headers["X-RateLimit-Remaining"]),
        ratelimit_reset=datetime.fromisoformat(response.headers["X-RateLimit-Reset"]),
        data=response.json()["data"],
    )

    if response.ratelimit_remaining < 5:
        logger.info("Requests rate limit low. Sleeping for 60 seconds...")
        sleep(60)

    return response
