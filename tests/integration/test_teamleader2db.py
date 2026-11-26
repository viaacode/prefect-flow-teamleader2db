"""
End-to-end test of main_flow against Postgres and the mock Teamleader API.
Start it with .dev/run.sh, which brings up the Docker containers first.
"""

import os
from datetime import datetime

import psycopg2
import pytest
import requests
from prefect.testing.utilities import prefect_test_harness
from prefect_sqlalchemy import DatabaseCredentials

from flows.blocks.teamleader import TeamleaderCredentials
from flows.main_flow import (
    DatabaseConfig,
    TeamleaderConfig,
    main_flow,
    sync_teamleader_resource,
)
from flows.utils.last_modified import ScopedLastModified
from flows.utils.models import Resource

pytestmark = pytest.mark.skipif(
    not os.getenv("DEV_STACK"), reason="needs the Docker stack from .dev/run.sh"
)

MOCK_URL = "http://localhost:18080"
DB = {
    "host": "localhost",
    "port": 55432,
    "user": "etl",
    "password": "etl",
    "database": "etl_harvest",
}
RESOURCES = [Resource.companies, Resource.departments, Resource.users]


@pytest.fixture(scope="module", autouse=True)
def prefect_blocks():
    with prefect_test_harness():
        DatabaseCredentials(
            driver="postgresql+psycopg2",
            username=DB["user"],
            password=DB["password"],
            host=DB["host"],
            port=DB["port"],
            database=DB["database"],
        ).save("etl-harvest")
        TeamleaderCredentials(
            client_id="dev-client",
            client_secret="dev-secret",
            refresh_token="dev-refresh-token",
        ).save("teamleader-credentials")
        yield


def run_flow(full_sync: bool, last_modified_scoped=None):
    state = main_flow(
        database_config=DatabaseConfig(credentials_block_name="etl-harvest"),
        teamleader_config=TeamleaderConfig(
            api_uri=MOCK_URL,
            auth_uri=f"{MOCK_URL}/oauth2",
            resources=RESOURCES,
        ),
        full_sync=full_sync,
        full_sync_day=datetime.now().day % 28 + 1,
        last_modified_scoped=last_modified_scoped,
        return_state=True,
    )
    assert state.is_completed(), state.message


def query(sql: str, params=()) -> list[tuple]:
    with psycopg2.connect(**DB) as conn, conn.cursor() as curs:
        curs.execute(sql, params)
        return curs.fetchall()


def mock_stats() -> dict:
    return requests.get(f"{MOCK_URL}/_admin/stats").json()


def test_full_sync_copies_all_items():
    run_flow(full_sync=True)

    item_counts = mock_stats()["item_counts"]
    for resource in RESOURCES:
        table = Resource.get_db_table_name(resource)
        [(count,)] = query(f"SELECT count(*) FROM {table}")
        assert count == item_counts[resource.name], table

    [(tl_type, name)] = query(
        "SELECT tl_type, tl_content->>'name' FROM tl_departments ORDER BY tl_uuid LIMIT 1"
    )
    assert tl_type == "tl_departments"
    assert name.startswith("departments ")


def test_new_refresh_token_is_saved():
    refreshes_before = mock_stats()["refresh_count"]
    run_flow(full_sync=True)

    assert mock_stats()["refresh_count"] == refreshes_before + 1
    saved = TeamleaderCredentials.load("teamleader-credentials").refresh_token
    assert saved.get_secret_value() != "dev-refresh-token"


def test_access_token_expiring_mid_run_is_refreshed(monkeypatch):
    monkeypatch.setattr(sync_teamleader_resource, "retries", 0)
    requests.post(f"{MOCK_URL}/_admin/settings", json={"access_token_max_uses": 5})
    try:
        run_flow(full_sync=True)
    finally:
        requests.post(f"{MOCK_URL}/_admin/settings", json={"access_token_max_uses": None})

    item_counts = mock_stats()["item_counts"]
    for resource in RESOURCES:
        table = Resource.get_db_table_name(resource)
        [(count,)] = query(f"SELECT count(*) FROM {table}")
        assert count == item_counts[resource.name], table


def test_incremental_sync_picks_up_changed_items():
    run_flow(full_sync=True)
    [(company_id,)] = query("SELECT tl_uuid::text FROM tl_companies ORDER BY tl_uuid LIMIT 1")
    [(company_count,)] = query("SELECT count(*) FROM tl_companies")
    [(last_modified,)] = query("SELECT max((tl_content->>'updated_at')::timestamptz) FROM tl_companies")

    requests.post(
        f"{MOCK_URL}/_admin/companies/{company_id}", json={"name": "Renamed company"}
    ).raise_for_status()
    run_flow(
        full_sync=False,
        last_modified_scoped=[ScopedLastModified(scope="companies", last_modified=last_modified)],
    )

    [(name,)] = query(
        "SELECT tl_content->>'name' FROM tl_companies WHERE tl_uuid = %s", (company_id,)
    )
    assert name == "Renamed company"
    assert query("SELECT count(*) FROM tl_companies") == [(company_count,)]


