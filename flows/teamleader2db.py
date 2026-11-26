import json
from datetime import datetime
from typing import Optional

from prefect import flow, get_run_logger, task
from prefect.client.orchestration import get_client
from prefect.runtime import flow_run
from prefect.task_runners import SequentialTaskRunner
from prefect.utilities.asyncutils import sync_compatible
from pydantic import BaseModel, Field

from flows.utils.database import (
    Connection,
    connect_database,
    create_teamleader_resource_table,
    truncate_table,
    upsert_into_table,
)
from flows.utils.last_modified import (
    ScopedLastModified,
    clear_last_modified,
    get_last_modified_by_scope,
    save_last_modified,
)
from flows.utils.models import (
    DEFAULT_TABLE_NAMES,
    Resource,
    ResourceTableNames,
    TeamleaderRequestInfo,
    TeamleaderRequestList,
    TeamleaderResponseInfo,
)
from flows.utils.teamleader import (
    TeamleaderSession,
    request_teamleader_info,
    request_teamleader_list,
)


class DatabaseConfig(BaseModel):
    credentials_block_name: str = Field(
        description="Prefect block name for the database credentials."
    )
    table_names: ResourceTableNames = Field(
        default=DEFAULT_TABLE_NAMES,
        description="Database table to write each Teamleader resource to.",
    )


class TeamleaderConfig(BaseModel):
    credentials_block_name: str = Field(
        default="teamleader-credentials",
        description="Prefect block name for the Teamleader credentials.",
    )
    api_uri: str = Field(
        default="https://api.focus.teamleader.eu",
        description="Base URI of the Teamleader API.",
    )
    auth_uri: str = Field(
        default="https://focus.teamleader.eu/oauth2",
        description="Base URI of the Teamleader OAuth2 server.",
    )
    resources: list[Resource] = Field(
        default=list(Resource),
        description="Teamleader resources to sync, executed sequentially.",
    )


def format_data_as_db_row(infos: list[TeamleaderResponseInfo], resource: Resource) -> list[tuple]:
    """
    Prepare the Teamleader responses for upload to the database.
    """
    resource_table_name = Resource.get_db_table_name(resource)
    return [
        (str(info.data["id"]), resource_table_name, json.dumps(info.data)) for info in infos
    ]


def sync_page(
    tl_uri: str,
    resource: Resource,
    page: int,
    updated_since: Optional[datetime],
    conn: Connection,
    table_name: str,
    session: TeamleaderSession,
) -> tuple[list[dict], list[TeamleaderResponseInfo]]:
    """
    Fetch one page of a Teamleader resource, fetch the details of every item on it
    and upsert them into the database. Returns the items of the list page and their details.
    """
    req = TeamleaderRequestList(
        base_uri=tl_uri,
        resource=resource,
        page=page,
        updated_since=updated_since,
    )
    response_list = request_teamleader_list(req, session)

    details = []
    for item in response_list.data:
        req = TeamleaderRequestInfo(base_uri=tl_uri, resource=resource, id=item["id"])
        info = request_teamleader_info(req, session)
        details.append(info)

    rows = format_data_as_db_row(details, resource)
    upsert_into_table(conn, table_name, rows)
    return response_list.data, details


def get_newest_updated_at(
    infos: list[TeamleaderResponseInfo], newest: Optional[datetime]
) -> Optional[datetime]:
    """
    The most recent `updated_at` of the given items and `newest`.
    """
    for info in infos:
        value = info.data.get("updated_at")
        if value is None:
            continue
        updated_at = datetime.fromisoformat(value)
        if newest is None or updated_at > newest:
            newest = updated_at
    return newest


@task(retries=3, retry_delay_seconds=10, task_run_name="sync-{resource.name}")
def sync_teamleader_resource(
    tl_uri: str,
    session: TeamleaderSession,
    resource: Resource,
    full_sync: bool,
    updated_since: Optional[datetime],
    db_block_name: str,
    table_name: str,
):
    """
    Sync a Teamleader resource (e.g. companies, users, contacts, etc.) to the etl_harvest database.
    """
    logger = get_run_logger()
    if updated_since is None:
        logger.info(f"Starting sync of all {resource.name} items to {table_name}")
    else:
        logger.info(
            f"Starting sync of {resource.name} items updated since {updated_since.isoformat()} to {table_name}"
        )

    conn = connect_database(db_block_name)
    create_teamleader_resource_table(table_name, conn)

    if full_sync:
        truncate_table(conn, table_name)

    page = 1
    total = 0
    prev_items = None
    newest_updated_at = None
    while True:
        items, details = sync_page(
            tl_uri=tl_uri,
            resource=resource,
            page=page,
            updated_since=updated_since,
            conn=conn,
            table_name=table_name,
            session=session,
        )
        if len(items) == 0 or items == prev_items:
            break

        page += 1
        total += len(items)
        prev_items = items
        newest_updated_at = get_newest_updated_at(details, newest_updated_at)
        logger.info(f"Synced {total} {resource.name} items to {table_name}")

    conn.close()

    if newest_updated_at is not None:
        save_last_modified(resource.name, newest_updated_at)


@sync_compatible
async def add_flow_run_tag(tag: str):
    tags = list(flow_run.tags)
    tags.append(tag)
    async with get_client() as client:
        await client.update_flow_run(flow_run.id, tags=tags)


@flow(name="prefect_flow_teamleader2db", task_runner=SequentialTaskRunner())
def teamleader2db_flow(
    database_config: DatabaseConfig,
    teamleader_config: TeamleaderConfig,
    full_sync: bool = False,
    full_sync_day: int = 1,
    last_modified_scoped: Optional[list[ScopedLastModified]] = None,
):
    """
    Sync Teamleader resources (companies, users, contracts, etc.) to the etl_harvest database.
    Runs a full sync when `full_sync` is set or on day `full_sync_day` of the month.
    Otherwise only items updated since their resource's date in `last_modified_scoped` are synced.
    """
    logger = get_run_logger()
    logger.info("Flow started")

    full_sync = full_sync or datetime.now().day == full_sync_day
    if full_sync:
        add_flow_run_tag("full_sync")
        clear_last_modified()

    if last_modified_scoped is None:
        last_modified_scoped = []

    logger.info(f"Syncing resources: {teamleader_config.resources}, full_sync={full_sync}")

    session = TeamleaderSession(
        auth_uri=teamleader_config.auth_uri,
        credentials_block_name=teamleader_config.credentials_block_name,
    )
    for resource in teamleader_config.resources:
        if full_sync:
            updated_since = None
        else:
            updated_since = get_last_modified_by_scope(last_modified_scoped, resource.name)
        sync_teamleader_resource.submit(
            tl_uri=teamleader_config.api_uri,
            session=session,
            resource=resource,
            full_sync=full_sync,
            updated_since=updated_since,
            db_block_name=database_config.credentials_block_name,
            table_name=database_config.table_names[resource.name],
        )
