import json
from datetime import datetime
from typing import Optional

from prefect import flow, get_run_logger, task, unmapped
from prefect.task_runners import SequentialTaskRunner

from .database import (
    Connection,
    connect_database,
    create_teamleader_resource_table,
    get_last_modified_date,
    truncate_table,
    upsert_into_table,
)
from .models import (
    Resource,
    TeamleaderRequestInfo,
    TeamleaderRequestList,
    TeamleaderResponseInfo,
)
from .teamleader import (
    TeamleaderSession,
    request_teamleader_info,
    request_teamleader_list,
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
    session: TeamleaderSession,
) -> list[dict]:
    """
    Fetch one page of a Teamleader resource, fetch the details of every item on it
    and upsert them into the database. Returns the items of the list page.
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
    upsert_into_table(conn, Resource.get_db_table_name(resource), rows)
    return response_list.data


@task(retries=3, retry_delay_seconds=10, task_run_name="sync-{resource.name}")
def sync_teamleader_resource(
    tl_uri: str,
    session: TeamleaderSession,
    resource: Resource,
    full_sync: bool,
    db_block_name: str,
):
    """
    Sync a Teamleader resource (e.g. companies, users, contacts, etc.) to the etl_harvest database.
    """
    logger = get_run_logger()
    resource_table_name = Resource.get_db_table_name(resource)
    logger.info(f"Starting sync of {resource} to {resource_table_name}")

    conn = connect_database(db_block_name)
    create_teamleader_resource_table(resource, conn)

    if full_sync:
        truncate_table(conn, resource_table_name)
        db_last_modified = None
    else:
        db_last_modified = get_last_modified_date(conn, resource_table_name)

    page = 1
    total = 0
    prev_items = None
    while True:
        items = sync_page(
            tl_uri=tl_uri,
            resource=resource,
            page=page,
            updated_since=db_last_modified,
            conn=conn,
            session=session,
        )
        if len(items) == 0 or items == prev_items:
            break

        page += 1
        total += len(items)
        prev_items = items
        logger.info(f"Synced {total} {resource.name} items to {resource_table_name}")

    conn.close()


# Sequential: the resource tasks share one Teamleader refresh token, which can only be used once.
@flow(name="prefect_flow_teamleader2db", task_runner=SequentialTaskRunner())
def main_flow(
    db_block_name: str,
    tl_api_uri: str = "https://api.focus.teamleader.eu",
    tl_auth_uri: str = "https://focus.teamleader.eu/oauth2",
    resources: list[Resource] = list(Resource),
    full_sync: bool = False,
):
    """
    Sync all Teamleader resources (companies, users, contracts, etc.) to the etl_harvest database.
    """
    logger = get_run_logger()
    logger.info(f"Starting main flow with resources: {resources} and full_sync={full_sync}")

    futures = sync_teamleader_resource.map(
        tl_uri=unmapped(tl_api_uri),
        session=unmapped(TeamleaderSession(tl_auth_uri)),
        resource=resources,
        full_sync=unmapped(full_sync),
        db_block_name=unmapped(db_block_name),
    )
    for future in futures:
        future.result()


if __name__ == "__main__":
    main_flow(
        full_sync=False,
        resources=[
            # Resource.customFieldDefinitions,
            # Resource.invoices,
            Resource.departments,
        ],
    )
