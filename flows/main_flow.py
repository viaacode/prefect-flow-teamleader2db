import json
from functools import partial

from prefect import flow, get_run_logger
from ruamel.yaml import Optional

from .authorization import get_auth_tokens_from_prefect
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
    TL_Auth,
    TL_Client,
    TL_RequestInfo,
    TL_RequestList,
    TL_ResponseInfo,
)
from .teamleader import (
    TeamleaderRequestException,
    request_teamleader_info,
    request_teamleader_list,
)


def format_data_as_db_row(infos: list[TL_ResponseInfo], resource: Resource) -> list[tuple]:
    """
    Prepare the Teamleader responses for upload to the database.
    """
    resource_table_name = Resource.get_db_table_name(resource)
    return [
        (str(info.data["id"]), resource_table_name, json.dumps(info.data)) for info in infos
    ]


def sync_teamleader_resource(
    tl_uri: str,
    resource: Resource,
    full_sync: bool,
    conn: Connection,
    auth: TL_Auth,
) -> TL_Auth:
    """
    Sync a Teamleader resource (e.g. companies, users, contacts, etc.) to the etl_harvest database.

    TL_Auth is returned from this flow because `refresh_auth_token` might have been during the execution of this flow.
    """

    logger = get_run_logger()
    resource_table_name = Resource.get_db_table_name(resource)
    logger.info(f"Starting sync of {resource} to {resource_table_name}")

    create_teamleader_resource_table(resource, conn)

    if full_sync:
        truncate_table(conn, resource_table_name)
        db_last_modified = None
    else:
        db_last_modified = get_last_modified_date(conn, resource_table_name)

    RequestList = partial(
        TL_RequestList,
        base_uri=tl_uri,
        resource=resource,
        updated_since=db_last_modified,
    )

    RequestInfo = partial(
        TL_RequestInfo,
        base_uri=tl_uri,
        resource=resource,
    )

    page = 1
    total = 0
    prev_response = None
    while True:
        req = RequestList(page=page)
        response_list, auth = request_teamleader_list(req, auth, conn)

        if len(response_list.data) == 0:
            break

        if prev_response == response_list.data:
            break

        page += 1
        total += len(response_list.data)
        prev_response = response_list.data

        details = []
        for item in response_list.data:
            req = RequestInfo(id=item["id"])
            info, auth = request_teamleader_info(req, auth, conn)
            details.append(info)

        rows = format_data_as_db_row(details, resource)
        upsert_into_table(conn, resource_table_name, rows)
        logger.info(f"Synced {total} {resource.name} items to {resource_table_name}")

    return auth


@flow(name="prefect_flow_teamleader2db")
def main_flow(
    tl_client_id_block_name: str = "teamleader-client-id",
    tl_client_secret_block_name: str = "teamleader-client-secret",
    db_block_name: str = "etl-harvest",
    tl_api_uri: str = "https://api.focus.teamleader.eu",
    tl_auth_uri: str = "https://focus.teamleader.eu/oauth2",
    resources: Optional[list[Resource]] = None,
    full_sync: bool = False,
):
    """
    Sync all Teamleader resources (companies, users, contracts, etc.) to the etl_harvest database.
    """
    tl_client = TL_Client.load(tl_client_id_block_name, tl_client_secret_block_name)
    conn = connect_database(db_block_name)

    logger = get_run_logger()
    auth = get_auth_tokens_from_prefect(tl_auth_uri, tl_client)
    resources = resources if resources is not None else [r for r in Resource]

    # If a subflow fails, its exception is caught so that subsequent subflows may still execute.
    for resource in resources:
        auth = sync_teamleader_resource(
            tl_uri=tl_api_uri,
            resource=resource,
            full_sync=full_sync,
            conn=conn,
            auth=auth
        )


if __name__ == "__main__":
    main_flow(
        tl_client_id_block_name="teamleader-client-id",
        tl_client_secret_block_name="teamleader-client-secret",
        full_sync=False,
        resources=[
            # Resource.customFieldDefinitions,
            # Resource.invoices,
            Resource.departments,
        ],
    )
