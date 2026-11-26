from datetime import datetime
from typing import Any, Optional

from prefect import get_run_logger
from prefect.client.orchestration import get_client
from prefect.runtime import deployment
from prefect.utilities.asyncutils import sync_compatible
from pydantic import BaseModel, Field

PARAMETER_NAME = "last_modified_scoped"


class ScopedLastModified(BaseModel):
    scope: str = Field(
        description="Teamleader resource the last modified date belongs to, e.g. companies."
    )
    last_modified: datetime = Field(
        description="Most recent updated_at of the synced items. The next run only fetches items updated since then."
    )


def get_last_modified_by_scope(
    last_modified_scoped: list[ScopedLastModified], scope: str
) -> Optional[datetime]:
    for scoped in last_modified_scoped:
        if scoped.scope == scope:
            return scoped.last_modified
    return None


@sync_compatible
async def save_last_modified(scope: str, last_modified: datetime):
    """
    Store the last modified date of one scope in the `last_modified_scoped` parameter
    of the running deployment. Does nothing when the flow does not run from a deployment.
    """
    logger = get_run_logger()
    if deployment.id is None:
        logger.info(f"Not running from a deployment, last modified of {scope} is not saved")
        return

    async with get_client() as client:
        current_deployment = await client.read_deployment(deployment.id)
        saved: list[dict[str, Any]] = current_deployment.parameters.get(PARAMETER_NAME) or []
        updated = []
        for entry in saved:
            if entry["scope"] != scope:
                updated.append(entry)
        updated.append({"scope": scope, "last_modified": last_modified.isoformat()})
        current_deployment.parameters[PARAMETER_NAME] = updated
        await client.update_deployment(current_deployment)
    logger.info(f"Saved last modified of {scope}: {last_modified.isoformat()}")


@sync_compatible
async def clear_last_modified():
    """
    Empty the `last_modified_scoped` parameter of the running deployment.
    Does nothing when the flow does not run from a deployment.
    """
    logger = get_run_logger()
    if deployment.id is None:
        logger.info("Not running from a deployment, last modified is not cleared")
        return

    async with get_client() as client:
        current_deployment = await client.read_deployment(deployment.id)
        current_deployment.parameters[PARAMETER_NAME] = []
        await client.update_deployment(current_deployment)
    logger.info("Cleared last modified of all resources")
