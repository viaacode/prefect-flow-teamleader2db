from datetime import datetime
from enum import Enum
from typing import Any

from prefect.blocks.system import Secret, String
from pydantic import BaseModel, Field, SecretStr, field_serializer


class Harvest_Tables:
    """
    The name of the tables in the ETL harvest database.
    """

    tl_companies = "tl_companies"
    tl_contacts = "tl_contacts"
    tl_custom_fields = "tl_custom_fields"
    tl_departments = "tl_departments"
    tl_events = "tl_events"
    tl_invoices = "tl_invoices"
    tl_oauth = "tl_oauth"
    tl_projects = "tl_projects"
    tl_users = "tl_users"


class Resource(Enum):
    """
    A Teamleader resource as defined in their [developer documentation](https://developer.teamleader.eu/).
    Names should be copied exactly.
    """

    companies = "companies"
    contacts = "contacts"
    invoices = "invoices"
    departments = "departments"
    events = "events"
    projects = "projects"
    users = "users"
    customFieldDefinitions = "customFieldDefinitions"

    @classmethod
    def get_db_table_name(cls, resource: "Resource") -> str:
        """
        Maps the Teamleader resource on a database table name.
        """
        mapping = {
            Resource.companies: Harvest_Tables.tl_companies,
            Resource.contacts: Harvest_Tables.tl_contacts,
            Resource.invoices: Harvest_Tables.tl_invoices,
            Resource.departments: Harvest_Tables.tl_departments,
            Resource.events: Harvest_Tables.tl_events,
            Resource.projects: Harvest_Tables.tl_projects,
            Resource.users: Harvest_Tables.tl_users,
            Resource.customFieldDefinitions: Harvest_Tables.tl_custom_fields,
        }
        return mapping[resource]


class TL_Client(BaseModel):
    """
    Teamleader client credentials. Client refers here to a specific application e.g. Teamleader2db.
    """

    client_id: str
    client_secret: SecretStr

    @staticmethod
    def load(id_block_name: str, secret_block_name: str):
        id: String = String.load(id_block_name)
        secret: Secret = Secret.load(secret_block_name)

        return TL_Client(
            client_id=id.value,
            client_secret=SecretStr(secret.value.get_secret_value()),
        )


class TL_Auth(BaseModel):
    """

    https://developer.teamleader.eu/#/introduction/authentication
    """

    uri: str = Field(default="https://focus.teamleader.eu/oauth2")
    client_id: str
    client_secret: SecretStr
    refresh_token: SecretStr
    access_token: SecretStr


class TL_RequestInfo(BaseModel):
    """
    A Teamleader request for info on a given resource and id
    """

    base_uri: str = Field(default="https://api.focus.teamleader.eu")
    resource: Resource
    id: str

    @property
    def path(self):
        return self.base_uri + "/" + self.resource.name + ".info"


class TL_RequestList(BaseModel):
    """
    A Teamleader request for a list of resources.
    """

    base_uri: str = Field(default="https://api.focus.teamleader.eu")
    resource: Resource
    page: int = Field(default=1, serialization_alias="page[number]")
    page_size: int = Field(default=20, serialization_alias="page[size]")
    updated_since: datetime | None = Field(
        default=None,
        serialization_alias="filter[updated_since]",
    )

    # needs to be isormat without microsecond ex: '2021-03-29T16:44:33+00:00'
    @field_serializer("updated_since")
    def serialize_updated_since(self, updated_since: datetime, _info):
        _ = _info
        return updated_since.replace(microsecond=0).isoformat()

    @property
    def path(self):
        return self.base_uri + "/" + self.resource.name + ".list"


class TL_Response(BaseModel):
    resource: Resource
    ratelimit_remaining: int
    ratelimit_reset: datetime
    data: list[dict[str, Any]] | dict[str, Any]


class TL_ResponseList(BaseModel):
    resource: Resource
    ratelimit_remaining: int
    ratelimit_reset: datetime
    data: list[dict[str, Any]]


class TL_ResponseInfo(BaseModel):
    resource: Resource
    ratelimit_remaining: int
    ratelimit_reset: datetime
    data: dict[str, Any]


def serialize(req) -> dict[str, Any]:
    """
    Convert the Pydantic request models into a Python dictionary.
    """
    if type(req) is TL_RequestList:
        return req.model_dump(
            exclude_none=True,
            by_alias=True,
            include={"page", "page_size", "updated_since"},
        )
    elif type(req) is TL_RequestInfo:
        return {"id": req.id}
    else:
        raise TypeError(
            f"Type of request should be {TL_RequestInfo.__name__} or {TL_RequestList.__name__}"
        )
